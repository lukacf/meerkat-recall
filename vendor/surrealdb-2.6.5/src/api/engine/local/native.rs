use crate::{
	api::{
		conn::{Connection, Route, Router},
		engine::local::Db,
		method::BoxFuture,
		opt::{Endpoint, EndpointKind},
		ExtraFeatures, Result, Surreal,
	},
	engine::tasks,
	opt::{auth::Root, WaitFor},
	value::Notification,
	Action,
};
use async_channel::{Receiver, Sender};
use futures::{stream::poll_fn, FutureExt, StreamExt};
use std::{
	collections::{BTreeMap, HashMap, HashSet},
	sync::{atomic::AtomicI64, Arc},
	task::Poll,
};
use surrealdb_core::{dbs::Session, iam::Level, kvs::Datastore, options::EngineOptions};
use tokio::sync::{watch, RwLock};
use tokio_util::sync::CancellationToken;

impl crate::api::Connection for Db {}

impl Connection for Db {
	fn connect(
		mut address: Endpoint,
		capacity: usize,
	) -> BoxFuture<'static, Result<Surreal<Self>>> {
		Box::pin(async move {
			let (route_tx, route_rx) = match capacity {
				0 => async_channel::unbounded(),
				capacity => async_channel::bounded(capacity),
			};

			let (conn_tx, conn_rx) = async_channel::bounded(1);
			address.config.recall_local_close =
				Some(crate::api::conn::local_close::LocalClose::new());
			let config = address.config.clone();

			tokio::spawn(run_router(address, conn_tx, route_rx));

			conn_rx.recv().await??;

			let mut features = HashSet::new();
			features.insert(ExtraFeatures::Backup);
			features.insert(ExtraFeatures::LiveQueries);

			let waiter = watch::channel(Some(WaitFor::Connection));
			let router = Router {
				features,
				config,
				sender: route_tx,
				last_id: AtomicI64::new(0),
			};

			Ok((router, waiter).into())
		})
	}
}

pub(crate) async fn run_router(
	address: Endpoint,
	conn_tx: Sender<Result<()>>,
	route_rx: Receiver<Route>,
) {
	let Some(owners) = address.config.recall_local_close.clone() else {
		let _ = conn_tx
			.send(Err(crate::api::err::Error::InternalError(
				"local connection close owner missing".into(),
			)
			.into()))
			.await;
		return;
	};
	let result =
		std::panic::AssertUnwindSafe(run_router_owned(address, conn_tx, route_rx, owners.clone()))
			.catch_unwind()
			.await
			.unwrap_or_else(|_| {
				Err(crate::api::err::Error::InternalError(
					"local router panicked during shutdown".into(),
				)
				.into())
			});
	owners.complete(result);
}

async fn run_router_owned(
	address: Endpoint,
	conn_tx: Sender<Result<()>>,
	route_rx: Receiver<Route>,
	owners: Arc<crate::api::conn::local_close::LocalClose>,
) -> Result<()> {
	let configured_root = match address.config.auth {
		Level::Root => Some(Root {
			username: &address.config.username,
			password: &address.config.password,
		}),
		_ => None,
	};

	let endpoint = match EndpointKind::from(address.url.scheme()) {
		EndpointKind::TiKv => address.url.as_str(),
		_ => &address.path,
	};

	let kvs = match Datastore::new(endpoint).await {
		Ok(kvs) => {
			if let Err(error) = kvs.check_version().await {
				conn_tx.send(Err(error.into())).await.ok();
				return Ok(());
			};
			if let Err(error) = kvs.bootstrap().await {
				conn_tx.send(Err(error.into())).await.ok();
				return Ok(());
			}
			// If a root user is specified, setup the initial datastore credentials
			if let Some(root) = configured_root {
				if let Err(error) = kvs
					.initialise_credentials(root.username, root.password)
					.await
				{
					conn_tx.send(Err(error.into())).await.ok();
					return Ok(());
				}
			}
			conn_tx.send(Ok(())).await.ok();
			kvs.with_auth_enabled(configured_root.is_some())
		}
		Err(error) => {
			conn_tx.send(Err(error.into())).await.ok();
			return Ok(());
		}
	};

	let kvs = match address
		.config
		.capabilities
		.allows_live_query_notifications()
	{
		true => kvs.with_notifications(),
		false => kvs,
	};

	let kvs = kvs
		.with_strict_mode(address.config.strict)
		.with_query_timeout(address.config.query_timeout)
		.with_transaction_timeout(address.config.transaction_timeout)
		.with_capabilities(address.config.capabilities);

	#[cfg(storage)]
	let kvs = kvs.with_temporary_directory(address.config.temporary_directory);

	let kvs = Arc::new(kvs);
	#[cfg(feature = "recall-close-test-utils")]
	owners.attach_datastore(&kvs);
	let vars = Arc::new(RwLock::new(BTreeMap::default()));
	let live_queries = Arc::new(RwLock::new(HashMap::new()));
	let session = Arc::new(RwLock::new(Session::default().with_rt(true)));

	let canceller = CancellationToken::new();

	let mut opt = EngineOptions::default();
	if let Some(interval) = address.config.node_membership_refresh_interval {
		opt.node_membership_refresh_interval = interval;
	}
	if let Some(interval) = address.config.node_membership_check_interval {
		opt.node_membership_check_interval = interval;
	}
	if let Some(interval) = address.config.node_membership_cleanup_interval {
		opt.node_membership_cleanup_interval = interval;
	}
	if let Some(interval) = address.config.changefeed_gc_interval {
		opt.changefeed_gc_interval = interval;
	}
	let background_gate = address.config.recall_background_gate.unwrap_or_default();
	let tasks = tasks::init_with_gate(
		kvs.clone(),
		canceller.clone(),
		&opt,
		background_gate.clone(),
	);

	let mut notifications = kvs.notifications().map(Box::pin);
	let mut notification_stream = poll_fn(move |cx| match &mut notifications {
		Some(rx) => rx.poll_next_unpin(cx),
		// return poll pending so that this future is never woken up again and therefore not
		// constantly polled.
		None => Poll::Pending,
	});

	loop {
		let kvs = kvs.clone();
		let session = session.clone();
		let vars = vars.clone();
		let live_queries = live_queries.clone();
		tokio::select! {
			route = route_rx.recv() => {
				let Ok(route) = route else {
					break
				};
				let child_owners = owners.clone();
				owners.spawn(async move {
					match super::router(route.request, &kvs, &session, &vars, &live_queries, &child_owners)
						.await
					{
						Ok(value) => {
							route.response.send(Ok(value)).await.ok();
						}
						Err(error) => {
							route.response.send(Err(error)).await.ok();
						}
					}
				});
			}
			notification = notification_stream.next() => {
				let Some(notification) = notification else {
					// TODO: Maybe we should do something more then ignore a closed notifications
					// channel?
					continue
				};

				let notification = Notification{
					query_id: *notification.id,
					action: Action::from_core(notification.action),
					data: notification.result
				};

				let background_gate = background_gate.clone();
				let canceller = canceller.clone();
				owners.spawn(async move {
					let id = notification.query_id;
					let sender = live_queries.read().await.get(&id).cloned();
					if let Some(sender) = sender {

						let delivered = tokio::select! {
							biased;
							_ = canceller.cancelled() => return,
							result = sender.send(Ok(notification)) => result,
						};
						if delivered.is_err() {
							live_queries.write().await.remove(&id);
							let _maintenance = tokio::select! {
								biased;
								_ = canceller.cancelled() => return,
								lease = background_gate.read() => lease,
							};
							if let Err(error) =
								super::kill_live_query(&kvs, id, &*session.read().await, vars.read().await.clone()).await
							{
								warn!("Failed to kill live query '{id}'; {error}");
							}
						}
					}
				});
			}
		}
	}
	// All accepted routes have been dispatched; close rejects new sends.
	// Cancel maintenance before it can enter another write, then finish all
	// entered request/notification/export owners without aborting them.
	canceller.cancel();
	let requests = owners.drain().await;
	let background = tasks.resolve().await;
	let shutdown = kvs.recall_close_local().await;
	drop(notification_stream);
	drop(live_queries);
	drop(session);
	drop(vars);
	drop(kvs);
	requests?;
	background?;
	shutdown?;
	Ok(())
}
