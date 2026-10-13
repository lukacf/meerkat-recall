use super::MlExportConfig;
use crate::{opt::Resource, value::Notification, Result};
use async_channel::Sender;
use bincode::Options;
use revision::Revisioned;
use serde::{ser::SerializeMap as _, Serialize};
use std::borrow::Cow;
use std::io::Read;
use std::path::PathBuf;
use surrealdb_core::kvs::export::Config as DbExportConfig;
use surrealdb_core::sql::{Array as CoreArray, Object as CoreObject, Query, Value as CoreValue};
use uuid::Uuid;

#[cfg(any(feature = "protocol-ws", feature = "protocol-http"))]
use surrealdb_core::sql::Table as CoreTable;

#[derive(Clone)]
#[allow(dead_code)]
pub(crate) enum Command {
	Use {
		namespace: Option<String>,
		database: Option<String>,
	},
	Signup {
		credentials: CoreObject,
	},
	Signin {
		credentials: CoreObject,
	},
	Authenticate {
		token: String,
	},
	Invalidate,
	Create {
		what: Resource,
		data: Option<CoreValue>,
	},
	Upsert {
		what: Resource,
		data: Option<CoreValue>,
	},
	Update {
		what: Resource,
		data: Option<CoreValue>,
	},
	Insert {
		// inserts can only be on a table.
		what: Option<String>,
		data: CoreValue,
	},
	InsertRelation {
		what: Option<String>,
		data: CoreValue,
	},
	Patch {
		what: Resource,
		data: Option<CoreValue>,
		upsert: bool,
	},
	Merge {
		what: Resource,
		data: Option<CoreValue>,
		upsert: bool,
	},
	Select {
		what: Resource,
	},
	Delete {
		what: Resource,
	},
	Query {
		query: Query,
		variables: CoreObject,
	},
	RawQuery {
		query: Cow<'static, str>,
		variables: CoreObject,
	},
	ExportFile {
		path: PathBuf,
		config: Option<DbExportConfig>,
	},
	ExportMl {
		path: PathBuf,
		config: MlExportConfig,
	},
	ExportBytes {
		bytes: Sender<Result<Vec<u8>>>,
		config: Option<DbExportConfig>,
	},
	ExportBytesMl {
		bytes: Sender<Result<Vec<u8>>>,
		config: MlExportConfig,
	},
	ImportFile {
		path: PathBuf,
	},
	ImportMl {
		path: PathBuf,
	},
	Health,
	Version,
	RecallHistoryInventory { namespace: String, database: String, timeout_ms: u64 },
	RecallPhysicalHistoryCheckpoint {
		namespace: String,
		database: String,
		timeout_ms: u64,
		authorities: Vec<surrealdb_core::kvs::RecallHistoryAuthority>,
	},
	RecallPhysicalCheckpointCapabilities,
	RecallPhysicalCheckpoint {
		namespace: String,
		database: String,
		timeout_ms: u64,
	},
	Set {
		key: String,
		value: CoreValue,
	},
	Unset {
		key: String,
	},
	SubscribeLive {
		uuid: Uuid,
		notification_sender: Sender<Result<Notification<CoreValue>>>,
	},
	Kill {
		uuid: Uuid,
	},
	Run {
		name: String,
		version: Option<String>,
		args: CoreArray,
	},
}

impl std::fmt::Debug for Command {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Use { namespace, database } => f.debug_struct("Use")
				.field("namespace", namespace)
				.field("database", database)
				.finish(),
			Self::Signup { credentials } => f.debug_struct("Signup")
				.field("credentials", credentials)
				.finish(),
			Self::Signin { credentials } => f.debug_struct("Signin")
				.field("credentials", credentials)
				.finish(),
			Self::Authenticate { token } => f.debug_struct("Authenticate")
				.field("token", token)
				.finish(),
			Self::Invalidate => f.write_str("Invalidate"),
			Self::Create { what, data } => f.debug_struct("Create")
				.field("what", what)
				.field("data", data)
				.finish(),
			Self::Upsert { what, data } => f.debug_struct("Upsert")
				.field("what", what)
				.field("data", data)
				.finish(),
			Self::Update { what, data } => f.debug_struct("Update")
				.field("what", what)
				.field("data", data)
				.finish(),
			Self::Insert { what, data } => f.debug_struct("Insert")
				.field("what", what)
				.field("data", data)
				.finish(),
			Self::InsertRelation { what, data } => f.debug_struct("InsertRelation")
				.field("what", what)
				.field("data", data)
				.finish(),
			Self::Patch { what, data, upsert } => f.debug_struct("Patch")
				.field("what", what)
				.field("data", data)
				.field("upsert", upsert)
				.finish(),
			Self::Merge { what, data, upsert } => f.debug_struct("Merge")
				.field("what", what)
				.field("data", data)
				.field("upsert", upsert)
				.finish(),
			Self::Select { what } => f.debug_struct("Select")
				.field("what", what)
				.finish(),
			Self::Delete { what } => f.debug_struct("Delete")
				.field("what", what)
				.finish(),
			Self::Query { query, variables } => f.debug_struct("Query")
				.field("query", query)
				.field("variables", &crate::api::method::query::RecallBindingsDebug(variables))
				.finish(),
			Self::RawQuery { query, variables } => f.debug_struct("RawQuery")
				.field("query", query)
				.field("variables", &crate::api::method::query::RecallBindingsDebug(variables))
				.finish(),
			Self::ExportFile { path, config } => f.debug_struct("ExportFile")
				.field("path", path)
				.field("config", config)
				.finish(),
			Self::ExportMl { path, config } => f.debug_struct("ExportMl")
				.field("path", path)
				.field("config", config)
				.finish(),
			Self::ExportBytes { bytes, config } => f.debug_struct("ExportBytes")
				.field("bytes", bytes)
				.field("config", config)
				.finish(),
			Self::ExportBytesMl { bytes, config } => f.debug_struct("ExportBytesMl")
				.field("bytes", bytes)
				.field("config", config)
				.finish(),
			Self::ImportFile { path } => f.debug_struct("ImportFile")
				.field("path", path)
				.finish(),
			Self::ImportMl { path } => f.debug_struct("ImportMl")
				.field("path", path)
				.finish(),
			Self::Health => f.write_str("Health"),
			Self::Version => f.write_str("Version"),
			Self::RecallHistoryInventory { namespace, database, timeout_ms } => f.debug_struct("RecallHistoryInventory")
				.field("namespace", namespace).field("database", database).field("timeout_ms", timeout_ms).finish(),
			Self::RecallPhysicalHistoryCheckpoint { namespace, database, timeout_ms, authorities } => f.debug_struct("RecallPhysicalHistoryCheckpoint")
				.field("namespace", namespace).field("database", database).field("timeout_ms", timeout_ms)
				.field("authority_count", &authorities.len()).finish_non_exhaustive(),
			Self::RecallPhysicalCheckpointCapabilities => f.write_str("RecallPhysicalCheckpointCapabilities"),
			Self::RecallPhysicalCheckpoint { namespace, database, timeout_ms } => f.debug_struct("RecallPhysicalCheckpoint")
				.field("namespace", namespace)
				.field("database", database)
				.field("timeout_ms", timeout_ms)
				.finish(),
			Self::Set { key, value } => f.debug_struct("Set")
				.field("key", key)
				.field("value", value)
				.finish(),
			Self::Unset { key } => f.debug_struct("Unset")
				.field("key", key)
				.finish(),
			Self::SubscribeLive { uuid, notification_sender } => f.debug_struct("SubscribeLive")
				.field("uuid", uuid)
				.field("notification_sender", notification_sender)
				.finish(),
			Self::Kill { uuid } => f.debug_struct("Kill")
				.field("uuid", uuid)
				.finish(),
			Self::Run { name, version, args } => f.debug_struct("Run")
				.field("name", name)
				.field("version", version)
				.field("args", args)
				.finish(),
		}
	}
}

impl Command {
	#[cfg(any(feature = "protocol-ws", feature = "protocol-http"))]
	pub(crate) fn into_router_request(self, id: Option<i64>) -> Option<RouterRequest> {
		use crate::api::engine::resource_to_values;
		use surrealdb_core::sql::{
			statements::{UpdateStatement, UpsertStatement},
			Data, Output,
		};

		let res = match self {
			Command::Use {
				namespace,
				database,
			} => RouterRequest {
				id,
				method: "use",
				params: Some(vec![CoreValue::from(namespace), CoreValue::from(database)].into()),
			},
			Command::Signup {
				credentials,
			} => RouterRequest {
				id,
				method: "signup",
				params: Some(vec![CoreValue::from(credentials)].into()),
			},
			Command::Signin {
				credentials,
			} => RouterRequest {
				id,
				method: "signin",
				params: Some(vec![CoreValue::from(credentials)].into()),
			},
			Command::Authenticate {
				token,
			} => RouterRequest {
				id,
				method: "authenticate",
				params: Some(vec![CoreValue::from(token)].into()),
			},
			Command::Invalidate => RouterRequest {
				id,
				method: "invalidate",
				params: None,
			},
			Command::Create {
				what,
				data,
			} => {
				let mut params = vec![what.into_core_value()];
				if let Some(data) = data {
					params.push(data);
				}

				RouterRequest {
					id,
					method: "create",
					params: Some(params.into()),
				}
			}
			Command::Upsert {
				what,
				data,
				..
			} => {
				let mut params = vec![what.into_core_value()];
				if let Some(data) = data {
					params.push(data);
				}

				RouterRequest {
					id,
					method: "upsert",
					params: Some(params.into()),
				}
			}
			Command::Update {
				what,
				data,
				..
			} => {
				let mut params = vec![what.into_core_value()];

				if let Some(data) = data {
					params.push(data);
				}

				RouterRequest {
					id,
					method: "update",
					params: Some(params.into()),
				}
			}
			Command::Insert {
				what,
				data,
			} => {
				let table = match what {
					Some(w) => {
						let mut table = CoreTable::default();
						table.0.clone_from(&w);
						CoreValue::from(table)
					}
					None => CoreValue::None,
				};

				let params = vec![table, data];

				RouterRequest {
					id,
					method: "insert",
					params: Some(params.into()),
				}
			}
			Command::InsertRelation {
				what,
				data,
			} => {
				let table = match what {
					Some(w) => {
						let mut tmp = CoreTable::default();
						tmp.0 = w.clone();
						CoreValue::from(tmp)
					}
					None => CoreValue::None,
				};
				let params = vec![table, data];

				RouterRequest {
					id,
					method: "insert_relation",
					params: Some(params.into()),
				}
			}
			Command::Patch {
				what,
				data,
				upsert,
				..
			} => {
				let query = if upsert {
					let mut stmt = UpsertStatement::default();
					stmt.what = resource_to_values(what);
					stmt.data = data.map(Data::PatchExpression);
					stmt.output = Some(Output::After);
					Query::from(stmt)
				} else {
					let mut stmt = UpdateStatement::default();
					stmt.what = resource_to_values(what);
					stmt.data = data.map(Data::PatchExpression);
					stmt.output = Some(Output::After);
					Query::from(stmt)
				};

				let variables = CoreObject::default();
				let params: Vec<CoreValue> = vec![query.into(), variables.into()];

				RouterRequest {
					id,
					method: "query",
					params: Some(params.into()),
				}
			}
			Command::Merge {
				what,
				data,
				upsert,
				..
			} => {
				let query = if upsert {
					let mut stmt = UpsertStatement::default();
					stmt.what = resource_to_values(what);
					stmt.data = data.map(Data::MergeExpression);
					stmt.output = Some(Output::After);
					Query::from(stmt)
				} else {
					let mut stmt = UpdateStatement::default();
					stmt.what = resource_to_values(what);
					stmt.data = data.map(Data::MergeExpression);
					stmt.output = Some(Output::After);
					Query::from(stmt)
				};

				let variables = CoreObject::default();
				let params: Vec<CoreValue> = vec![query.into(), variables.into()];

				RouterRequest {
					id,
					method: "query",
					params: Some(params.into()),
				}
			}
			Command::Select {
				what,
				..
			} => RouterRequest {
				id,
				method: "select",
				params: Some(CoreValue::Array(vec![what.into_core_value()].into())),
			},
			Command::Delete {
				what,
				..
			} => RouterRequest {
				id,
				method: "delete",
				params: Some(CoreValue::Array(vec![what.into_core_value()].into())),
			},
			Command::Query {
				query,
				variables,
			} => {
				let params: Vec<CoreValue> = vec![query.into(), variables.into()];
				RouterRequest {
					id,
					method: "query",
					params: Some(params.into()),
				}
			}
			Command::RawQuery {
				query,
				variables,
			} => {
				let params: Vec<CoreValue> = vec![query.into_owned().into(), variables.into()];
				RouterRequest {
					id,
					method: "query",
					params: Some(params.into()),
				}
			}
			Command::ExportFile {
				..
			}
			| Command::ExportBytes {
				..
			}
			| Command::ImportFile {
				..
			}
			| Command::ExportBytesMl {
				..
			}
			| Command::ExportMl {
				..
			}
			| Command::ImportMl {
				..
			} => return None,
			Command::Health => RouterRequest {
				id,
				method: "ping",
				params: None,
			},
			Command::Version => RouterRequest {
				id,
				method: "version",
				params: None,
			},
			Command::RecallHistoryInventory { namespace, database, timeout_ms } => RouterRequest {
				id,
				method: "recall_source_history_inventory",
				params: Some(vec![CoreValue::from(surrealdb_core::rpc::recall_history_inventory::PROTOCOL),
					CoreValue::from(namespace), CoreValue::from(database), CoreValue::from(timeout_ms)].into()),
			},
			Command::RecallPhysicalHistoryCheckpoint { namespace, database, timeout_ms, authorities } => RouterRequest {
				id,
				method: "recall_physical_history_checkpoint",
				params: Some(vec![
					CoreValue::from(surrealdb_core::rpc::recall_physical_history_checkpoint::PROTOCOL),
					CoreValue::from(namespace), CoreValue::from(database), CoreValue::from(timeout_ms),
					CoreValue::Array(authorities.iter().map(|authority| authority.to_rpc_value()).collect::<Vec<_>>().into()),
				].into()),
			},
			Command::RecallPhysicalCheckpointCapabilities => RouterRequest {
				id,
				method: "recall_physical_checkpoint_capabilities",
				params: Some(vec![CoreValue::from(surrealdb_core::rpc::recall_physical_checkpoint::PROTOCOL)].into()),
			},
			Command::RecallPhysicalCheckpoint { namespace, database, timeout_ms } => RouterRequest {
				id,
				method: "recall_physical_checkpoint",
				params: Some(vec![
					CoreValue::from(surrealdb_core::rpc::recall_physical_checkpoint::PROTOCOL),
					CoreValue::from(namespace), CoreValue::from(database), CoreValue::from(timeout_ms),
				].into()),
			},
			Command::Set {
				key,
				value,
			} => RouterRequest {
				id,
				method: "let",
				params: Some(CoreValue::from(vec![CoreValue::from(key), value])),
			},
			Command::Unset {
				key,
			} => RouterRequest {
				id,
				method: "unset",
				params: Some(CoreValue::from(vec![CoreValue::from(key)])),
			},
			Command::SubscribeLive {
				..
			} => return None,
			Command::Kill {
				uuid,
			} => RouterRequest {
				id,
				method: "kill",
				params: Some(CoreValue::from(vec![CoreValue::from(uuid)])),
			},
			Command::Run {
				name,
				version,
				args,
			} => RouterRequest {
				id,
				method: "run",
				params: Some(
					vec![CoreValue::from(name), CoreValue::from(version), CoreValue::Array(args)]
						.into(),
				),
			},
		};
		Some(res)
	}

	#[cfg(feature = "protocol-http")]
	pub(crate) fn needs_flatten(&self) -> bool {
		match self {
			Command::Upsert {
				what,
				..
			}
			| Command::Update {
				what,
				..
			}
			| Command::Patch {
				what,
				..
			}
			| Command::Merge {
				what,
				..
			}
			| Command::Select {
				what,
			}
			| Command::Delete {
				what,
			} => matches!(what, Resource::RecordId(_)),
			Command::Insert {
				data,
				..
			} => !data.is_array(),
			_ => false,
		}
	}
}

/// A struct which will be serialized as a map to behave like the previously used BTreeMap.
///
/// This struct serializes as if it is a surrealdb_core::sql::Value::Object.
#[allow(dead_code)]
pub(crate) struct RouterRequest {
	id: Option<i64>,
	method: &'static str,
	params: Option<CoreValue>,
}

impl std::fmt::Debug for RouterRequest {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if matches!(self.method, "recall_physical_history_checkpoint" | "recall_source_history_inventory") {
			return f.debug_struct("RouterRequest").field("id", &self.id)
				.field("method", &self.method).field("params", &"[REDACTED]").finish();
		}
		let mut params = self.params.clone();
		if self.method == "query" {
			if let Some(CoreValue::Array(values)) = &mut params {
				if let Some(CoreValue::Object(bindings)) = values.0.get_mut(1) {
					for name in ["__recall_reference_key", "__recall_journal_identity", "__recall_erasure_reservation"] {
						if let Some(value) = bindings.get_mut(name) { *value = CoreValue::from("[REDACTED]"); }
					}
				}
			}
		}
		f.debug_struct("RouterRequest").field("id", &self.id).field("method", &self.method)
			.field("params", &params).finish()
	}
}

#[cfg(feature = "protocol-ws")]
fn stringify_queries(value: CoreValue) -> CoreValue {
	match value {
		CoreValue::Query(query) => CoreValue::Strand(query.to_string().into()),
		CoreValue::Array(array) => CoreValue::Array(CoreArray::from(
			array.0.into_iter().map(stringify_queries).collect::<Vec<_>>(),
		)),
		_ => value,
	}
}

impl RouterRequest {
	#[cfg(feature = "protocol-ws")]
	pub(crate) fn stringify_queries(self) -> Self {
		Self {
			params: self.params.map(stringify_queries),
			..self
		}
	}
}

impl Serialize for RouterRequest {
	fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		struct InnerRequest<'a>(&'a RouterRequest);
		struct InnerNumberVariant(i64);
		struct InnerNumber(i64);
		struct InnerMethod(&'static str);
		struct InnerStrand(&'static str);
		struct InnerObject<'a>(&'a RouterRequest);

		impl Serialize for InnerNumberVariant {
			fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
			where
				S: serde::Serializer,
			{
				serializer.serialize_newtype_variant("Value", 3, "Number", &InnerNumber(self.0))
			}
		}

		impl Serialize for InnerNumber {
			fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
			where
				S: serde::Serializer,
			{
				serializer.serialize_newtype_variant("Number", 0, "Int", &self.0)
			}
		}

		impl Serialize for InnerMethod {
			fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
			where
				S: serde::Serializer,
			{
				serializer.serialize_newtype_variant("Value", 4, "Strand", &InnerStrand(self.0))
			}
		}

		impl Serialize for InnerStrand {
			fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
			where
				S: serde::Serializer,
			{
				serializer.serialize_newtype_struct("$surrealdb::private::sql::Strand", self.0)
			}
		}

		impl Serialize for InnerRequest<'_> {
			fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
			where
				S: serde::Serializer,
			{
				let size = 1 + self.0.id.is_some() as usize + self.0.params.is_some() as usize;
				let mut map = serializer.serialize_map(Some(size))?;
				if let Some(id) = self.0.id.as_ref() {
					map.serialize_entry("id", &InnerNumberVariant(*id))?;
				}
				map.serialize_entry("method", &InnerMethod(self.0.method))?;
				if let Some(params) = self.0.params.as_ref() {
					map.serialize_entry("params", params)?;
				}
				map.end()
			}
		}

		impl Serialize for InnerObject<'_> {
			fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
			where
				S: serde::Serializer,
			{
				serializer.serialize_newtype_struct("Object", &InnerRequest(self.0))
			}
		}

		serializer.serialize_newtype_variant(
			"$surrealdb::private::sql::Value",
			9,
			"Object",
			&InnerObject(self),
		)
	}
}

impl Revisioned for RouterRequest {
	fn revision() -> u16 {
		1
	}

	fn serialize_revisioned<W: std::io::Write>(
		&self,
		w: &mut W,
	) -> std::result::Result<(), revision::Error> {
		// version
		Revisioned::serialize_revisioned(&1u32, w)?;
		// object variant
		Revisioned::serialize_revisioned(&9u32, w)?;
		// object wrapper version
		Revisioned::serialize_revisioned(&1u32, w)?;

		let size = 1 + self.id.is_some() as usize + self.params.is_some() as usize;
		size.serialize_revisioned(w)?;

		let serializer = bincode::options()
			.with_no_limit()
			.with_little_endian()
			.with_varint_encoding()
			.reject_trailing_bytes();

		if let Some(x) = self.id.as_ref() {
			serializer
				.serialize_into(&mut *w, "id")
				.map_err(|err| revision::Error::Serialize(err.to_string()))?;

			// the Value version
			1u16.serialize_revisioned(w)?;

			// the Value::Number variant
			3u16.serialize_revisioned(w)?;

			// the Number version
			1u16.serialize_revisioned(w)?;

			// the Number::Int variant
			0u16.serialize_revisioned(w)?;

			x.serialize_revisioned(w)?;
		}

		serializer
			.serialize_into(&mut *w, "method")
			.map_err(|err| revision::Error::Serialize(err.to_string()))?;

		// the Value version
		1u16.serialize_revisioned(w)?;

		// the Value::Strand variant
		4u16.serialize_revisioned(w)?;

		// the Strand version
		1u16.serialize_revisioned(w)?;

		serializer
			.serialize_into(&mut *w, self.method)
			.map_err(|e| revision::Error::Serialize(format!("{:?}", e)))?;

		if let Some(x) = self.params.as_ref() {
			serializer
				.serialize_into(&mut *w, "params")
				.map_err(|err| revision::Error::Serialize(err.to_string()))?;

			x.serialize_revisioned(w)?;
		}

		Ok(())
	}

	fn deserialize_revisioned<R: Read>(_: &mut R) -> std::result::Result<Self, revision::Error>
	where
		Self: Sized,
	{
		panic!("deliberately unimplemented");
	}
}

#[cfg(test)]
mod test {
	use std::io::Cursor;

	use revision::Revisioned;
	use surrealdb_core::sql::{Number, Value};

	use super::RouterRequest;

	fn assert_converts<S, D, I>(req: &RouterRequest, s: S, d: D)
	where
		S: FnOnce(&RouterRequest) -> I,
		D: FnOnce(I) -> Value,
	{
		let ser = s(req);
		let val = d(ser);
		let Value::Object(obj) = val else {
			panic!("not an object");
		};
		assert_eq!(
			obj.get("id").cloned().and_then(|x| if let Value::Number(Number::Int(x)) = x {
				Some(x)
			} else {
				None
			}),
			req.id
		);
		let Some(Value::Strand(x)) = obj.get("method") else {
			panic!("invalid method field: {}", obj)
		};
		assert_eq!(x.0, req.method);

		assert_eq!(obj.get("params").cloned(), req.params);
	}

	#[test]
	fn router_request_value_conversion() {
		let request = RouterRequest {
			id: Some(1234),
			method: "request",
			params: Some(vec![Value::from(1234i64), Value::from("request")].into()),
		};

		println!("test convert bincode");

		assert_converts(
			&request,
			|i| bincode::serialize(i).unwrap(),
			|b| bincode::deserialize(&b).unwrap(),
		);

		println!("test convert json");

		assert_converts(
			&request,
			|i| serde_json::to_string(i).unwrap(),
			|b| serde_json::from_str(&b).unwrap(),
		);

		println!("test convert revisioned");

		assert_converts(
			&request,
			|i| {
				let mut buf = Vec::new();
				i.serialize_revisioned(&mut Cursor::new(&mut buf)).unwrap();
				buf
			},
			|b| Value::deserialize_revisioned(&mut Cursor::new(b)).unwrap(),
		);

		println!("done");
	}
}

#[cfg(test)]
mod recall_debug_tests {
	use super::*;

	#[test]
	fn query_command_and_router_debug_redact_private_binding() {
		let variables = CoreObject::from(std::collections::BTreeMap::from([
			("__recall_reference_key".to_owned(), CoreValue::from("recall-debug-sentinel")),
			("ordinary".to_owned(), CoreValue::from("ordinary-debug-sentinel")),
		]));
		let command = Command::RawQuery { query: "RETURN $ordinary;".into(), variables: variables.clone() };
		let debug = format!("{command:?}");
		assert!(!debug.contains("recall-debug-sentinel"));
		assert!(debug.contains("ordinary-debug-sentinel"));
		let router = RouterRequest { id: None, method: "query", params: Some(CoreValue::from(vec![CoreValue::from("RETURN $ordinary;"), CoreValue::Object(variables)])) };
		let debug = format!("{router:?}");
		assert!(!debug.contains("recall-debug-sentinel"));
		assert!(debug.contains("ordinary-debug-sentinel"));
	}
}

#[cfg(all(test, any(feature = "protocol-ws", feature = "protocol-http")))]
mod recall_history_wire_tests {
	use super::*;
	use std::collections::BTreeSet;

	#[test]
	fn physical_history_wire_is_native_and_debug_redacts_all_authority_fields() {
		let authority = surrealdb_core::kvs::RecallHistoryAuthority::new(
			"ns".into(), "db".into(), "11111111-1111-4111-8111-111111111111".into(),
			"space".into(), [7; 32], vec!["entity".into()], BTreeSet::from(["a".repeat(64)]),
		).unwrap().bind_history("b".repeat(64), BTreeSet::new(), BTreeSet::new()).unwrap();
		let encoded = authority.to_rpc_value();
		let command = Command::RecallPhysicalHistoryCheckpoint {
			namespace: "ns".into(), database: "db".into(), timeout_ms: 40_000, authorities: vec![authority],
		};
		assert!(!format!("{command:?}").contains(&"a".repeat(64)));
		let request = command.into_router_request(Some(42)).unwrap();
		assert_eq!(request.method, "recall_physical_history_checkpoint");
		assert_eq!(request.params, Some(CoreValue::from(vec![
			CoreValue::from("meerkat-recall-physical-history-checkpoint/v1"), "ns".into(), "db".into(),
			40_000_i64.into(), CoreValue::Array(vec![encoded].into()),
		])));
		let debug = format!("{request:?}");
		assert!(debug.contains("[REDACTED]"));
		assert!(!debug.contains(&"a".repeat(64)));
		assert!(!debug.contains("11111111-1111-4111-8111-111111111111"));
	}
}

#[cfg(all(test, any(feature = "protocol-http", feature = "protocol-ws")))]
mod recall_checkpoint_wire_tests {
    use super::{Command, CoreValue as Value};

    #[test]
    fn checkpoint_transport_encodes_version_scope_and_timeout() {
        let capability = Command::RecallPhysicalCheckpointCapabilities
            .into_router_request(Some(41))
            .unwrap();
        assert_eq!(capability.method, "recall_physical_checkpoint_capabilities");
        assert_eq!(
            capability.params,
            Some(Value::from(vec![Value::from(
                "meerkat-recall-physical-checkpoint/v1"
            )]))
        );
        let request = Command::RecallPhysicalCheckpoint {
            namespace: "scope-ns".into(),
            database: "scope-db".into(),
            timeout_ms: 59_000,
        }
        .into_router_request(Some(42))
        .unwrap();
        assert_eq!(request.method, "recall_physical_checkpoint");
        assert_eq!(
            request.params,
            Some(Value::from(vec![
                Value::from("meerkat-recall-physical-checkpoint/v1"),
                Value::from("scope-ns"),
                Value::from("scope-db"),
                Value::from(59_000_u64),
            ]))
        );
    }

}
