//! Trusted query custody and parsed import admission for the pinned native backend.

use super::{Any, StorageClient, StorageError, StorageResult, Surreal};
use std::collections::BTreeMap;
use surrealdb::sql::visit::{Visit, Visitor};
use surrealdb::sql::{Array, Id, Query, Thing, Value};

const KEY_PARAMETER: &str = "__recall_reference_key";
const JOURNAL_PARAMETER: &str = "__recall_journal_identity";
const RESERVATION_PARAMETER: &str = "__recall_erasure_reservation";
const CAPABILITY_JOURNAL: &str = "00000000-0000-0000-0000-000000000001";
const CAPABILITY_SPACE: &str = "__recall_capability__";
// Independent HMAC-SHA256 vectors for the frozen reference/v1 byte encoding.
// Native Number(42) and native String("42") must remain distinct.
const CAPABILITY_TOKENS: [&str; 2] = [
    "03d2d2962853eaa5410909793d65f296968ae5e6024ab522121d575cf1108454",
    "40e9322203b5e4d741f086599f851ac1b7c4e8a2e0bad1398da4ffde5911874c",
];

// Inspect actual SDK error variants, never query text or arbitrary substrings.
pub(super) fn is_erasure_reservation_error(error: &surrealdb::Error) -> bool {
    crate::error::is_erasure_in_progress(error)
}

struct OrdinaryQuery;

impl Visitor for OrdinaryQuery {
    type Error = StorageError;

    fn visit_option(
        &mut self,
        option: &surrealdb::sql::statements::OptionStatement,
    ) -> StorageResult<()> {
        if option.name.0.eq_ignore_ascii_case("IMPORT") && option.what {
            return Err(StorageError::Conflict(
                "native import mode requires exclusive verified restore".into(),
            ));
        }
        Ok(())
    }
}

pub(super) fn binding_value<T: serde::Serialize + 'static>(bindings: T) -> StorageResult<Value> {
    surrealdb::sql::to_value(bindings)
        .map_err(|_| StorageError::QueryError("invalid native query bindings".into()))
}

fn parsed_ordinary_query(sql: &str, bindings: Option<&Value>) -> StorageResult<Query> {
    let query =
        surrealdb::sql::parse(sql).map_err(|error| StorageError::QueryError(error.to_string()))?;
    query.visit(&mut OrdinaryQuery)?;
    // The SDK accepts executable native values as well as ordinary JSON. Walk
    // those ASTs too, without interpreting reference-looking prose as SQL.
    if let Some(bindings) = bindings {
        bindings.visit(&mut OrdinaryQuery)?;
    }
    Ok(query)
}

impl StorageClient {
    pub(super) fn trusted_query(
        &self,
        db: &Surreal<Any>,
        sql: &str,
        bindings: Option<Value>,
    ) -> StorageResult<surrealdb::method::Query<'static, Any>> {
        let parsed = parsed_ordinary_query(sql, bindings.as_ref())?;
        let query = match bindings {
            Some(bindings) => db.query(parsed).bind(bindings),
            None => db.query(parsed),
        };
        // The pinned SDK appends maps/overwrites tuple keys in bind order.
        // Runtime custody is always last, including retry and unchecked paths.
        Ok(query
            .bind((
                KEY_PARAMETER,
                Value::Bytes(self.journal.reference_key_bytes().to_vec().into()),
            ))
            .bind((JOURNAL_PARAMETER, self.journal.identity().to_owned()))
            .bind((
                RESERVATION_PARAMETER,
                super::current_erasure_reservation(self.journal.identity())
                    .map_or(Value::None, Value::from),
            ))
            .into_owned())
    }

    pub(super) fn trusted_restore_query(
        &self,
        db: &Surreal<Any>,
        sql: &str,
    ) -> StorageResult<surrealdb::method::Query<'static, Any>> {
        if !self.maintenance || self.maintenance_lease.is_none() {
            return Err(StorageError::Conflict(
                "native snapshot replacement requires exclusive restore custody".into(),
            ));
        }
        // Native exports include saved auto-generated array-child fields.
        // Restore their full explicit definitions over the placeholders made
        // while replaying parents, before any destructive replacement starts.
        let parsed = super::native_restore::parsed_restore_query(sql)?;
        Ok(db
            .query(parsed)
            .bind((
                KEY_PARAMETER,
                Value::Bytes(self.journal.reference_key_bytes().to_vec().into()),
            ))
            .bind((JOURNAL_PARAMETER, self.journal.identity().to_owned()))
            .bind((
                RESERVATION_PARAMETER,
                super::current_erasure_reservation(self.journal.identity())
                    .map_or(Value::None, Value::from),
            ))
            .into_owned())
    }

    pub(super) async fn refresh_native_reference_guards(&self) -> StorageResult<()> {
        #[derive(serde::Deserialize)]
        struct DatabaseInfo {
            tables: BTreeMap<String, String>,
        }

        if self.is_embedded() {
            return Ok(());
        }
        let mut response = self.query_checked("INFO FOR DB").await?;
        let info: Option<DatabaseInfo> = response.take(0)?;
        let tables = info
            .ok_or_else(|| {
                StorageError::DataIntegrityError("restored database table inventory missing".into())
            })?
            .tables
            .into_keys()
            .collect::<Vec<_>>();
        crate::reference_fence::install_native_reference_guards(self, &tables).await
    }

    /// Read-only version and independent known-vector handshake before admission.
    /// Both retained remote transports require the compatible Recall server.
    pub(crate) async fn verify_reference_capability(&self) -> StorageResult<()> {
        if self.is_embedded() {
            return Ok(());
        }
        let mut bindings = BTreeMap::<String, Value>::new();
        bindings.insert("probe_journal".into(), CAPABILITY_JOURNAL.into());
        bindings.insert("probe_space".into(), CAPABILITY_SPACE.into());
        bindings.insert(
            "probe_key".into(),
            Value::Bytes((0_u8..32).collect::<Vec<_>>().into()),
        );
        bindings.insert("probe_tables".into(), Value::Array(Array::default()));
        bindings.insert(
            "probe_records".into(),
            Value::Array(Array::from(vec![
                Value::Thing(Thing::from(("recall_probe", Id::String("42".into())))),
                Value::Thing(Thing::from(("recall_probe", Id::Number(42)))),
            ])),
        );
        let mut response = self.query_bind_checked(
            "RETURN { version: recall::reference_token_version(), tokens: recall::reference_tokens($probe_journal, $probe_space, $probe_key, $probe_tables, $probe_records), fields_positive: recall::original_fields_match({present:7,extra:true},{present:7,missing:NONE}), fields_changed: recall::original_fields_match({present:8},{present:7}), fields_null: recall::original_fields_match({missing:NULL},{missing:NONE}), row_positive: recall::original_row_matches({space_id:'probe',payload:'original',extra:true},'probe',{payload:'original',missing:NONE}), row_absent: recall::original_row_matches(NONE,'probe',{}), row_foreign: recall::original_row_matches({space_id:'foreign',payload:'original'},'probe',{payload:'original'}), row_changed: recall::original_row_matches({space_id:'probe',payload:'changed'},'probe',{payload:'original'}) };",
            bindings,
        ).await.map_err(|_| incompatible_native())?;
        let result: Option<ReferenceCapability> =
            response.take(0).map_err(|_| incompatible_native())?;
        if result.is_some_and(|result| result.matches()) {
            Ok(())
        } else {
            Err(incompatible_native())
        }
    }
}

#[derive(serde::Deserialize)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "Independent positive and negative server capability observations are distinct wire fields."
)]
struct ReferenceCapability {
    version: u32,
    tokens: Vec<String>,
    fields_positive: bool,
    fields_changed: bool,
    fields_null: bool,
    row_positive: bool,
    row_absent: bool,
    row_foreign: bool,
    row_changed: bool,
}

impl ReferenceCapability {
    fn matches(&self) -> bool {
        self.version == 1
            && self.tokens.iter().map(String::as_str).eq(CAPABILITY_TOKENS)
            && self.fields_positive
            && !self.fields_changed
            && !self.fields_null
            && self.row_positive
            && !self.row_absent
            && !self.row_foreign
            && !self.row_changed
    }
}

fn incompatible_native() -> StorageError {
    StorageError::ConnectionError(
        "native backend requires the compatible Recall reference-token v1 server, exact known vectors and original-field/row comparison capabilities".into(),
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    async fn client() -> StorageClient {
        StorageClient::new(super::super::StorageConfig {
            database: format!("reference_custody_{}", uuid::Uuid::new_v4().simple()),
            ..super::super::StorageConfig::default()
        })
        .await
        .unwrap()
    }

    fn custody_query() -> &'static str {
        "RETURN recall::reference_tokens($__recall_journal_identity, '__recall_custody__', $__recall_reference_key, [], type::thing('recall_reference_key', $__recall_journal_identity));"
    }

    fn forged_bindings() -> BTreeMap<String, Value> {
        BTreeMap::from([
            (KEY_PARAMETER.into(), Value::Bytes(vec![0; 32].into())),
            (
                JOURNAL_PARAMETER.into(),
                "00000000-0000-0000-0000-000000000099".into(),
            ),
        ])
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn all_four_query_paths_bind_original_custody_after_caller_values() {
        let client = client().await;
        let responses = [
            client.query(custody_query()).await.unwrap(),
            client.query_checked(custody_query()).await.unwrap(),
            client
                .query_bind(custody_query(), forged_bindings())
                .await
                .unwrap(),
            client
                .query_bind_checked(custody_query(), forged_bindings())
                .await
                .unwrap(),
        ];
        for mut response in responses {
            let actual: Vec<String> = response.take(0).unwrap();
            assert_eq!(
                actual,
                vec![client.journal.reference_key_fingerprint().to_owned()]
            );
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn isolated_custom_function_inherits_query_custody_without_persisting_key() {
        let client = client().await;
        client
            .query_checked(&format!(
                "DEFINE FUNCTION fn::reference_custody_probe() {{ {} }};",
                custody_query(),
            ))
            .await
            .unwrap();
        let mut response = client
            .query_checked("RETURN fn::reference_custody_probe();")
            .await
            .unwrap();
        let actual: Vec<String> = response.take(0).unwrap();
        assert_eq!(
            actual,
            vec![client.journal.reference_key_fingerprint().to_owned()]
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn ordinary_import_mode_is_refused_in_all_four_paths_before_any_write() {
        let client = client().await;
        let sql = "OPTION IMPORT; CREATE import_custody_probe:unexpected SET visible=true;";
        assert!(client.query(sql).await.is_err());
        assert!(client.query_checked(sql).await.is_err());
        assert!(client.query_bind(sql, forged_bindings()).await.is_err());
        assert!(client
            .query_bind_checked(sql, forged_bindings())
            .await
            .is_err());
        assert!(client
            .maintenance_client()
            .query_checked(sql)
            .await
            .is_err());
        let mut response = client
            .query_checked("SELECT VALUE id FROM import_custody_probe")
            .await
            .unwrap();
        let rows: Vec<surrealdb::sql::Thing> = response.take(0).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn import_admission_uses_ast_not_text_or_keyword_matching() {
        for sql in [
            "OPTION IMPORT; RETURN 1;",
            "option import = true; RETURN 1;",
        ] {
            assert!(parsed_ordinary_query(sql, None).is_err());
        }
        for sql in [
            "RETURN 'OPTION IMPORT';",
            "/* OPTION IMPORT */ RETURN 1;",
            "OPTION IMPORT = FALSE; RETURN 1;",
        ] {
            assert!(parsed_ordinary_query(sql, None).is_ok(), "{sql}");
        }
        let bound = Value::Query(surrealdb::sql::parse("OPTION IMPORT; RETURN 1;").unwrap());
        assert!(parsed_ordinary_query("RETURN $input", Some(&bound)).is_err());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn snapshot_import_requires_private_exclusive_restore_handle() {
        let client = client().await;
        assert!(client
            .replace_snapshot_bytes(b"OPTION IMPORT; RETURN 1;")
            .await
            .is_err());
        assert!(client
            .maintenance_client()
            .replace_snapshot_bytes(b"OPTION IMPORT; RETURN 1;")
            .await
            .is_err());
        let mut response = client.query_checked("RETURN 1;").await.unwrap();
        assert_eq!(response.take::<Option<i64>>(0).unwrap(), Some(1));
    }

    #[test]
    fn capability_requires_exact_version_and_typed_known_vector() {
        let mut capability = ReferenceCapability {
            version: 1,
            tokens: CAPABILITY_TOKENS
                .iter()
                .map(|token| (*token).to_owned())
                .collect(),
            fields_positive: true,
            fields_changed: false,
            fields_null: false,
            row_positive: true,
            row_absent: false,
            row_foreign: false,
            row_changed: false,
        };
        assert!(capability.matches());
        capability.fields_positive = false;
        assert!(!capability.matches());
        capability.fields_positive = true;
        capability.fields_changed = true;
        assert!(!capability.matches());
        capability.fields_changed = false;
        capability.fields_null = true;
        assert!(!capability.matches());
        capability.fields_null = false;
        assert!(capability.matches());
        for fields in [
            serde_json::json!({}),
            serde_json::json!({"fields_positive":true,"fields_changed":false}),
            serde_json::json!({"fields_positive":"true","fields_changed":false,"fields_null":false}),
        ] {
            let mut response = serde_json::json!({
                "version":1,"tokens":CAPABILITY_TOKENS,
                "row_positive":true,"row_absent":false,"row_foreign":false,"row_changed":false
            });
            response
                .as_object_mut()
                .unwrap()
                .extend(fields.as_object().unwrap().clone());
            assert!(serde_json::from_value::<ReferenceCapability>(response).is_err());
        }
        capability.row_positive = false;
        assert!(!capability.matches());
        capability.row_positive = true;
        capability.row_absent = true;
        assert!(!capability.matches());
        capability.row_absent = false;
        capability.row_foreign = true;
        assert!(!capability.matches());
        capability.row_foreign = false;
        capability.row_changed = true;
        assert!(!capability.matches());
        capability.row_changed = false;
        assert!(capability.matches());
        let complete = serde_json::json!({
            "version":1,"tokens":CAPABILITY_TOKENS,
            "fields_positive":true,"fields_changed":false,"fields_null":false,
            "row_positive":true,"row_absent":false,"row_foreign":false,"row_changed":false
        });
        assert!(
            serde_json::from_value::<ReferenceCapability>(complete.clone())
                .unwrap()
                .matches()
        );
        for key in ["row_positive", "row_absent", "row_foreign", "row_changed"] {
            let mut missing = complete.clone();
            missing.as_object_mut().unwrap().remove(key);
            assert!(serde_json::from_value::<ReferenceCapability>(missing).is_err());
            let mut malformed = complete.clone();
            malformed
                .as_object_mut()
                .unwrap()
                .insert(key.into(), "false".into());
            assert!(serde_json::from_value::<ReferenceCapability>(malformed).is_err());
        }
        capability.version = 2;
        assert!(!capability.matches());
        capability.version = 1;
        capability.tokens.pop();
        assert!(!capability.matches());
        capability.tokens.push(capability.tokens[0].clone());
        assert!(!capability.matches());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn native_backup_omits_private_membership_but_retains_guard_and_other_tables() {
        let client = client().await;
        let token = "57".repeat(32);
        client.query_bind_checked(
            "CREATE source_erasure_membership:private SET space_id='backup-space', token=$private_token; CREATE source_erasure_guard:backup SET space_id='backup-space', membership_count=1; CREATE backup_public_probe:kept SET space_id='backup-space', marker='retained-public-row';",
            serde_json::json!({"private_token": token}),
        ).await.unwrap();
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("backup");
        crate::snapshot::export_named(
            &client,
            &output,
            "private-membership-exclusion",
            &crate::snapshot::SnapshotRetention::default(),
        )
        .await
        .unwrap();
        let bytes = std::fs::read(output.join("data.snapshot")).unwrap();
        let sql = std::str::from_utf8(&bytes).unwrap();
        assert!(!sql.contains(&token));
        assert!(!sql.contains(&hex::encode(client.journal.reference_key_bytes())));
        assert!(sql.contains("source_erasure_guard"));
        assert!(sql.contains("retained-public-row"));
        assert!(crate::snapshot::verify(&output).await.is_ok());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn native_restore_installs_guards_on_historical_payload_tables_before_ready() {
        let client = client().await;
        let guard = client
            .begin_maintenance(
                super::super::RecoveryPhase::Restoring,
                std::time::Duration::from_secs(5),
            )
            .await
            .unwrap();
        let privileged = client.maintenance_client_with_guard(&guard);
        privileged.replace_snapshot_bytes(
            b"OPTION IMPORT; DEFINE TABLE historical_payload SCHEMALESS; CREATE historical_payload:old SET space_id='history', value='preserved'; OPTION IMPORT = FALSE;",
        ).await.unwrap();
        let erased = Thing::from(("historical_identity", Id::Number(42)));
        let tokens = crate::reference_fence::reference_tokens(
            client.journal.identity(),
            "history",
            client.journal.reference_key_bytes(),
            &Value::Thing(erased.clone()),
        )
        .unwrap();
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = BTreeMap::new();
        crate::reference_fence::append_reference_membership_sql(
            &mut sql,
            &mut bindings,
            "history",
            client.journal.identity(),
            client.journal.reference_key_fingerprint(),
            &tokens,
        )
        .unwrap();
        sql.push_str("COMMIT TRANSACTION;");
        privileged.query_bind_checked(&sql, bindings).await.unwrap();
        assert!(client.query_checked("RETURN 1").await.is_err());
        drop(privileged);
        guard.finish();
        assert!(client
            .query_bind_checked(
                "UPDATE historical_payload:old SET lost_reference=$erased;",
                crate::bindings! { "erased" => erased },
            )
            .await
            .is_err());
        let mut response = client
            .query_checked("SELECT VALUE value FROM historical_payload:old;")
            .await
            .unwrap();
        assert_eq!(response.take::<Vec<String>>(0).unwrap(), vec!["preserved"]);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn native_journal_anchor_requires_original_key_fingerprint() {
        let client = client().await;
        client.initialize_journal_identity().await.unwrap();
        client.verify_journal_identity().await.unwrap();
        client
            .query_checked(
                "UPDATE recall_durability:journal SET reference_key_fingerprint='substituted';",
            )
            .await
            .unwrap();
        assert!(client.verify_journal_identity().await.is_err());
        assert!(client.initialize_journal_identity().await.is_err());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn all_four_query_paths_return_typed_reservation_failure_without_partial_success() {
        let client = client().await;
        let sql = "BEGIN TRANSACTION; CREATE reservation_failure_probe:rolled_back SET visible=true; THROW 'publication:erasure_space_reserved'; COMMIT TRANSACTION;";
        let results = [
            client.query(sql).await,
            client.query_checked(sql).await,
            client.query_bind(sql, forged_bindings()).await,
            client.query_bind_checked(sql, forged_bindings()).await,
        ];
        for result in results {
            assert!(matches!(result, Err(StorageError::ErasureInProgress)));
        }
        let mut response = client
            .query_checked("SELECT VALUE id FROM reservation_failure_probe;")
            .await
            .unwrap();
        assert!(response.take::<Vec<Thing>>(0).unwrap().is_empty());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn unchecked_unrelated_statement_errors_and_positions_remain_available() {
        let client = client().await;
        let sql = "RETURN 11; THROW 'unrelated failure mentioning publication:erasure_space_reserved'; RETURN 22;";
        for mut response in [
            client.query(sql).await.unwrap(),
            client.query_bind(sql, forged_bindings()).await.unwrap(),
        ] {
            assert_eq!(response.num_statements(), 3);
            assert_eq!(response.take::<Option<i64>>(0).unwrap(), Some(11));
            assert!(response
                .take::<Option<i64>>(1)
                .unwrap_err()
                .to_string()
                .contains("unrelated failure"));
            assert_eq!(response.take::<Option<i64>>(2).unwrap(), Some(22));
        }
    }

    #[test]
    fn only_exact_native_or_remote_thrown_reservation_errors_are_promoted() {
        assert!(is_erasure_reservation_error(&surrealdb::Error::Db(
            surrealdb::error::Db::Thrown("publication:erasure_space_reserved".into()),
        )));
        assert!(is_erasure_reservation_error(&surrealdb::Error::Api(
            surrealdb::error::Api::Query(
                "An error occurred: publication:erasure_space_reserved".into()
            ),
        )));
        for message in [
            "query text mentions publication:erasure_space_reserved",
            "An error occurred: publication:erasure_space_reserved with suffix",
            "Parse error: publication:erasure_space_reserved",
        ] {
            assert!(!is_erasure_reservation_error(&surrealdb::Error::Api(
                surrealdb::error::Api::Query(message.into()),
            )));
            assert!(!is_erasure_reservation_error(&surrealdb::Error::Db(
                surrealdb::error::Db::Thrown(message.into()),
            )));
        }
    }
}
