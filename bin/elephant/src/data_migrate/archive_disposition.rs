//! Complete source accounting for the explicit operational archive disposition.
//! Original source declarations, including declared zero versus absence, survive.

use anyhow::{ensure, Context};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// The storage writer owns the closed class; this import prevents the CLI
// accounting boundary from silently drifting from the actual retained rows.
use elephant_storage::historical_operational_archive::OPERATIONAL_TABLES;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum OperationalDisposition {
    #[default]
    Native,
    ManagedArchiveV1,
}

impl OperationalDisposition {
    pub fn from_plan(plan: &serde_json::Value) -> anyhow::Result<Self> {
        match plan.get("operational_disposition") {
            None => Ok(Self::Native),
            Some(value) => Ok(serde_json::from_value(value.clone())?),
        }
    }

    pub fn archives(self, table: &str) -> bool {
        self == Self::ManagedArchiveV1 && OPERATIONAL_TABLES.contains(&table)
    }

    pub fn declared_archive_tables(self, source: &BTreeMap<String, u64>) -> BTreeMap<String, u64> {
        source
            .iter()
            .filter(|(table, _)| self.archives(table))
            .map(|(table, count)| (table.clone(), *count))
            .collect()
    }

    pub fn account(
        self,
        source: &BTreeMap<String, u64>,
        native: &BTreeMap<String, u64>,
        archived: &BTreeMap<String, u64>,
        retained_global: &BTreeMap<String, u64>,
    ) -> anyhow::Result<SourceAccounting> {
        ensure!(
            *archived == self.declared_archive_tables(source),
            "archive declarations differ from the complete source disposition"
        );
        for (table, count) in native {
            ensure!(
                !self.archives(table) || *count == 0,
                "archive row was also copied natively"
            );
            ensure!(
                source.contains_key(table) || *count == 0,
                "undeclared native source rows"
            );
        }
        for (table, count) in retained_global {
            ensure!(
                table == "migration_history" && source.get(table) == Some(count),
                "unknown or mismatched retained global source rows"
            );
        }
        let mut tables = BTreeMap::new();
        let mut rows = 0_u64;
        for (table, count) in source {
            let disposition = if table == "migration_history" {
                ensure!(
                    retained_global.get(table).copied().unwrap_or(0) == *count,
                    "retained global source count differs"
                );
                ensure!(
                    native.get(table).copied().unwrap_or(0) == 0,
                    "global source control was copied natively"
                );
                SourceDisposition::RecognizedGlobalRetainedControl
            } else if self.archives(table) {
                SourceDisposition::ManagedArchiveExactLexical
            } else {
                ensure!(
                    native.get(table).copied().unwrap_or(0) == *count,
                    "native source count differs for {table}"
                );
                SourceDisposition::NativeExactProjection
            };
            rows = rows
                .checked_add(*count)
                .context("source accounting count overflow")?;
            tables.insert(
                table.clone(),
                SourceTableAccounting {
                    rows: *count,
                    disposition: if *count == 0 {
                        SourceDisposition::DeclaredEmpty
                    } else {
                        disposition
                    },
                    declared_storage: disposition,
                },
            );
        }
        Ok(SourceAccounting {
            operational_disposition: self,
            rows,
            tables,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum SourceDisposition {
    DeclaredEmpty,
    NativeExactProjection,
    ManagedArchiveExactLexical,
    RecognizedGlobalRetainedControl,
}

#[derive(Debug, Serialize)]
pub(super) struct SourceTableAccounting {
    pub rows: u64,
    pub disposition: SourceDisposition,
    pub declared_storage: SourceDisposition,
}

#[derive(Debug, Serialize)]
pub(super) struct SourceAccounting {
    pub operational_disposition: OperationalDisposition,
    pub rows: u64,
    pub tables: BTreeMap<String, SourceTableAccounting>,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn map(rows: &[(&str, u64)]) -> BTreeMap<String, u64> {
        rows.iter()
            .map(|(name, n)| ((*name).to_owned(), *n))
            .collect()
    }
    #[test]
    fn disposition_is_explicit_closed_and_never_swallows_an_unknown_mode() -> anyhow::Result<()> {
        assert_eq!(
            OperationalDisposition::from_plan(&serde_json::json!({}))?,
            OperationalDisposition::Native
        );
        for value in [
            serde_json::Value::Null,
            serde_json::json!(true),
            serde_json::json!({"audit_log":"archive"}),
            serde_json::json!("archive"),
        ] {
            assert!(OperationalDisposition::from_plan(
                &serde_json::json!({"operational_disposition":value})
            )
            .is_err());
        }
        let mode = OperationalDisposition::from_plan(
            &serde_json::json!({"operational_disposition":"managed_archive_v1"}),
        )?;
        assert!(mode.archives("work_edge"));
        assert!(!mode.archives("source"));
        assert!(!mode.archives("unknown_authority"));
        Ok(())
    }
    #[test]
    fn accounting_preserves_declared_empty_and_refuses_missing_extra_or_double_rows(
    ) -> anyhow::Result<()> {
        let source = map(&[
            ("doc", 2),
            ("audit_log", 3),
            ("work_edge", 0),
            ("migration_history", 1),
        ]);
        let native = map(&[("doc", 2), ("audit_log", 0)]);
        let archive = map(&[("audit_log", 3), ("work_edge", 0)]);
        let global = map(&[("migration_history", 1)]);
        let mode = OperationalDisposition::ManagedArchiveV1;
        let result = mode.account(&source, &native, &archive, &global)?;
        assert_eq!(result.rows, 6);
        assert_eq!(result.tables["work_edge"].rows, 0);
        assert_eq!(
            result.tables["work_edge"].disposition,
            SourceDisposition::DeclaredEmpty
        );
        assert_eq!(
            result.tables["work_edge"].declared_storage,
            SourceDisposition::ManagedArchiveExactLexical
        );
        assert!(!result.tables.contains_key("work_deadletter"));
        assert_eq!(
            result.tables["audit_log"].disposition,
            SourceDisposition::ManagedArchiveExactLexical
        );
        for bad in [
            map(&[("audit_log", 3)]),
            map(&[("audit_log", 2), ("work_edge", 0)]),
            map(&[("audit_log", 3), ("work_edge", 0), ("work_deadletter", 0)]),
        ] {
            assert!(mode.account(&source, &native, &bad, &global).is_err());
        }
        assert!(mode
            .account(
                &source,
                &map(&[("doc", 2), ("audit_log", 3)]),
                &archive,
                &global
            )
            .is_err());
        assert!(mode
            .account(&source, &map(&[("doc", 1)]), &archive, &global)
            .is_err());
        assert!(mode
            .account(&source, &native, &archive, &BTreeMap::new())
            .is_err());
        assert!(OperationalDisposition::Native
            .account(&source, &native, &archive, &global)
            .is_err());
        assert!(OperationalDisposition::Native
            .account(
                &source,
                &map(&[("doc", 2), ("audit_log", 3), ("work_edge", 0)]),
                &BTreeMap::new(),
                &global
            )
            .is_ok());
        Ok(())
    }
}
