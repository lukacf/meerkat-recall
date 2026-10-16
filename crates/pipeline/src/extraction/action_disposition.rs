//! A model may suggest an exact action link. It cannot assert ownership or close
//! an action; authenticated disposition is a separate canonical mutation.
use crate::PipelineError;
use elephant_types::RecordId;
use serde::{Deserialize, Serialize};
/// Bounded model suggestion; its identity and quoted span need current validation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionDispositionProposal {
    /// Exact suggested existing requires_action assertion.
    pub target_action_id: RecordId,
    /// Exact subject of the suggested action.
    pub subject_entity_id: RecordId,
    /// Suggested closure reason, not an authenticated owner decision.
    pub reason: String,
    /// Node-local UTF-8 byte start.
    pub evidence_start: u32,
    /// Node-local exclusive UTF-8 byte end.
    pub evidence_end: u32,
    /// Exact contiguous source quote checked against immutable text.
    pub evidence_quote: String,
}
pub(crate) fn parse_action_disposition_proposals(
    structured: Option<&serde_json::Value>,
    text: &str,
) -> Result<Vec<ActionDispositionProposal>, PipelineError> {
    let parsed;
    let value = if let Some(value) = structured {
        value
    } else {
        parsed = match serde_json::from_str::<serde_json::Value>(text) {
            Ok(value) => value,
            Err(_) if text.contains("action_disposition_proposals") => {
                return Err(invalid("action proposal JSON invalid"))
            }
            Err(_) => return Ok(vec![]),
        };
        &parsed
    };
    let Some(value) = value.get("action_disposition_proposals") else {
        return Ok(vec![]);
    };
    let array = value
        .as_array()
        .ok_or_else(|| invalid("action proposal list invalid"))?;
    if array.len() > 16 {
        return Err(invalid("action proposal count exceeds bound"));
    }
    let mut results = Vec::new();
    let mut targets = std::collections::BTreeSet::new();
    for value in array {
        let proposal: ActionDispositionProposal = serde_json::from_value(value.clone())
            .map_err(|_| invalid("action proposal fields invalid"))?;
        if proposal.target_action_id.table() != elephant_types::TableName::Assertion
            || proposal.subject_entity_id.table() != elephant_types::TableName::Entity
            || proposal.reason.trim().is_empty()
            || proposal.reason.len() > 4096
            || proposal.evidence_quote.len() > 32768
            || proposal.evidence_start >= proposal.evidence_end
            || !targets.insert(proposal.target_action_id.clone())
        {
            return Err(invalid(
                "action proposal identity/evidence/ambiguity invalid",
            ));
        }
        results.push(proposal);
    }
    Ok(results)
}
fn invalid(message: &str) -> PipelineError {
    PipelineError::ValidationError(message.into())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn proposal_is_separate_bounded_and_cannot_claim_actor_authority() -> Result<(), PipelineError>
    {
        let action = RecordId::new_v4(elephant_types::TableName::Assertion);
        let subject = RecordId::new_v4(elephant_types::TableName::Entity);
        let row = serde_json::json!({"target_action_id":action,"subject_entity_id":subject,"reason":"Owner says closed","evidence_start":0,"evidence_end":6,"evidence_quote":"closed"});
        let value =
            serde_json::json!({"assertions":[],"action_disposition_proposals":[row.clone()]});
        assert_eq!(
            parse_action_disposition_proposals(Some(&value), "")?.len(),
            1
        );
        let mut forged = row.clone();
        forged["recorded_by"] = serde_json::json!("identity:owner");
        assert!(parse_action_disposition_proposals(
            Some(&serde_json::json!({"action_disposition_proposals":[forged]})),
            ""
        )
        .is_err());
        assert!(parse_action_disposition_proposals(
            Some(&serde_json::json!({"action_disposition_proposals":[row.clone(),row.clone()]})),
            ""
        )
        .is_err());
        assert!(parse_action_disposition_proposals(
            Some(&serde_json::json!({"action_disposition_proposals":vec![row;17]})),
            ""
        )
        .is_err());
        assert!(parse_action_disposition_proposals(
            Some(&serde_json::json!({"assertions":[]})),
            ""
        )?
        .is_empty());
        Ok(())
    }
}
