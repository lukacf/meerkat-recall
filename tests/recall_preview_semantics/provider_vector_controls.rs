//! Controlled fixture shape checks. These never assert provider relevance.
use super::spy;

#[test]
fn punctuation_only_original_inputs_remain_nonempty_nonzero_and_deterministic() -> anyhow::Result<()>
{
    for dimensions in [8, 12, 384] {
        for text in ["* * *", "—…", "\n\t ", "***\n***"] {
            let original = elephant_storage::hashed_text_embedding(text, dimensions);
            anyhow::ensure!(original.iter().all(|value| *value == 0.0));
            let mut first = original.clone();
            spy::nonzero_controlled_vector(text, &mut first)?;
            let mut second = original;
            spy::nonzero_controlled_vector(text, &mut second)?;
            anyhow::ensure!(first == second && first.len() == dimensions);
            anyhow::ensure!(first.iter().all(|value| value.is_finite()));
            anyhow::ensure!(
                first
                    .iter()
                    .filter(|value| value.to_bits() == 1.0_f32.to_bits())
                    .count()
                    == 1
            );
        }
        let text = "Alice followed the white rabbit";
        let original = elephant_storage::hashed_text_embedding(text, dimensions);
        anyhow::ensure!(original.iter().any(|value| *value != 0.0));
        let mut after = original.clone();
        spy::nonzero_controlled_vector(text, &mut after)?;
        anyhow::ensure!(after == original, "original word-vector ranking changed");
    }
    Ok(())
}

#[test]
fn invalid_controlled_inputs_or_vectors_are_not_repaired() {
    assert!(spy::nonzero_controlled_vector("", &mut [0.0; 8]).is_err());
    assert!(spy::nonzero_controlled_vector("nonempty", &mut []).is_err());
    assert!(spy::nonzero_controlled_vector("nonempty", &mut [f32::NAN]).is_err());
    assert!(spy::nonzero_controlled_vector("nonempty", &mut [f32::INFINITY]).is_err());
}
