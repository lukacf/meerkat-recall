//! Versioned full-input segmentation and pooling shared by the concrete providers.

use super::{EmbeddingError, EmbeddingResult, EmbeddingVector};

pub(super) const INITIAL_SEGMENT_BYTES: usize = 2048;
pub(super) const MAX_SPLIT_DEPTH: u8 = 16;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Preprocessing {
    Exact,
    Full,
    BgeEnRetrieval,
}

impl Preprocessing {
    pub(super) fn parse(value: &str) -> EmbeddingResult<Self> {
        match value {
            "text-v1" => Ok(Self::Exact),
            "text-full-v2" => Ok(Self::Full),
            elephant_types::provider_binding::BGE_EN_RETRIEVAL_V1 => Ok(Self::BgeEnRetrieval),
            _ => Err(invalid("unsupported embedding preprocessing")),
        }
    }

    #[cfg(feature = "fastembed")]
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "text-v1",
            Self::Full => "text-full-v2",
            Self::BgeEnRetrieval => elephant_types::provider_binding::BGE_EN_RETRIEVAL_V1,
        }
    }
}

/// Prepare role-specific model inputs before counting tokens. The operation is
/// pinned to this request's dispatch, never cached with the model resource.
#[cfg(feature = "fastembed")]
pub(super) fn local_inputs(
    texts: &[String],
    preprocessing: Preprocessing,
    operation: Option<elephant_types::provider_binding::ModelOperation>,
) -> EmbeddingResult<Vec<String>> {
    use elephant_types::provider_binding::{ModelOperation, BGE_EN_RETRIEVAL_QUERY_INSTRUCTION};
    if preprocessing != Preprocessing::BgeEnRetrieval {
        return Ok(texts.to_vec());
    }
    match operation {
        Some(ModelOperation::QueryEmbedding) => texts
            .iter()
            .map(|text| {
                let capacity = BGE_EN_RETRIEVAL_QUERY_INSTRUCTION
                    .len()
                    .checked_add(text.len())
                    .ok_or_else(|| invalid("BGE query input length overflow"))?;
                let mut input = String::with_capacity(capacity);
                input.push_str(BGE_EN_RETRIEVAL_QUERY_INSTRUCTION);
                input.push_str(text);
                Ok(input)
            })
            .collect(),
        Some(ModelOperation::DocumentEmbedding | ModelOperation::ReindexEmbedding) => {
            Ok(texts.to_vec())
        }
        _ => Err(invalid(
            "BGE retrieval requires an approved embedding operation",
        )),
    }
}

/// Count with the approved, untruncated tokenizer including special tokens.
/// Fitting changes only the explicit v2 representation, never v1 input bytes.
#[cfg(feature = "fastembed")]
pub(super) fn local_segments(
    texts: &[String],
    preprocessing: Preprocessing,
    limit: usize,
    mut count: impl FnMut(&str) -> EmbeddingResult<usize>,
) -> EmbeddingResult<Vec<(Segment, u64)>> {
    if limit == 0 {
        return Err(invalid("local tokenizer input limit is zero"));
    }
    if preprocessing == Preprocessing::Full {
        require_nonempty_inputs(texts)?;
    }
    let mut pending: std::collections::VecDeque<_> = match preprocessing {
        Preprocessing::Full => segments(texts).into(),
        Preprocessing::Exact | Preprocessing::BgeEnRetrieval => texts
            .iter()
            .enumerate()
            .map(|(input, text)| Segment {
                input,
                start: 0,
                end: text.len(),
                text: text.clone(),
                depth: 0,
            })
            .collect(),
    };
    let mut accepted = Vec::new();
    while let Some(segment) = pending.pop_front() {
        let tokens = count(&segment.text)?;
        if tokens <= limit {
            accepted.push((
                segment,
                u64::try_from(tokens).map_err(|_| invalid("token count overflow"))?,
            ));
        } else if preprocessing == Preprocessing::Full {
            let [first, second] = segment.split()?;
            pending.push_front(second);
            pending.push_front(first);
        } else {
            return Err(EmbeddingError::InputTooLarge {
                preprocessing: preprocessing.as_str(),
            });
        }
    }
    Ok(accepted)
}

pub(super) fn invalid(reason: &str) -> EmbeddingError {
    EmbeddingError::ProviderError(format!("embedding input: {reason}"))
}

pub(super) fn require_nonempty_inputs(texts: &[String]) -> EmbeddingResult<()> {
    if texts.iter().any(String::is_empty) {
        return Err(invalid("text-full-v2 requires nonempty individual inputs"));
    }
    Ok(())
}

/// Byte coordinates always refer to one original input. Splits preserve both
/// halves and never infer coverage from a provider's response cardinality alone.
#[derive(Clone, Debug)]
pub(super) struct Segment {
    pub(super) input: usize,
    pub(super) start: usize,
    pub(super) end: usize,
    pub(super) text: String,
    depth: u8,
}

impl Segment {
    pub(super) fn split(self) -> EmbeddingResult<[Self; 2]> {
        if self.depth >= MAX_SPLIT_DEPTH {
            return Err(invalid("full-input fitting exceeded its split depth"));
        }
        let mut at = self.text.len() / 2;
        while at > 0 && !self.text.is_char_boundary(at) {
            at -= 1;
        }
        if at == 0 {
            at = self
                .text
                .char_indices()
                .nth(1)
                .map_or(self.text.len(), |(index, _)| index);
        }
        if at == 0 || at >= self.text.len() {
            return Err(invalid(
                "provider cannot accept one complete Unicode scalar",
            ));
        }
        let middle = self.start + at;
        let depth = self.depth + 1;
        Ok([
            Self {
                input: self.input,
                start: self.start,
                end: middle,
                text: self.text[..at].into(),
                depth,
            },
            Self {
                input: self.input,
                start: middle,
                end: self.end,
                text: self.text[at..].into(),
                depth,
            },
        ])
    }
}

pub(super) fn segments(texts: &[String]) -> Vec<Segment> {
    let mut result = Vec::new();
    for (input, text) in texts.iter().enumerate() {
        if text.is_empty() {
            result.push(Segment {
                input,
                start: 0,
                end: 0,
                text: String::new(),
                depth: 0,
            });
            continue;
        }
        let mut start = 0;
        while start < text.len() {
            let mut end = start.saturating_add(INITIAL_SEGMENT_BYTES).min(text.len());
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            result.push(Segment {
                input,
                start,
                end,
                text: text[start..end].into(),
                depth: 0,
            });
            start = end;
        }
    }
    result
}

pub(super) fn validate_vectors(
    vectors: &[EmbeddingVector],
    count: usize,
    dimensions: usize,
) -> EmbeddingResult<()> {
    if dimensions == 0
        || vectors.len() != count
        || vectors.iter().any(|vector| {
            vector.len() != dimensions
                || vector.iter().any(|value| !value.is_finite())
                || vector.iter().all(|value| *value == 0.0)
        })
    {
        return Err(invalid(
            "provider returned invalid embedding shape or values",
        ));
    }
    Ok(())
}

/// No output is exposed until every original byte has contributed exactly once.
/// Each original input has a separate weighted sum; batching never mixes them.
pub(super) struct Accumulator {
    lengths: Vec<usize>,
    consumed: Vec<usize>,
    complete: Vec<bool>,
    sums: Vec<Vec<f64>>,
    dimensions: usize,
}

impl Accumulator {
    pub(super) fn new(texts: &[String], dimensions: usize) -> EmbeddingResult<Self> {
        require_nonempty_inputs(texts)?;
        if dimensions == 0 {
            return Err(invalid("embedding dimensions must be positive"));
        }
        Ok(Self {
            lengths: texts.iter().map(String::len).collect(),
            consumed: vec![0; texts.len()],
            complete: vec![false; texts.len()],
            sums: vec![vec![0.0; dimensions]; texts.len()],
            dimensions,
        })
    }

    #[allow(clippy::cast_precision_loss)] // Inputs are already resident byte arrays.
    pub(super) fn add(&mut self, segment: &Segment, vector: &[f32]) -> EmbeddingResult<()> {
        let input = segment.input;
        if input >= self.lengths.len()
            || self.complete[input]
            || segment.start != self.consumed[input]
            || segment.end < segment.start
            || segment.end > self.lengths[input]
            || segment.text.len() != segment.end - segment.start
            || (segment.text.is_empty() && self.lengths[input] != 0)
            || vector.len() != self.dimensions
            || vector.iter().any(|value| !value.is_finite())
            || vector.iter().all(|value| *value == 0.0)
        {
            return Err(invalid("full-input segment coverage or vector differs"));
        }
        let weight = segment.text.len() as f64;
        for (sum, value) in self.sums[input].iter_mut().zip(vector) {
            *sum += f64::from(*value) * weight;
            if !sum.is_finite() {
                return Err(invalid("full-input vector accumulation overflow"));
            }
        }
        self.consumed[input] = segment.end;
        self.complete[input] = segment.end == self.lengths[input];
        Ok(())
    }

    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
    pub(super) fn finish(mut self) -> EmbeddingResult<Vec<EmbeddingVector>> {
        if self.complete.iter().any(|complete| !complete) || self.consumed != self.lengths {
            return Err(invalid("full-input coverage is incomplete"));
        }
        let mut output = Vec::with_capacity(self.sums.len());
        for (sum, bytes) in self.sums.iter_mut().zip(self.lengths) {
            let denominator = bytes as f64;
            for component in &mut *sum {
                *component /= denominator;
            }
            let norm = sum.iter().map(|value| value * value).sum::<f64>().sqrt();
            if !norm.is_finite() || norm == 0.0 {
                return Err(invalid("full-input vector has zero or invalid norm"));
            }
            let normalized: Vec<f32> = sum.iter().map(|value| (value / norm) as f32).collect();
            if normalized.iter().any(|value| !value.is_finite()) {
                return Err(invalid("full-input vector normalization overflow"));
            }
            output.push(normalized);
        }
        Ok(output)
    }
}
