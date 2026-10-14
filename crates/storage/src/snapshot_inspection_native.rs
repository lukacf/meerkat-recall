//! Pinned SurrealQL AST traversal, never query execution.
use super::{
    incomplete, BTreeMap, BTreeSet, Budget, Id, InspectedSnapshotIdentity, InspectedSnapshotSchema,
    Inventory, NativeInspectionValue, SnapshotInspectionLimits, StorageResult, Thing, Value,
};
use surrealdb::sql::statements::DefineStatement;
use surrealdb::sql::{Data, Function, Number, Statement};
use surrealdb::syn::parser::{ParserSettings, StatementStream};

pub(super) fn inspect(bytes: &[u8], inventory: &mut Inventory<'_>) -> StorageResult<()> {
    let input = std::str::from_utf8(bytes).map_err(|_| incomplete("native_utf8"))?;
    lexical_bounds(input, inventory.budget.limits)?;
    let settings = ParserSettings {
        object_recursion_limit: inventory.budget.limits.depth,
        query_recursion_limit: inventory.budget.limits.depth,
        legacy_strands: false,
        ..ParserSettings::default()
    };
    let mut parser = StatementStream::new_with_settings(settings);
    let mut remaining = bytes.into();
    let mut offset = 0usize;
    loop {
        let before = input.len() - offset;
        let statement = parser
            .parse_complete(&mut remaining)
            .map_err(|_| incomplete("native_parse"))?;
        let consumed = before - remaining.len();
        let raw = input
            .get(offset..offset + consumed)
            .ok_or_else(|| incomplete("native_statement_boundary"))?;
        offset += consumed;
        let Some(statement) = statement else {
            break;
        };
        inventory.report.statement_count += 1;
        if inventory.report.statement_count > inventory.budget.limits.statements as u64 {
            return Err(incomplete("statement_limit"));
        }
        match statement {
            Statement::Option(option) if option.name.0 == "IMPORT" && option.what => {}
            Statement::Define(definition) => {
                if let DefineStatement::Table(table) = &definition {
                    inventory.table(&table.name.0)?;
                }
                // Schema expressions remain original bounded text. Parsing is
                // structural only; expression-bearing defaults/events are never
                // evaluated or silently discarded from the physical inventory.
                let schema = InspectedSnapshotSchema {
                    sha256: crate::snapshot::digest(raw.as_bytes()),
                    statement: inventory
                        .budget
                        .limits
                        .include_records
                        .then(|| raw.to_owned()),
                };
                inventory.charge_report(&schema)?;
                inventory.report.schema_statements.push(schema);
            }
            Statement::Insert(insert) => {
                if insert.into.is_some()
                    || insert.ignore
                    || insert.update.is_some()
                    || insert.output.is_some()
                    || insert.timeout.is_some()
                    || insert.parallel
                    || insert.version.is_some()
                {
                    return Err(incomplete("unsupported_native_insert"));
                }
                // Native Object parsing overwrites duplicate keys. Inspect every
                // original object key before trusting the parsed object tree.
                reject_duplicate_keys(raw, inventory.budget.limits)?;
                let Data::SingleExpression(Value::Array(rows)) = insert.data else {
                    return Err(incomplete("unsupported_native_insert_data"));
                };
                if rows.len()
                    > inventory
                        .budget
                        .limits
                        .rows
                        .saturating_sub(inventory.report.rows.len())
                {
                    return Err(incomplete("row_limit"));
                }
                for row in rows.0 {
                    let Value::Object(object) = &row else {
                        return Err(incomplete("non_object_row"));
                    };
                    let Some(Value::Thing(id)) = object.get("id") else {
                        return Err(incomplete("native_identity_shape"));
                    };
                    let id = id.clone();
                    let record = tagged(&row, &mut inventory.budget, 0)?;
                    inventory.row(id, record)?;
                }
            }
            _ => return Err(incomplete("unsupported_native_statement")),
        }
    }
    Ok(())
}

pub(super) fn identity(
    id: &Thing,
    budget: &mut Budget<'_>,
    depth: usize,
) -> StorageResult<InspectedSnapshotIdentity> {
    budget.node(depth)?;
    budget.string(&id.tb)?;
    let key = match &id.id {
        Id::String(value) => {
            budget.string(value)?;
            NativeInspectionValue::String(value.clone())
        }
        Id::Number(value) => NativeInspectionValue::Int(*value),
        Id::Uuid(value) => NativeInspectionValue::Uuid(value.0.to_string()),
        Id::Array(value) => tagged(&Value::Array(value.clone()), budget, depth + 1)?,
        Id::Object(value) => tagged(&Value::Object(value.clone()), budget, depth + 1)?,
        _ => return Err(incomplete("unsupported_native_identity")),
    };
    Ok(InspectedSnapshotIdentity {
        table: id.tb.clone(),
        id: key,
    })
}

fn tagged(
    value: &Value,
    budget: &mut Budget<'_>,
    depth: usize,
) -> StorageResult<NativeInspectionValue> {
    budget.node(depth)?;
    Ok(match value {
        Value::None => NativeInspectionValue::None,
        Value::Null => NativeInspectionValue::Null,
        Value::Bool(value) => NativeInspectionValue::Bool(*value),
        Value::Strand(value) => {
            budget.string(&value.0)?;
            NativeInspectionValue::String(value.0.clone())
        }
        Value::Number(Number::Int(value)) => NativeInspectionValue::Int(*value),
        Value::Number(Number::Float(value)) => {
            NativeInspectionValue::FloatBits(format!("{:016x}", value.to_bits()))
        }
        Value::Number(Number::Decimal(value)) => NativeInspectionValue::Decimal(value.to_string()),
        Value::Uuid(value) => NativeInspectionValue::Uuid(value.0.to_string()),
        Value::Datetime(value) => NativeInspectionValue::Datetime(
            value.0.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        ),
        Value::Duration(value) => NativeInspectionValue::Duration {
            seconds: value.0.as_secs(),
            nanoseconds: value.0.subsec_nanos(),
        },
        Value::Bytes(value) => {
            if value.len() > budget.limits.string_bytes {
                return Err(incomplete("bytes_limit"));
            }
            NativeInspectionValue::Bytes(hex::encode(value.as_slice()))
        }
        Value::Thing(value) => {
            NativeInspectionValue::Thing(Box::new(identity(value, budget, depth + 1)?))
        }
        Value::Array(values) => NativeInspectionValue::Array(
            values
                .iter()
                .map(|value| tagged(value, budget, depth + 1))
                .collect::<StorageResult<_>>()?,
        ),
        Value::Object(values) => {
            let mut result = BTreeMap::new();
            for (key, value) in values.iter() {
                budget.string(key)?;
                result.insert(key.clone(), tagged(value, budget, depth + 1)?);
            }
            NativeInspectionValue::Map(result)
        }
        Value::Function(function) => {
            // This is the pinned export spelling of a stored Bytes literal.
            // Decode its one literal argument locally; never invoke the builtin.
            let Function::Normal(name, args) = function.as_ref() else {
                return Err(incomplete("unsupported_native_expression"));
            };
            let [Value::Strand(encoded)] = args.as_slice() else {
                return Err(incomplete("unsupported_native_expression"));
            };
            if name != "encoding::base64::decode" {
                return Err(incomplete("unsupported_native_expression"));
            }
            budget.string(&encoded.0)?;
            NativeInspectionValue::Bytes(hex::encode(decode_base64(&encoded.0)?))
        }
        _ => return Err(incomplete("unsupported_native_value")),
    })
}

fn decode_base64(text: &str) -> StorageResult<Vec<u8>> {
    let bytes = text.as_bytes();
    if bytes.len() % 4 == 1 {
        return Err(incomplete("invalid_native_bytes"));
    }
    let mut output = Vec::with_capacity(bytes.len() / 4 * 3 + 2);
    let mut accumulator = 0u32;
    let mut bits = 0u32;
    for &byte in bytes {
        let digit = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return Err(incomplete("invalid_native_bytes")),
        };
        accumulator = (accumulator << 6) | u32::from(digit);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            output.push((accumulator >> bits).to_le_bytes()[0]);
            accumulator &= (1 << bits) - 1;
        }
    }
    if accumulator != 0 {
        return Err(incomplete("invalid_native_bytes"));
    }
    Ok(output)
}

// A small lexical preflight bounds recursion and encoded strings before the
// pinned parser allocates them. It understands export quotes/comments only;
// unfamiliar encodings fail closed rather than being heuristically evaluated.
fn lexical_bounds(input: &str, limits: &SnapshotInspectionLimits) -> StorageResult<()> {
    let mut cursor = Cursor {
        input,
        offset: 0,
        limits,
    };
    let mut depth = 0usize;
    let mut tokens = 0usize;
    while let Some(token) = cursor.next()? {
        tokens += 1;
        if tokens > limits.nodes {
            return Err(incomplete("native_token_limit"));
        }
        match token {
            Token::Punct(b'{' | b'[' | b'(') => {
                depth += 1;
                if depth > limits.depth {
                    return Err(incomplete("value_depth_limit"));
                }
            }
            Token::Punct(b'}' | b']' | b')') => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| incomplete("native_delimiters"))?;
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(incomplete("native_delimiters"));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Token<'a> {
    Punct(u8),
    Text(&'a str, bool),
}
struct Cursor<'a> {
    input: &'a str,
    offset: usize,
    limits: &'a SnapshotInspectionLimits,
}
impl<'a> Cursor<'a> {
    fn next(&mut self) -> StorageResult<Option<Token<'a>>> {
        let bytes = self.input.as_bytes();
        loop {
            while bytes.get(self.offset).is_some_and(u8::is_ascii_whitespace) {
                self.offset += 1;
            }
            if bytes.get(self.offset..self.offset + 2) == Some(b"--")
                || bytes.get(self.offset..self.offset + 2) == Some(b"//")
            {
                while bytes.get(self.offset).is_some_and(|byte| *byte != b'\n') {
                    self.offset += 1;
                }
                continue;
            }
            if bytes.get(self.offset..self.offset + 2) == Some(b"/*") {
                let start = self.offset + 2;
                let Some(end) = self.input[start..].find("*/") else {
                    return Err(incomplete("native_comment"));
                };
                self.offset = start + end + 2;
                continue;
            }
            break;
        }
        let Some(&byte) = bytes.get(self.offset) else {
            return Ok(None);
        };
        let start = self.offset;
        if matches!(byte, b'\'' | b'"' | b'`') {
            self.offset += 1;
            let mut escaped = false;
            while let Some(&current) = bytes.get(self.offset) {
                if self.offset - (start + 1) > self.limits.string_bytes {
                    return Err(incomplete("string_limit"));
                }
                self.offset += 1;
                if escaped {
                    escaped = false;
                } else if current == b'\\' {
                    escaped = true;
                } else if current == byte {
                    return Ok(Some(Token::Text(&self.input[start..self.offset], true)));
                }
            }
            return Err(incomplete("native_quote"));
        }
        if byte.is_ascii_alphanumeric() || byte == b'_' || byte >= 128 {
            self.offset += 1;
            while bytes
                .get(self.offset)
                .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b >= 128)
            {
                self.offset += 1;
            }
            if self.offset - start > self.limits.string_bytes {
                return Err(incomplete("string_limit"));
            }
            let word = self
                .input
                .get(start..self.offset)
                .ok_or_else(|| incomplete("native_token"))?;
            return Ok(Some(Token::Text(word, false)));
        }
        self.offset += 1;
        Ok(Some(Token::Punct(byte)))
    }
}

fn reject_duplicate_keys(input: &str, limits: &SnapshotInspectionLimits) -> StorageResult<()> {
    struct Frame {
        object: bool,
        keys: BTreeSet<String>,
        expects_key: bool,
        candidate: Option<String>,
    }
    let mut stack: Vec<Frame> = Vec::new();
    let mut cursor = Cursor {
        input,
        offset: 0,
        limits,
    };
    while let Some(token) = cursor.next()? {
        match token {
            Token::Punct(b'{' | b'[' | b'(') => {
                let object = matches!(token, Token::Punct(b'{'));
                stack.push(Frame {
                    object,
                    keys: BTreeSet::new(),
                    expects_key: object,
                    candidate: None,
                });
            }
            Token::Punct(b'}' | b']' | b')') => {
                stack.pop().ok_or_else(|| incomplete("native_delimiters"))?;
            }
            Token::Punct(b',') => {
                if let Some(frame) = stack.last_mut() {
                    frame.expects_key = frame.object;
                    frame.candidate = None;
                }
            }
            Token::Punct(b':') => {
                if let Some(frame) = stack
                    .last_mut()
                    .filter(|frame| frame.object && frame.expects_key)
                {
                    let key = frame
                        .candidate
                        .take()
                        .ok_or_else(|| incomplete("unsupported_native_object_key"))?;
                    if !frame.keys.insert(key) {
                        return Err(incomplete("duplicate_map_key"));
                    }
                    frame.expects_key = false;
                }
            }
            Token::Text(text, quoted) => {
                if let Some(frame) = stack
                    .last_mut()
                    .filter(|frame| frame.object && frame.expects_key)
                {
                    if frame.candidate.is_some() {
                        return Err(incomplete("unsupported_native_object_key"));
                    }
                    let key = if quoted {
                        if text.starts_with('`') {
                            return Err(incomplete("unsupported_native_object_key"));
                        }
                        let Value::Strand(key) = surrealdb::syn::value(text)
                            .map_err(|_| incomplete("native_key_parse"))?
                        else {
                            return Err(incomplete("native_key_parse"));
                        };
                        key.0
                    } else {
                        text.to_owned()
                    };
                    frame.candidate = Some(key);
                }
            }
            Token::Punct(_) => {}
        }
    }
    if !stack.is_empty() {
        return Err(incomplete("native_delimiters"));
    }
    Ok(())
}
