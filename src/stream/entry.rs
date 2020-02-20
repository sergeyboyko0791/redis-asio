use crate::{RedisValue, RedisResult, RedisError, RedisErrorKind, FromRedisValue, from_redis_value};
use std::num::ParseIntError;
use std::error::Error;
use std::fmt;
use std::collections::HashMap;

#[derive(Clone, PartialEq, PartialOrd)]
pub struct EntryId((u64, u64));

/// Structure that wraps a entry received on XREAD/XREADGROUP request.
#[derive(Clone, PartialEq)]
pub struct StreamEntry {
    /// Stream name
    pub stream: String,
    /// Stream entry id is a simple string "milliseconds-id"
    pub id: EntryId,
    /// Note Redis allows to use key as a binary Bulk String
    /// but in the library it is forbidden for easy of use API.
    /// Value may be any of the RedisValue types
    pub values: HashMap<String, RedisValue>,
}

/// Structure that wraps an range entry received on XRANGE request.
#[derive(Clone, PartialEq)]
pub struct RangeEntry {
    /// Stream entry id is a simple string "milliseconds-id"
    pub id: EntryId,
    /// Note Redis allows to use key as a binary Bulk String
    /// but in the library it is forbidden for easy of use API.
    /// Value may be any of the RedisValue types
    pub values: HashMap<String, RedisValue>,
}

impl StreamEntry {
    pub(crate) fn new(stream: String, id: EntryId, values: HashMap<String, RedisValue>) -> Self {
        StreamEntry {
            stream,
            id,
            values,
        }
    }
}

impl RangeEntry {
    pub(crate) fn new(id: EntryId, values: HashMap<String, RedisValue>) -> Self {
        RangeEntry {
            id,
            values,
        }
    }
}

/// Parse XREAD/XREADGROUP result: RedisValue to vec of StreamEntry
pub(crate) fn parse_stream_entries(value: RedisValue) -> RedisResult<Vec<StreamEntry>> {
    // usually count of entries within one stream is 1,
    // because in finally case we subscribe on only new messages
    const LEN_FACTOR: usize = 1;

    let streams: Vec<StreamInfo> = from_redis_value(&value)?;

    let capacity = streams.len() * LEN_FACTOR;
    let mut stream_entries: Vec<StreamEntry> = Vec::with_capacity(capacity);

    // transform Vec<EntryInfo> to Vec<StreamEntry>
    for StreamInfo { id, entries } in streams.into_iter() {
        for entry in entries.into_iter() {
            let stream_entry =
                StreamEntry::new(id.clone(), EntryId::from_string(entry.id)?, entry.key_values);

            stream_entries.push(stream_entry);
        }
    }

    Ok(stream_entries)
}

/// Parse XRANGE result: RedisValue to vec of StreamEntry
pub fn parse_range_entries(value: RedisValue) -> RedisResult<Vec<RangeEntry>> {
    let entries: Vec<EntryInfo> = from_redis_value(&value)?;

    let mut result_entries: Vec<RangeEntry> = Vec::with_capacity(entries.len());

    // transform the Vec<EntryInfo> to Vec<RangeEntry>
    for entry in entries.into_iter() {
        let entry =
            RangeEntry::new(EntryId::from_string(entry.id)?, entry.key_values);

        result_entries.push(entry);
    }

    Ok(result_entries)
}

/// Internal structure is used to parse RedisValue into StreamEntry
struct StreamInfo {
    id: String,
    entries: Vec<EntryInfo>,
}

/// Internal structure is used to parse RedisValue into StreamEntry
#[derive(Debug)]
struct EntryInfo {
    id: String,
    key_values: HashMap<String, RedisValue>,
}

#[derive(Clone)]
pub enum RangeType {
    Any,
    GreaterThan(EntryId),
    LessThan(EntryId),
    GreaterLessThan(EntryId, EntryId),
}

impl RangeType {
    /// Check if the left bound is less than the right bound
    pub fn is_valid(&self) -> bool {
        match self {
            RangeType::GreaterLessThan(left, right) => left < right,
            _ => true
        }
    }

    pub(crate) fn to_left_right(&self) -> (String, String) {
        match self {
            RangeType::Any => ("-".to_string(), "+".to_string()),
            RangeType::GreaterThan(left) => (left.to_string(), "+".to_string()),
            RangeType::LessThan(right) => ("-".to_string(), right.to_string()),
            RangeType::GreaterLessThan(left, right) => (left.to_string(), right.to_string()),
        }
    }
}

impl fmt::Debug for StreamEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(stream={}, id=\"{:?}\", {:?})", self.stream, self.id, self.values)?;
        Ok(())
    }
}

impl fmt::Debug for RangeEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(id=\"{:?}\", {:?})", self.id, self.values)?;
        Ok(())
    }
}

impl fmt::Debug for EntryId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())?;
        Ok(())
    }
}

impl EntryId {
    pub fn new(ms: u64, id: u64) -> EntryId {
        EntryId((ms, id))
    }

    // Parse the Redis Stream Entry as pair: <milliseconds, id>
    pub(crate) fn from_string(id: String) -> RedisResult<EntryId> {
        const ENTRY_ID_CHUNK_LEN: usize = 2;
        const ENTRY_ID_MS_POS: usize = 0;
        const ENTRY_ID_ID_POS: usize = 1;

        let tokens: Vec<&str>
            = id.split('-').filter(|token| !token.is_empty()).collect();
        if tokens.len() != ENTRY_ID_CHUNK_LEN {
            return Err(
                RedisError::new(
                    RedisErrorKind::ParseError,
                    format!("Couldn't parse a Redis entry id: {:?}", &id))
            );
        }

        let ms = tokens[ENTRY_ID_MS_POS].parse::<u64>().map_err(&to_redis_error)?;
        let id = tokens[ENTRY_ID_ID_POS].parse::<u64>().map_err(&to_redis_error)?;
        Ok(Self((ms, id)))
    }

    pub fn to_string(&self) -> String {
        format!("{}-{}", (self.0).0, (self.0).1)
    }
}

fn to_redis_error(err: ParseIntError) -> RedisError {
    RedisError::new(RedisErrorKind::ParseError, err.description().to_string())
}

impl FromRedisValue for EntryInfo {
    fn from_redis_value(value: &RedisValue) -> RedisResult<Self> {
        let (id, key_values): (String, HashMap<String, RedisValue>) = from_redis_value(value)?;

        Ok(EntryInfo { id, key_values })
    }
}

impl FromRedisValue for StreamInfo {
    fn from_redis_value(value: &RedisValue) -> RedisResult<Self> {
        let (id, entries): (String, Vec<EntryInfo>) = from_redis_value(value)?;
        Ok(Self { id, entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_test_parse_stream_entry() {
        let entry1 = RedisValue::Array(vec![
            RedisValue::BulkString(b"1581870410019-0".to_vec()),
            RedisValue::Array(vec![
                RedisValue::BulkString(b"1key1".to_vec()),
                RedisValue::BulkString(b"1value1".to_vec()),
                RedisValue::BulkString(b"1key2".to_vec()),
                RedisValue::Int(2)
            ])
        ]);

        let entry2 = RedisValue::Array(vec![
            RedisValue::BulkString(b"1581870414714-0".to_vec()),
            RedisValue::Array(vec![
                RedisValue::BulkString(b"2key1".to_vec()),
                RedisValue::BulkString(b"2value1".to_vec()),
                RedisValue::BulkString(b"2key2".to_vec()),
                RedisValue::BulkString(b"2value2".to_vec()),
                RedisValue::BulkString(b"2key3".to_vec()),
                RedisValue::BulkString(b"2value3".to_vec())
            ])
        ]);

        let entry3 = RedisValue::Array(vec![
            RedisValue::BulkString(b"1581855076637-0".to_vec()),
            RedisValue::Array(vec![
                RedisValue::BulkString(b"3key1".to_vec()),
                RedisValue::BulkString(b"3value1".to_vec())
            ])
        ]);

        let stream1 = RedisValue::Array(vec![
            RedisValue::BulkString(b"stream1".to_vec()),
            RedisValue::Array(vec![
                entry1,
                entry2
            ])
        ]);

        let stream2 = RedisValue::Array(vec![
            RedisValue::BulkString(b"stream2".to_vec()),
            RedisValue::Array(vec![entry3])
        ]);

        let value = RedisValue::Array(vec![stream1, stream2]);

        let result = parse_stream_entries(value).unwrap();

        let mut entry1: HashMap<String, RedisValue> = HashMap::new();
        entry1.insert("1key1".to_string(), RedisValue::BulkString(b"1value1".to_vec()));
        entry1.insert("1key2".to_string(), RedisValue::Int(2));

        let mut entry2: HashMap<String, RedisValue> = HashMap::new();
        entry2.insert("2key1".to_string(), RedisValue::BulkString(b"2value1".to_vec()));
        entry2.insert("2key2".to_string(), RedisValue::BulkString(b"2value2".to_vec()));
        entry2.insert("2key3".to_string(), RedisValue::BulkString(b"2value3".to_vec()));

        let mut entry3: HashMap<String, RedisValue> = HashMap::new();
        entry3.insert("3key1".to_string(), RedisValue::BulkString(b"3value1".to_vec()));

        let origin = vec![
            StreamEntry::new("stream1".to_string(), EntryId((1581870410019, 0)), entry1),
            StreamEntry::new("stream1".to_string(), EntryId((1581870414714, 0)), entry2),
            StreamEntry::new("stream2".to_string(), EntryId((1581855076637, 0)), entry3)
        ];

        assert_eq!(origin, result);
    }

    #[test]
    fn test_invalid_entry_id() {
        let entry = RedisValue::Array(vec![
            // x insted of -
            RedisValue::BulkString(b"1581855076637x0".to_vec()),
            RedisValue::Array(vec![
                RedisValue::BulkString(b"key".to_vec()),
                RedisValue::Int(2)
            ])
        ]);

        let stream = RedisValue::Array(vec![
            RedisValue::BulkString(b"stream".to_vec()),
            RedisValue::Array(vec![entry])
        ]);

        let value = RedisValue::Array(vec![stream]);

        assert!(parse_stream_entries(value).is_err(), "Expect an parse error");
    }

    #[test]
    fn test_invalid_key_value() {
        let entry = RedisValue::Array(vec![
            RedisValue::BulkString(b"1581855076637-0".to_vec()),
            RedisValue::Array(vec![
                // there is only key without value
                RedisValue::BulkString(b"key".to_vec())
            ])
        ]);

        let stream = RedisValue::Array(vec![
            RedisValue::BulkString(b"stream".to_vec()),
            RedisValue::Array(vec![entry])
        ]);

        let value = RedisValue::Array(vec![stream]);

        assert!(parse_stream_entries(value).is_err(), "Expect an parse error");
    }

    #[test]
    fn test_invalid_entry_structure() {
        let entry = RedisValue::Array(vec![
            // there is no keys and values
            RedisValue::BulkString(b"1581855076637-0".to_vec())
        ]);

        let stream = RedisValue::Array(vec![
            RedisValue::BulkString(b"stream".to_vec()),
            RedisValue::Array(vec![entry])
        ]);

        let value = RedisValue::Array(vec![stream]);

        assert!(parse_stream_entries(value).is_err(), "Expect an parse error");
    }
}
