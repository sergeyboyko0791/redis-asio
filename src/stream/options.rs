use crate::{RedisResult, RedisError, RedisErrorKind};
use super::EntryId;

#[derive(Clone)]
pub struct SubscribeOptions {
    /// Stream name
    pub(crate) stream: String,
    /// Optional group info
    pub(crate) group: Option<RedisGroup>,
}

pub struct ReadExplicitOptions {
    /// Stream name
    pub(crate) stream: String,
    /// Optional group info
    pub(crate) group: Option<RedisGroup>,
    /// Max count of entries
    pub(crate) count: u16,
    /// Get entries with ID greater than the start_id
    pub(crate) start_id: EntryId,
}

pub struct RangeOptions {
    /// Stream name
    pub(crate) stream: String,
    /// Max count of entries
    pub(crate) count: u16,
    /// Get entries with ID in the range
    pub(crate) range: RangeType,
}

pub enum RangeType {
    GreaterThan(EntryId),
    LessThan(EntryId),
    GreaterLessThan(EntryId, EntryId),
}

#[derive(Clone)]
pub struct RedisGroup {
    /// Group name
    pub(crate) group: String,
    /// Consumer name
    pub(crate) consumer: String,
}

impl SubscribeOptions {
    pub fn new(stream: String) -> SubscribeOptions {
        let group: Option<RedisGroup> = None;
        SubscribeOptions { stream, group }
    }

    pub fn with_group(stream: String, group: RedisGroup) -> SubscribeOptions {
        let group = Some(group);
        SubscribeOptions { stream, group }
    }
}

impl ReadExplicitOptions {
    pub fn new(stream: String, count: u16, start_id: EntryId) -> ReadExplicitOptions {
        let group: Option<RedisGroup> = None;
        ReadExplicitOptions { stream, group, count, start_id }
    }

    pub fn with_group(stream: String, group: RedisGroup, count: u16, start_id: EntryId)
                      -> ReadExplicitOptions {
        let group = Some(group);
        ReadExplicitOptions { stream, group, count, start_id }
    }
}

impl RangeOptions {
    pub fn new(stream: String, count: u16, range: RangeType) -> RedisResult<RangeOptions> {
        let group: Option<RedisGroup> = None;
        if let RangeType::GreaterLessThan(left, right) = &range {
            if left > right {
                return Err(
                    RedisError::new(RedisErrorKind::InvalidOptions,
                                    format!("Left bound {:?} should be less than right bound {:?}", left, right)));
            }
        }

        Ok(RangeOptions { stream, count, range })
    }
}
