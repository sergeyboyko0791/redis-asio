use super::{AddOptions, EntryId, RangeType};
use std::net::SocketAddr;
use futures::Future;
use crate::{RedisCoreConnection, RedisError, RedisErrorKind, RedisValue, RedisCommand, ToRedisArgument, FromRedisValue, command, from_redis_value, RedisResult};

pub struct PendingOptions {
    /// Stream name.
    pub(crate) stream: String,
    /// Group name.
    pub(crate) group: String,
    /// Consumer name.
    pub(crate) consumer: String,
    /// Get entries with ID in the range.
    pub(crate) range: RangeType,
    /// Max count of entries. All pending entries will be requested ff the values is None.
    pub(crate) count: Option<u16>,
}

pub struct PendingMessage {
    /// The ID of the message.
    pub id: EntryId,
    /// The name of the consumer that fetched the message and has still to acknowledge it.
    pub consumer: String,
    /// The number of milliseconds that elapsed since the last time
    /// this message was delivered to this consumer.
    pub last_delivered_time_ms: i64,
    /// The number of times this message was delivered.
    pub delivered_times: i64,
}

pub struct TouchGroupOptions {
    pub(crate) stream: String,
    pub(crate) group: String,
}

pub struct AckOptions {
    pub(crate) stream: String,
    pub(crate) group: String,
    pub(crate) entry_id: EntryId,
}

pub enum AckResponse {
    Ok,
    NotExists,
}

pub(crate) fn ack_entry_command(options: AckOptions) -> RedisCommand {
    command("XACK")
        .arg(options.stream)
        .arg(options.group)
        .arg(options.entry_id.to_string())
}

pub(crate) fn pending_list_command(options: PendingOptions) -> RedisCommand {
    let (left, right) = options.range.to_left_right();

    let mut cmd = command("XPENDING")
        .arg(options.stream)
        .arg(options.group)
        .arg(left)
        .arg(right);
    if let Some(count) = options.count {
        cmd.arg_mut(count);
    }

    cmd.arg(options.consumer)
}

pub(crate) fn touch_group_command(options: TouchGroupOptions) -> RedisCommand {
    command("XGROUP")
        .arg("CREATE")
        .arg(options.stream)
        .arg(options.group)
        .arg("$")
}

impl AckResponse {
    pub(crate) fn new(count_acknowledged: i64) -> Self {
        match count_acknowledged {
            0 => AckResponse::NotExists,
            _ => AckResponse::Ok,
        }
    }
}

impl PendingOptions {
    pub fn new(stream: String, group: String, consumer: String, range: RangeType)
               -> RedisResult<Self> {
        if !range.is_valid() {
            return Err(
                RedisError::new(RedisErrorKind::InvalidOptions,
                                format!("Left bound should be less than right bound")));
        }

        let count: Option<u16> = None;
        Ok(PendingOptions { stream, group, consumer, range, count })
    }

    pub fn with_count(stream: String, group: String, consumer: String, range: RangeType, count: u16)
                      -> RedisResult<Self> {
        if !range.is_valid() {
            return Err(
                RedisError::new(RedisErrorKind::InvalidOptions,
                                format!("Left bound should be less than right bound")));
        }

        let count = Some(count);
        Ok(PendingOptions { stream, group, consumer, range, count })
    }
}

impl TouchGroupOptions {
    pub fn new(stream: String, group: String) -> Self {
        TouchGroupOptions { stream, group }
    }
}

impl AckOptions {
    pub fn new(stream: String, group: String, entry_id: EntryId) -> Self {
        AckOptions { stream, group, entry_id }
    }
}

impl FromRedisValue for PendingMessage {
    fn from_redis_value(value: &RedisValue) -> RedisResult<Self> {
        let mut values: Vec<RedisValue> = from_redis_value(value)?;
        if values.len() != 5 {
            return Err(
                RedisError::new(
                    RedisErrorKind::ParseError,
                    format!("Couldn't convert the Redis value: \"{:?}\" to PendingMessage", values)));
        }

        let id_str: String = from_redis_value(&values[0])?;
        let id = EntryId::from_string(id_str)?;
        let consumer: String = from_redis_value(&values[1])?;
        let last_delivered_time_ms: i64 = from_redis_value(&values[2])?;
        let delivered_times: i64 = from_redis_value(&values[3])?;

        Ok(PendingMessage { id, consumer, last_delivered_time_ms, delivered_times })
    }
}
