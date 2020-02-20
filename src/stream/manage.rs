use super::EntryId;
use crate::{RedisCommand, RedisResult, command};


/// Set of options that are required by `RedisStream::pending_entries()`
#[derive(Clone)]
pub struct PendingOptions {
    /// Get pending entries from the following streams with ID greater than the corresponding entry IDs
    pub(crate) streams: Vec<(String, EntryId)>,
    /// Group name.
    pub(crate) group: String,
    /// Consumer name.
    pub(crate) consumer: String,
    /// Max count of entries. All pending entries will be requested ff the values is None.
    pub(crate) count: Option<u16>,
}

/// Set of options that are required by `RedisStream::touch_group()`
#[derive(Clone)]
pub struct TouchGroupOptions {
    pub(crate) stream: String,
    pub(crate) group: String,
}

/// Set of options that are required by `RedisStream::ack_entry()`
#[derive(Clone)]
pub struct AckOptions {
    pub(crate) stream: String,
    pub(crate) group: String,
    pub(crate) entry_id: EntryId,
}

/// Structure that wraps a response on XACK request.
#[derive(PartialEq, Debug, Clone)]
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
    let mut cmd = command("XREADGROUP")
        .arg("GROUP")
        .arg(options.group)
        .arg(options.consumer);
    if let Some(count) = options.count {
        cmd.arg_mut(count);
    }

    cmd.arg_mut("STREAMS");
    let mut ids_cmd = RedisCommand::new();
    for (stream, start_id) in options.streams {
        cmd.arg_mut(stream);
        ids_cmd.arg_mut(start_id.to_string());
    }

    cmd.append(ids_cmd);
    cmd
}

pub(crate) fn touch_group_command(options: TouchGroupOptions) -> RedisCommand {
    command("XGROUP")
        .arg("CREATE")
        .arg(options.stream)
        .arg(options.group)
        .arg("$")
        .arg("MKSTREAM") // make an empty stream if there is no such one yet
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
    pub fn new(stream: String, group: String, consumer: String, start_id: EntryId)
               -> RedisResult<Self> {
        let streams = vec![(stream, start_id)];
        let count: Option<u16> = None;
        Ok(PendingOptions { streams, group, consumer, count })
    }

    pub fn with_count(stream: String, group: String, consumer: String, start_id: EntryId, count: u16)
                      -> RedisResult<Self> {
        let streams = vec![(stream, start_id)];
        let count = Some(count);
        Ok(PendingOptions { streams, group, consumer, count })
    }

    pub fn add_stream(&mut self, stream: String, start_id: EntryId) {
        self.streams.push((stream, start_id))
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
