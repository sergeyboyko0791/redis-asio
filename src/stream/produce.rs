use super::EntryId;
use crate::{RedisCommand, IntoRedisArgument, command};
use std::collections::HashMap;


/// Set of options that are required by `RedisStream::send_entry()`
#[derive(Clone)]
pub struct SendEntryOptions {
    /// Stream name
    pub(crate) stream: String,
    /// Optional explicit entry id
    pub(crate) entry_id: Option<EntryId>,
}

impl SendEntryOptions {
    pub fn new(stream: String) -> SendEntryOptions {
        let entry_id: Option<EntryId> = None;
        SendEntryOptions { stream, entry_id }
    }

    pub fn with_id(stream: String, entry_id: EntryId) -> SendEntryOptions {
        let entry_id = Some(entry_id);
        SendEntryOptions { stream, entry_id }
    }
}

pub(crate) fn add_command<T>(options: SendEntryOptions, key_values: HashMap<String, T>) -> RedisCommand
    where T: IntoRedisArgument {
    let mut cmd = command("XADD").arg(options.stream);

    match options.entry_id {
        Some(entry_id) => cmd.arg_mut(entry_id.to_string()),
        _ => cmd.arg_mut("*")
    }

    for (key, value) in key_values.into_iter() {
        cmd.arg_mut(key);
        cmd.arg_mut(value);
    }

    cmd
}
