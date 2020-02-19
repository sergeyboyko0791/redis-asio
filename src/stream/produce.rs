use super::EntryId;
use crate::{RedisCommand, ToRedisArgument, command};
use std::collections::HashMap;

pub struct AddOptions {
    /// Stream name
    pub(crate) stream: String,
    /// Optional explicit entry id
    pub(crate) entry_id: Option<EntryId>,
}

impl AddOptions {
    pub fn new(stream: String) -> AddOptions {
        let entry_id: Option<EntryId> = None;
        AddOptions { stream, entry_id }
    }

    pub fn with_id(stream: String, entry_id: EntryId) -> AddOptions {
        let entry_id = Some(entry_id);
        AddOptions { stream, entry_id }
    }
}

pub(crate) fn add_command<T>(options: AddOptions, key_values: HashMap<String, T>) -> RedisCommand
    where T: ToRedisArgument {
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
