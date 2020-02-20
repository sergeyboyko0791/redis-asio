//! Stream module that contains specific interfaces
//! for work with Redis-Stream "https://redis.io/topics/streams-intro".

mod entry;
mod stream;
mod produce;
mod consume;
mod manage;

pub use entry::{StreamEntry, EntryId, RangeEntry, RangeType};
pub use stream::RedisStream;
pub use produce::SendEntryOptions;
pub use consume::{SubscribeOptions, ReadExplicitOptions, RangeOptions, RedisGroup, Subscribe};
pub use manage::{AckOptions, PendingOptions, TouchGroupOptions, AckResponse};

use entry::{parse_stream_entries, parse_range_entries};
use produce::add_command;
use consume::{subscribe, subscribe_cmd, read_explicit_cmd, range_cmd};
use manage::{ack_entry_command, pending_list_command, touch_group_command};
