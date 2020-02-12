#[derive(Clone)]
pub struct RedisStreamOptions {
    /// Stream name
    pub stream: String,
    /// Optional group info
    pub group: Option<RedisGroup>,
}

#[derive(Clone)]
pub struct RedisGroup {
    /// Group name
    pub group: String,
    /// Consumer name
    pub consumer: String,
}

impl RedisStreamOptions {
    pub fn new(stream: String) -> RedisStreamOptions {
        RedisStreamOptions { stream, group: None }
    }

    pub fn with_group(stream: String, group: RedisGroup) -> RedisStreamOptions {
        let group = Some(group);
        RedisStreamOptions { stream, group }
    }
}
