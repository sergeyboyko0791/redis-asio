#[derive(Clone)]
pub struct RedisStreamOptions {
    stream: String,
    group: Option<String>,
}

impl RedisStreamOptions {
    pub fn new(stream: String) -> RedisStreamOptions {
        RedisStreamOptions { stream, group: None }
    }

    pub fn with_group(stream: String, group: String) -> RedisStreamOptions {
        let group = Some(group);
        RedisStreamOptions { stream, group }
    }
}
