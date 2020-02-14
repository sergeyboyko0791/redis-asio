use super::RedisValue;

pub struct RedisCommand {
    args: Vec<RedisValue>,
}

impl RedisCommand {
    pub fn new(cmd: &str) -> RedisCommand {
        RedisCommand {
            args: vec![RedisValue::BulkString(cmd.as_bytes().to_vec())]
        }
    }

    pub fn arg(mut self, arg: &str) -> RedisCommand {
        self.args.push(RedisValue::BulkString(arg.as_bytes().to_vec()));
        self
    }

    pub fn into_to_redis_value(self) -> RedisValue {
        RedisValue::Array(self.args)
    }
}
