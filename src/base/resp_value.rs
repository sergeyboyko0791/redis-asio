use super::{RedisValue, RedisError, RedisErrorKind};

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum RespInternalValue {
    Nil,
    Error(String),
    Status(String),
    Int(i64),
    BulkString(Vec<u8>),
    Array(Vec<RespInternalValue>),
}

impl RespInternalValue {
    pub fn from_redis_value(value: RedisValue) -> RespInternalValue {
        match value {
            RedisValue::Nil => RespInternalValue::Nil,
            RedisValue::Ok => RespInternalValue::Status("OK".to_string()),
            RedisValue::Status(x) => RespInternalValue::Status(x),
            RedisValue::Int(x) => RespInternalValue::Int(x),
            RedisValue::BulkString(x) => RespInternalValue::BulkString(x),
            RedisValue::Array(x) =>
                RespInternalValue::Array(
                    x.into_iter()
                        .map(|val| RespInternalValue::from_redis_value(val))
                        .collect())
        }
    }

    pub fn into_redis_value(self) -> Result<RedisValue, RedisError> {
        match self {
            RespInternalValue::Nil => Ok(RedisValue::Nil),
            RespInternalValue::Error(x) => Err(RedisError::from(RedisErrorKind::ReceiveError, x)),
            RespInternalValue::Status(x) => match x.as_str() {
                "OK" => Ok(RedisValue::Ok),
                _ => Ok(RedisValue::Status(x))
            },
            RespInternalValue::Int(x) => Ok(RedisValue::Int(x)),
            RespInternalValue::BulkString(x) => Ok(RedisValue::BulkString(x)),
            RespInternalValue::Array(x) => {
                let mut res: Vec<RedisValue> = Vec::with_capacity(x.len());
                for val in x.into_iter() {
                    res.push(val.into_redis_value()?);
                }
                Ok(RedisValue::Array(res))
            }
        }
    }
}
