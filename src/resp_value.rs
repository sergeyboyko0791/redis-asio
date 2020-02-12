use crate::{RedisValue, RedisCoreError, RedisErrorKind};

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
    pub fn into_redis_value(self) -> Result<RedisValue, RedisCoreError> {
        match self {
            RespInternalValue::Nil => Ok(RedisValue::Nil),
            RespInternalValue::Error(x) => Err(RedisCoreError::from(RedisErrorKind::ReceiveError, x)),
            RespInternalValue::Status(x) => match x.as_str() {
                "Ok" => Ok(RedisValue::Ok),
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
