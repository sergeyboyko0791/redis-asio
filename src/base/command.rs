use crate::base::RespInternalValue;

pub fn command(cmd: &str) -> RedisCommand {
    RedisCommand::new(cmd)
}

// TODO delete clone trait
#[derive(Clone)]
pub enum RedisArgument {
    Int(i64),
    String(String),
    Bytes(Vec<u8>),
}

pub struct RedisCommand {
    args: Vec<RespInternalValue>,
}

pub trait ToRedisArgument {
    // TODO delete it
    fn to_redis_argument(&self) -> RedisArgument;

    fn into_redis_argument(self) -> RedisArgument;
}

impl RedisCommand {
    pub(crate) fn new(cmd: &str) -> RedisCommand {
        RedisCommand {
            args: vec![RespInternalValue::BulkString(cmd.as_bytes().to_vec())]
        }
    }

    pub fn arg<T: ToRedisArgument>(mut self, arg: T) -> RedisCommand {
        self.args.push(arg.into_redis_argument().into_resp_value());
        self
    }

    pub fn arg_mut<T: ToRedisArgument>(&mut self, arg: T) {
        self.args.push(arg.into_redis_argument().into_resp_value());
    }

    pub fn into_resp_value(self) -> RespInternalValue {
        RespInternalValue::Array(self.args)
    }
}

impl RedisArgument {
    pub(crate) fn into_resp_value(self) -> RespInternalValue {
        match self {
            RedisArgument::Int(x) => RespInternalValue::BulkString(x.to_string().into()),
            RedisArgument::String(x) => RespInternalValue::BulkString(x.into()),
            RedisArgument::Bytes(x) => RespInternalValue::BulkString(x),
        }
    }
}

impl ToRedisArgument for RedisArgument {
    fn to_redis_argument(&self) -> RedisArgument {
        self.clone()
    }

    fn into_redis_argument(self) -> RedisArgument {
        self
    }
}

impl ToRedisArgument for &str {
    fn to_redis_argument(&self) -> RedisArgument {
        RedisArgument::String(self.to_string())
    }

    fn into_redis_argument(self) -> RedisArgument {
        RedisArgument::String(self.to_string())
    }
}

impl ToRedisArgument for String {
    fn to_redis_argument(&self) -> RedisArgument {
        RedisArgument::String(self.clone())
    }

    fn into_redis_argument(self) -> RedisArgument {
        RedisArgument::String(self)
    }
}

impl ToRedisArgument for Vec<u8> {
    fn to_redis_argument(&self) -> RedisArgument {
        RedisArgument::Bytes(self.clone())
    }

    fn into_redis_argument(self) -> RedisArgument {
        RedisArgument::Bytes(self)
    }
}

macro_rules! declare_to_int_argument {
    ($itype:ty) => {
        impl ToRedisArgument for $itype {
            fn to_redis_argument(&self) -> RedisArgument {
                RedisArgument::Int(*self as i64)
            }

            fn into_redis_argument(self) -> RedisArgument {
                RedisArgument::Int(self as i64)
            }
        }
    };
}

declare_to_int_argument!(u8);
declare_to_int_argument!(i8);
declare_to_int_argument!(u16);
declare_to_int_argument!(i16);
declare_to_int_argument!(u32);
declare_to_int_argument!(i32);
declare_to_int_argument!(i64);
declare_to_int_argument!(u64);
declare_to_int_argument!(usize);
