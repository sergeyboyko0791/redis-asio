use crate::RespInternalValue;


/// Make a Redis command represents array of `BulkString`s
/// wrapped into `RedisCommand`.
///
/// # Example
///
/// ```
/// use redis_asio::command;
/// let cmd = redis_asio::command("SET").arg("foo").arg("bar");
/// ```
pub fn command(cmd: &str) -> RedisCommand {
    RedisCommand::cmd(cmd)
}

/// Enumeration of base types that can be put into
/// `RedisCommand` chain as argument.
pub enum RedisArgument {
    Int(i64),
    String(String),
    Bytes(Vec<u8>),
}

/// Redis command wrapper represents array of `BulkString`s
#[derive(Clone)]
pub struct RedisCommand {
    args: Vec<RespInternalValue>,
}

/// Trait interface requires to implement method to convert base type
/// into `RedisArgument`.
///
/// # Example
/// ```
/// use redis_asio::{RedisArgument, IntoRedisArgument, command};
///
/// struct ClientStruct { pub data: String }
///
/// impl IntoRedisArgument for ClientStruct {
///     fn into_redis_argument(self) -> RedisArgument {
///         RedisArgument::String(self.data)
///     }
/// }
///
/// let value = ClientStruct { data: "Hello, world".to_string() };
/// let cmd = command("SET").arg("foo").arg(value);
/// ```
pub trait IntoRedisArgument {
    fn into_redis_argument(self) -> RedisArgument;
}

impl RedisCommand {
    pub(crate) fn new() -> RedisCommand {
        RedisCommand { args: Vec::new() }
    }

    /// Method is used by `redis::command()`.
    pub(crate) fn cmd(cmd: &str) -> RedisCommand {
        RedisCommand {
            args: vec![RespInternalValue::BulkString(cmd.as_bytes().to_vec())]
        }
    }

    /// Add new argument into `RedisCommand` and move the one back.
    /// The argument should implement the `IntoRedisArgument` trait.
    pub fn arg<T: IntoRedisArgument>(mut self, arg: T) -> RedisCommand {
        self.args.push(arg.into_redis_argument().into_resp_value());
        self
    }

    /// Add new argument into `RedisCommand` through object changing.
    /// The argument should implement the `IntoRedisArgument` trait.
    pub fn arg_mut<T: IntoRedisArgument>(&mut self, arg: T) {
        self.args.push(arg.into_redis_argument().into_resp_value());
    }

    /// Append the other `RedisCommand`'s arguments into self arguments.
    pub fn append(&mut self, mut other: RedisCommand) {
        self.args.append(&mut other.args);
    }

    // TODO make it pub(crate) maybe.
    /// Convert the self into `RespInternalValue`.
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

impl IntoRedisArgument for RedisArgument {
    fn into_redis_argument(self) -> RedisArgument {
        self
    }
}

impl IntoRedisArgument for &str {
    fn into_redis_argument(self) -> RedisArgument {
        RedisArgument::String(self.to_string())
    }
}

impl IntoRedisArgument for String {
    fn into_redis_argument(self) -> RedisArgument {
        RedisArgument::String(self)
    }
}

impl IntoRedisArgument for Vec<u8> {
    fn into_redis_argument(self) -> RedisArgument {
        RedisArgument::Bytes(self)
    }
}

macro_rules! declare_to_int_argument {
    ($itype:ty) => {
        impl IntoRedisArgument for $itype {
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
