use std::fmt;
use std::error::Error;

#[derive(Debug, Clone)]
pub enum RedisErrorKind {
    InternalError,
    IncorrectConversion,
    ConnectionError,
    ParseError,
    ReceiveError,
    InvalidOptions,
}

#[derive(Debug)]
pub struct RedisError {
    pub error: RedisErrorKind,
    desc: String,
}

// TODO rename all Result<T, RedisCoreError> uses to the type
pub type RedisResult<T> = Result<T, RedisError>;

impl RedisError {
    pub fn new(error: RedisErrorKind, desc: String) -> RedisError {
        RedisError { error, desc }
    }
}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "error: \"{}\", description: \"{}\"",
               to_string(&self.error),
               &self.desc)
    }
}

impl std::error::Error for RedisError {
    fn description(&self) -> &str {
        &self.desc
    }
}

impl From<std::io::Error> for RedisError {
    fn from(err: std::io::Error) -> Self {
        RedisError { error: RedisErrorKind::ConnectionError, desc: err.description().to_string() }
    }
}

fn to_string(err: &RedisErrorKind) -> &'static str {
    match err {
        RedisErrorKind::InternalError => "InternalError",
        RedisErrorKind::IncorrectConversion => "IncorrectConversion",
        RedisErrorKind::ConnectionError => "ConnectionError",
        RedisErrorKind::ParseError => "ParseError",
        RedisErrorKind::ReceiveError => "ReceiveError",
        RedisErrorKind::InvalidOptions => "InvalidOptions",
    }
}
