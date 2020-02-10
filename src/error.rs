use std::fmt;
use std::error::Error;

#[derive(Debug, Clone)]
pub enum RedisErrorKind {
    IncorrectConversion,
    ConnectionError,
    ParseError,
    ReceiveError,
}

#[derive(Debug)]
pub struct RedisCoreError {
    pub error: RedisErrorKind,
    desc: String,
}

pub type RedisCoreResult<T> = Result<T, RedisCoreError>;

impl RedisCoreError {
    pub fn from(error: RedisErrorKind, desc: String) -> RedisCoreError {
        RedisCoreError { error, desc }
    }
}

impl fmt::Display for RedisCoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "error: \"{}\", description: \"{}\"",
               to_string(&self.error),
               &self.desc)
    }
}

impl std::error::Error for RedisCoreError {
    fn description(&self) -> &str {
        &self.desc
    }
}

impl From<std::io::Error> for RedisCoreError {
    fn from(err: std::io::Error) -> Self {
        RedisCoreError { error: RedisErrorKind::ConnectionError, desc: err.description().to_string() }
    }
}

fn to_string(err: &RedisErrorKind) -> &'static str {
    match err {
        RedisErrorKind::IncorrectConversion => "IncorrectConversion",
        RedisErrorKind::ConnectionError => "ConnectionError",
        RedisErrorKind::ParseError => "ParseError",
        RedisErrorKind::ReceiveError => "ReceiveError"
    }
}
