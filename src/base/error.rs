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
pub struct RedisError {
    pub error: RedisErrorKind,
    desc: String,
}

// TODO rename all Result<T, RedisCoreError> uses to the type
pub type RedisResult<T> = Result<T, RedisError>;

impl RedisError {
    // TODO delete it
    pub fn new_some() -> RedisError {
        RedisError { error: RedisErrorKind::ReceiveError, desc: "some error".to_string() }
    }

    // TODO rename to new()
    pub fn from(error: RedisErrorKind, desc: String) -> RedisError {
        RedisError { error, desc }
    }

    // TODO add from()
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
        RedisErrorKind::IncorrectConversion => "IncorrectConversion",
        RedisErrorKind::ConnectionError => "ConnectionError",
        RedisErrorKind::ParseError => "ParseError",
        RedisErrorKind::ReceiveError => "ReceiveError"
    }
}
