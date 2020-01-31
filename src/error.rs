use std::fmt;

#[derive(Debug)]
pub enum ErrorKind {
    IncorrectConversion,
}

#[derive(Debug)]
pub struct RedisCoreError {
    error: ErrorKind,
    desc: String,
}

pub type RedisCoreResult<T> = Result<T, RedisCoreError>;

impl RedisCoreError {
    pub fn from(error: ErrorKind, desc: String) -> RedisCoreError {
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

fn to_string(err: &ErrorKind) -> &'static str {
    match err {
        ErrorKind::IncorrectConversion => "IncorrectConversion",
    }
}
