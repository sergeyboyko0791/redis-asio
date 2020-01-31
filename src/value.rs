use crate::error::{RedisCoreResult, RedisCoreError, ErrorKind};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::cmp::PartialEq;

#[derive(PartialEq, Eq)]
pub enum RespValue {
    Nil,
    Error(String),
    Status(String),
    Int(i64),
    BulkString(String),
    Array(Vec<RespValue>),
}

pub trait FromRespValue: Sized {
    fn from_resp_value(value: &RespValue) -> RedisCoreResult<Self>;
}

pub fn from_resp_value<T: FromRespValue>(value: &RespValue) -> RedisCoreResult<T> {
    T::from_resp_value(value)
}

impl FromRespValue for i64 {
    fn from_resp_value(value: &RespValue) -> RedisCoreResult<Self> {
        match value {
            RespValue::Int(x) => Ok(*x),
            RespValue::BulkString(x) => x.parse::<i64>()
                .map_err(|err|
                    RedisCoreError::from(ErrorKind::IncorrectConversion,
                                         format!("Couldn't convert the {:?} to i64, error: {}", value, err.description()),
                    )
                ),
            _ => Err(make_conversion_error(value, "i64")),
        }
    }
}

impl FromRespValue for String {
    fn from_resp_value(value: &RespValue) -> RedisCoreResult<Self> {
        match value {
            RespValue::Status(x) => Ok(x.clone()),
            RespValue::BulkString(x) => Ok(x.clone()),
            _ => Err(make_conversion_error(value, "String"))
        }
    }
}

impl<T: FromRespValue> FromRespValue for Vec<T> {
    fn from_resp_value(value: &RespValue) -> RedisCoreResult<Self> {
        match value {
            RespValue::Array(x) => x.iter()
                .map(|val| from_resp_value(val))
                .collect(),
            _ => Err(make_conversion_error(value, "Array"))
        }
    }
}

fn make_conversion_error(value: &RespValue, dest_conversion: &str) -> RedisCoreError {
    RedisCoreError::from(ErrorKind::IncorrectConversion,
                         String::from(format!("Couldn't convert the {:?} to {}", value, dest_conversion)))
}

impl fmt::Debug for RespValue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", to_string(&self))
    }
}

fn to_string(val: &RespValue) -> String {
    match val {
        RespValue::Nil => return String::from("Nil"),
        RespValue::Error(x) => format!("Error({:?})", x),
        RespValue::Status(x) => format!("Status({:?})", x),
        RespValue::Int(x) => format!("Int({:?})", x.to_string()),
        RespValue::BulkString(x) => format!("BulkString({:?})", x),
        RespValue::Array(values) =>
            format!("{:?}",
                    values.iter()
                        .map(|v| to_string(v))
                        .collect::<Vec<String>>())
    }
}
