use super::{RedisResult, RedisError, RedisErrorKind};
use std::error::Error;
use std::fmt;
use std::cmp::PartialEq;
use std::str::FromStr;
use std::collections::HashMap;
use std::hash::Hash;
use core::num::ParseIntError;
use crate::base::RespInternalValue;

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum RedisValue {
    Nil,
    Ok,
    Status(String),
    Int(i64),
    BulkString(Vec<u8>),
    Array(Vec<RedisValue>),
}

impl RedisValue {
    //TODO add maybe to_resp_value and corresponding methods for RespValue to RedisValue

    pub(crate) fn from_resp_value(resp_value: RespInternalValue) -> RedisResult<RedisValue> {
        match resp_value {
            RespInternalValue::Nil => Ok(RedisValue::Nil),
            RespInternalValue::Error(x) => Err(RedisError::new(RedisErrorKind::ReceiveError, x)),
            RespInternalValue::Status(x) => match x.as_str() {
                "OK" => Ok(RedisValue::Ok),
                _ => Ok(RedisValue::Status(x))
            },
            RespInternalValue::Int(x) => Ok(RedisValue::Int(x)),
            RespInternalValue::BulkString(x) => Ok(RedisValue::BulkString(x)),
            RespInternalValue::Array(x) => {
                let mut res: Vec<RedisValue> = Vec::with_capacity(x.len());
                for val in x.into_iter() {
                    res.push(Self::from_resp_value(val)?);
                }
                Ok(RedisValue::Array(res))
            }
        }
    }
}

pub trait FromRedisValue: Sized {
    fn from_redis_value(value: &RedisValue) -> RedisResult<Self>;

    fn from_redis_u8(_: u8) -> Option<Self> {
        None
    }
}

pub fn from_redis_value<T: FromRedisValue>(value: &RedisValue) -> RedisResult<T> {
    T::from_redis_value(value)
        .map_err(|err|
            RedisError::new(
                err.error.clone(),
                format!("Couldn't convert the Redis value: \"{:?}\". Reason: \"{}\"", value, err.description()
                ),
            )
        )
}

impl FromRedisValue for RedisValue {
    fn from_redis_value(value: &RedisValue) -> RedisResult<Self> {
        Ok(value.clone())
    }
}

impl FromRedisValue for u8 {
    fn from_redis_value(value: &RedisValue) -> RedisResult<Self> {
        int_from_redis_value::<u8>(value)
    }

    fn from_redis_u8(num: u8) -> Option<Self> {
        Some(num)
    }
}

impl FromRedisValue for String {
    fn from_redis_value(value: &RedisValue) -> RedisResult<Self> {
        match value {
            RedisValue::Status(x) => Ok(x.clone()),
            RedisValue::BulkString(x) => {
                String::from_utf8(x.clone()).map_err(|err| to_conversion_error(err))
            }
            _ => Err(conversion_error_from_value(value, "String"))
        }
    }
}

impl<T: FromRedisValue> FromRedisValue for Vec<T> {
    fn from_redis_value(value: &RedisValue) -> RedisResult<Self> {
        match value {
            RedisValue::BulkString(bulk_data) => {
                let mut result: Vec<T> = Vec::with_capacity(bulk_data.len());
                for num in bulk_data.iter() {
                    match T::from_redis_u8(*num) {
                        Some(x) => result.push(x),
                        _ => return Err(conversion_error_from_value(bulk_data, "Vec"))
                    }
                }
                Ok(result)
            }
            RedisValue::Array(x) => {
                let mut result: Vec<T> = Vec::with_capacity(x.len());
                for val in x.iter() {
                    match from_redis_value(val) {
                        Ok(x) => result.push(x),
                        Err(err) => return Err(err),
                    }
                }
                Ok(result)
            }
            _ => Err(conversion_error_from_value(value, "Array"))
        }
    }
}

impl<K: FromRedisValue + Eq + Hash, V: FromRedisValue> FromRedisValue for HashMap<K, V> {
    fn from_redis_value(value: &RedisValue) -> RedisResult<Self> {
        match value {
            RedisValue::Array(key_values) => {
                const KEY_VALUE_CHUNK_LEN: usize = 2;
                const KEY_POS: usize = 0;
                const VALUE_POS: usize = 1;

                // count of keys and values should be evenl
                if key_values.len() % KEY_VALUE_CHUNK_LEN != 0 {
                    return Err(conversion_error_from_value(value, "HashMap"));
                }

                let mut result =
                    HashMap::with_capacity(key_values.len() / KEY_VALUE_CHUNK_LEN);

                for chunk in key_values.chunks_exact(KEY_VALUE_CHUNK_LEN) {
                    let key: K = from_redis_value(&chunk[KEY_POS])?;
                    let value: V = from_redis_value(&chunk[VALUE_POS])?;
                    result.insert(key, value);
                }

                Ok(result)
            }
            _ => Err(conversion_error_from_value(value, "HashMap"))
        }
    }
}

// TODO make macro and implement that for (T, ..., T)
impl<T1, T2> FromRedisValue for (T1, T2)
    where T1: FromRedisValue + fmt::Debug,
          T2: FromRedisValue + fmt::Debug {
    fn from_redis_value(value: &RedisValue) -> RedisResult<Self> {
        let values: Vec<RedisValue> = from_redis_value(value)?;
        if values.len() != 2 {
            return Err(
                RedisError::new(
                    RedisErrorKind::ParseError,
                    format!("Couldn't convert the Redis value: \"{:?}\" to tuple of 2 elements",
                            values)));
        }

        let first: T1 = from_redis_value(&values[0])?;
        let second: T2 = from_redis_value(&values[1])?;

        Ok((first, second))
    }
}

fn to_conversion_error<T>(err: T) -> RedisError
    where T: Error {
    RedisError::new(RedisErrorKind::IncorrectConversion, err.description().to_string())
}

fn conversion_error_from_value<T>(src_value: &T, dst_type: &str) -> RedisError
    where T: fmt::Debug {
    RedisError::new(RedisErrorKind::IncorrectConversion,
                    format!("{:?} is not convertible to {}", src_value, dst_type))
}

fn int_from_redis_value<T>(value: &RedisValue) -> RedisResult<T>
    where T: ToIntConvertible {
    match value {
        RedisValue::Int(x) => Ok(T::convert_from_int(*x)),
        RedisValue::BulkString(x) => {
            match String::from_utf8(x.clone()) {
                Ok(xstr) =>
                    T::convert_from_str(xstr)
                        .map_err(|_| conversion_error_from_value(&x, "i64")),
                Err(_) => Err(conversion_error_from_value(x, "i64"))
            }
        }
        _ => Err(conversion_error_from_value(value, "i64"))
    }
}

trait ToIntConvertible: Sized + FromStr {
    fn convert_from_str(val: String) -> Result<Self, ParseIntError>;
    fn convert_from_int(val: i64) -> Self;
}

impl ToIntConvertible for u8 {
    fn convert_from_str(val: String) -> Result<u8, ParseIntError> { val.parse::<u8>() }
    fn convert_from_int(val: i64) -> u8 { val as u8 }
}

macro_rules! declare_to_int_convertible {
    ($itype:ty) => {
        impl ToIntConvertible for $itype {
            fn convert_from_str(val: String) -> Result<$itype, ParseIntError> { val.parse::<$itype>() }
            fn convert_from_int(val: i64) -> $itype { val as $itype }
        }

        impl FromRedisValue for $itype {
            fn from_redis_value(value: &RedisValue) -> RedisResult<Self> {
                int_from_redis_value::<$itype>(value)
            }
        }
    };
}

declare_to_int_convertible!(i8);
declare_to_int_convertible!(i16);
declare_to_int_convertible!(u16);
declare_to_int_convertible!(i32);
declare_to_int_convertible!(u32);
declare_to_int_convertible!(i64);
declare_to_int_convertible!(u64);
declare_to_int_convertible!(isize);
declare_to_int_convertible!(usize);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_test_from_redis_value() {
        #[derive(PartialEq, Debug)]
        struct ArrayNode { data: String }

        impl FromRedisValue for ArrayNode {
            fn from_redis_value(value: &RedisValue) -> RedisResult<Self> {
                Ok(ArrayNode { data: from_redis_value(value)? })
            }
        }

        let value = RedisValue::Array(
            vec![RedisValue::BulkString(String::from("data1").into_bytes()),
                 RedisValue::BulkString(String::from("data2").into_bytes())]);

        let origin = vec![ArrayNode { data: String::from("data1") },
                          ArrayNode { data: String::from("data2") }];
        assert_eq!(origin, from_redis_value::<Vec<ArrayNode>>(&value).unwrap());
    }

    #[test]
    fn test_from_nil_value() {
        let val = RedisValue::Nil;
        assert!(from_redis_value::<i64>(&val).is_err(), "expected Err");
        assert!(from_redis_value::<String>(&val).is_err(), "expected Err");
        assert!(from_redis_value::<Vec<i64>>(&val).is_err(), "expected Err");
    }

    #[test]
    fn test_from_ok_value() {
        let val = RedisValue::Ok;
        assert!(from_redis_value::<i64>(&val).is_err(), "expected Err");
        assert!(from_redis_value::<String>(&val).is_err(), "expected Err");
        assert!(from_redis_value::<Vec<i64>>(&val).is_err(), "expected Err");
    }

    #[test]
    fn test_from_status_value() {
        let val = RedisValue::Status(String::from("Status"));
        assert_eq!(String::from("Status"), from_redis_value::<String>(&val).unwrap());
        assert!(from_redis_value::<i64>(&val).is_err(), "expected Err");
        assert!(from_redis_value::<Vec<i64>>(&val).is_err(), "expected Err");
    }

    #[test]
    fn test_from_int_value() {
        let src: i64 = std::i64::MAX - 5;
        let val = RedisValue::Int(src);
        assert_eq!(src as i8, from_redis_value::<i8>(&val).unwrap());
        assert_eq!(src as u8, from_redis_value::<u8>(&val).unwrap());
        assert_eq!(src as i16, from_redis_value::<i16>(&val).unwrap());
        assert_eq!(src as u16, from_redis_value::<u16>(&val).unwrap());
        assert_eq!(src as i32, from_redis_value::<i32>(&val).unwrap());
        assert_eq!(src as u32, from_redis_value::<u32>(&val).unwrap());
        assert_eq!(src as i64, from_redis_value::<i64>(&val).unwrap());
        assert_eq!(src as u64, from_redis_value::<u64>(&val).unwrap());
        assert!(from_redis_value::<String>(&val).is_err(), "expected Err");
        assert!(from_redis_value::<Vec<i64>>(&val).is_err(), "expected Err");
    }

    #[test]
    fn test_from_bulkstring_value() {
        let raw_data = vec![1, 2, 250, 251, 255];
        let string_data = String::from("BulkString");
        let val1 = RedisValue::BulkString(raw_data.clone());
        let val2 = RedisValue::BulkString(string_data.clone().into_bytes());

        assert!(from_redis_value::<String>(&val1).is_err(),
                "expected Err on cannot convert raw data to String");
        assert_eq!(raw_data, from_redis_value::<Vec<u8>>(&val1).unwrap());
        assert!(from_redis_value::<Vec<i8>>(&val1).is_err(), "expected Err");
        assert!(from_redis_value::<Vec<i64>>(&val1).is_err(), "expected Err");
        assert!(from_redis_value::<i64>(&val1).is_err(), "expected Err");

        assert_eq!(string_data, from_redis_value::<String>(&val2).unwrap());
        assert_eq!(string_data.into_bytes(), from_redis_value::<Vec<u8>>(&val2).unwrap());
    }

    #[test]
    fn test_from_array_value() {
        let data
            = vec![RedisValue::Nil,
                   RedisValue::Ok,
                   RedisValue::Status(String::from("Status")),
                   RedisValue::Int(12345),
                   RedisValue::BulkString(vec![1, 2, 3, 4, 5]),
                   RedisValue::Array(
                       vec![RedisValue::Int(9876),
                            RedisValue::BulkString(String::from("BulkString").into_bytes())])];

        let val1 = RedisValue::Array(data.clone());
        assert_eq!(data, from_redis_value::<Vec<RedisValue>>(&val1).unwrap());
    }
}
