use crate::value::RedisValue;
use tokio_io::codec::{Encoder, Decoder};
use std::io::{Result as IoResult, Error as IoError, ErrorKind as IoErrorKind};
use std::error::Error;
use std::io::Cursor;
use bytes::BytesMut;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crate::error::RedisCoreError;


#[derive(PartialEq, Eq, Debug)]
pub enum RespInternalValue {
    Error(String),
    Status(String),
    Int(i64),
    BulkString(Vec<u8>),
    Array(Vec<RespInternalValue>),
}

pub struct RedisCodec {}

impl Encoder for RedisCodec {
    type Item = RedisValue;
    type Error = RedisCoreError;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl Decoder for RedisCodec {
    type Item = RedisValue;
    type Error = RedisCoreError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let data = buf.as_ref();
        Ok(None)
    }
}

mod resp_start_bytes {
    pub const ERROR: u8 = '-' as u8;
    pub const STATUS: u8 = '+' as u8;
    pub const INT: u8 = ':' as u8;
    pub const STRING: u8 = '$' as u8;
    pub const ARRAY: u8 = '*' as u8;
}

const FIELD_END_BYTES: (u8, u8) = ('\r' as u8, '\n' as u8);

struct ParseResult<T> {
    value: T,
    value_src_len: usize,
}

type OptParseResult<T> = Option<ParseResult<T>>;

fn parse_resp_value(data: &[u8]) -> IoResult<OptParseResult<RespInternalValue>> {
    let value_id = match Cursor::new(data).read_u8() {
        Ok(x) => x,
        Err(_) => return Ok(None)
    };
    // TODO uncomment that
//    match value_id {
//        resp_start_bytes::STATUS =>
//    }
    Ok(None)
}

fn parse_error(data: &[u8]) -> IoResult<OptParseResult<RespInternalValue>> {
    parse_simple_string(data)
        .map(|opt_parse_result|
            opt_parse_result.map(|ParseResult { value, value_src_len }| {
                let value = RespInternalValue::Error(value);
                ParseResult { value, value_src_len }
            }))
}

fn parse_status(data: &[u8]) -> IoResult<OptParseResult<RespInternalValue>> {
    parse_simple_string(data)
        .map(|opt_parse_result|
            opt_parse_result.map(|ParseResult { value, value_src_len }| {
                let value = RespInternalValue::Status(value);
                ParseResult { value, value_src_len }
            }))
}

fn parse_int(data: &[u8]) -> IoResult<OptParseResult<RespInternalValue>> {
    parse_simple_int(data)
        .map(|opt_parse_result|
            opt_parse_result.map(|ParseResult { value, value_src_len }| {
                let value = RespInternalValue::Int(value);
                ParseResult { value, value_src_len }
            }))
}

//fn parse_string(data: &[u8]) -> IoResult<OptParseResult<RespInternalValue>> {
//    let ParseResult { value, value_src_len: len_len } =
//        match parse_simple_int(data)? {
//            Some(x) => x,
//            _ => return Ok(None),
//        };
//
//    let string_len = value as usize;
//    let value_src_len = string_len as usize + len_len;
//
//    let value = String::from_utf8_lossy(&data[len_len..value_src_len]).to_string();
//}

fn parse_simple_string(data: &[u8]) -> IoResult<OptParseResult<String>> {
    let string_src_len
        = match data.iter().position(|x| *x == FIELD_END_BYTES.0) {
        Some(x) => x,
        _ => return Ok(None),
    };

    if string_src_len >= data.len() - 1 {
        // the value_src_len position points to a last element in the data
        // therefore we could receive incomplete package
        return Ok(None);
    }

    if data[string_src_len + 1] != FIELD_END_BYTES.1 {
        return Err(IoError::new(IoErrorKind::InvalidData,
                                "A status or an Error does not contain the enclosing \"\\r\n\""));
    }

    match String::from_utf8(data[0..string_src_len].to_vec()) {
        Ok(value) => {
            // "Some status\r\n" where:
            // "Some status".len() = string_src_len,
            // "\r\n".len() = 2
            let value_src_len = string_src_len + 2;
            Ok(Some(ParseResult { value, value_src_len }))
        }
        Err(err) => Err(
            IoError::new(IoErrorKind::InvalidData,
                         format!("Couldn't parse a status from bytes: {}", err.description()))
        )
    }
}

fn parse_simple_int(data: &[u8]) -> IoResult<OptParseResult<i64>> {
    let opt_parse_result = parse_simple_string(data)?;
    let ParseResult { value, value_src_len } = match opt_parse_result {
        Some(x) => x,
        _ => return Ok(None),
    };

    let value = match value.parse::<i64>() {
        Ok(x) => x,
        Err(err) => return Err(
            IoError::new(IoErrorKind::InvalidData,
                         format!("Couldn't convert the {:?} to i64, error: {}", value, err.description()),
            )
        ),
    };

    Ok(Some(ParseResult { value, value_src_len }))
}

#[test]
fn test_parse_status() {
    let data = Vec::from("Ok\r\n");
    let ParseResult { value, value_src_len }
        = parse_status(data.as_slice()).unwrap().unwrap();

    assert_eq!(RespInternalValue::Status(String::from("Ok")), value);
    assert_eq!(data.len(), value_src_len);

    assert!(parse_status(Vec::from("Ok\r").as_mut_slice()).unwrap().is_none(), "expected Ok(None)");
    assert!(parse_status(Vec::from("Ok\r$").as_mut_slice()).is_err(), "expected Err");
}

#[test]
fn test_parse_error() {
    let data = Vec::from("Error\r\n");
    let ParseResult { value, value_src_len }
        = parse_error(data.as_slice()).unwrap().unwrap();

    assert_eq!(RespInternalValue::Error(String::from("Error")), value);
    assert_eq!(data.len(), value_src_len);

    assert!(parse_error(Vec::from("Error\r").as_mut_slice()).unwrap().is_none(), "expected Ok(None)");
    assert!(parse_error(Vec::from("Error\r$").as_mut_slice()).is_err(), "expected Err");
}

#[test]
fn test_parse_int() {
    let data = Vec::from("-12345\r\n");
    let ParseResult { value, value_src_len }
        = parse_int(data.as_slice()).unwrap().unwrap();

    assert_eq!(RespInternalValue::Int(-12345i64), value);
    assert_eq!(data.len(), value_src_len);

    assert!(parse_int(Vec::from("-12345\r").as_mut_slice()).unwrap().is_none(), "expected Ok(None)");
    assert!(parse_int(Vec::from("-12345\r$").as_mut_slice()).is_err(), "expected Err");
    assert!(parse_int(Vec::from("-12X45\r\n").as_mut_slice()).is_err(), "expected Err");
}
