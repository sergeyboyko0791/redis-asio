use crate::value::{RespValue, FromRespValue};
use crate::error;

use tokio_core::io::{Codec, EasyBuf};
use std::io::{Result as IoResult, Error as IoError, ErrorKind as IoErrorKind};
use std::error::Error;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;


pub struct RedisCodec {}

impl Codec for RedisCodec {
    type In = RespValue;
    type Out = RespValue;

    fn decode(&mut self, buf: &mut EasyBuf) -> IoResult<Option<Self::In>> {
        let data = buf.as_ref();
        Ok(None)
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> IoResult<()> {
        Ok(())
    }
}

mod resp_start_bytes {
    pub const ERROR: u8 = '-' as u8;
    pub const STATUS: u8 = '+' as u8;
    pub const INT: u8 = ':' as u8;
    pub const STRING: u8 = '$' as u8;
    pub const ARRAY: u8 = '*' as u8;
}

//const FIELD_END_BYTES: &str = "\r\n";
const FIELD_END_BYTES: (u8, u8) = ('\r' as u8, '\n' as u8);

struct ParseResult<T> {
    value: T,
    value_src_len: usize,
}

type OptParseResult<T> = Option<ParseResult<T>>;

fn parse_resp_value(data: &[u8]) -> IoResult<OptParseResult<RespValue>> {
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

fn parse_error(data: &[u8]) -> IoResult<OptParseResult<RespValue>> {
    parse_simple_string(data)
        .map(|opt_parse_result|
            opt_parse_result.map(|ParseResult { value, value_src_len }| {
                let value = RespValue::Error(value);
                ParseResult { value, value_src_len }
            }))
}

fn parse_status(data: &[u8]) -> IoResult<OptParseResult<RespValue>> {
    parse_simple_string(data)
        .map(|opt_parse_result|
            opt_parse_result.map(|ParseResult { value, value_src_len }| {
                let value = RespValue::Status(value);
                ParseResult { value, value_src_len }
            }))
}

fn parse_int(data: &[u8]) -> IoResult<OptParseResult<RespValue>> {
    parse_simple_int(data)
        .map(|opt_parse_result|
            opt_parse_result.map(|ParseResult { value, value_src_len }| {
                let value = RespValue::Int(value);
                ParseResult { value, value_src_len }
            }))
}

//fn parse_string(data: &[u8]) -> IoResult<OptParseResult<RespValue>> {
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
    let value_src_len
        = match data.iter().position(|x| *x == FIELD_END_BYTES.0) {
        Some(x) => x,
        _ => return Ok(None),
    };

    if value_src_len >= data.len() - 1 {
        // the value_src_len position points to a last element in the data
        // therefore we could receive incomplete package
        return Ok(None);
    }

    if data[value_src_len + 1] != FIELD_END_BYTES.1 {
        return Err(IoError::new(IoErrorKind::InvalidData,
                                "A status or an Error does not contain the enclosing \"\\r\n\""));
    }

    match String::from_utf8(data[0..value_src_len].to_vec()) {
        Ok(value) => {
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
    let ParseResult { value, value_src_len }
        = parse_status(Vec::from("Ok\r\n").as_mut_slice()).unwrap().unwrap();
    assert_eq!(RespValue::Status(String::from("Ok")), value);

    assert!(parse_status(Vec::from("Ok\r").as_mut_slice()).unwrap().is_none(), "expected Ok(None)");

    assert!(parse_status(Vec::from("Ok\r$").as_mut_slice()).is_err(), "expected Err");
}

#[test]
fn test_parse_error() {
    let ParseResult { value, value_src_len }
        = parse_error(Vec::from("Error\r\n").as_mut_slice()).unwrap().unwrap();
    assert_eq!(RespValue::Error(String::from("Error")), value);

    assert!(parse_error(Vec::from("Error\r").as_mut_slice()).unwrap().is_none(), "expected Ok(None)");

    assert!(parse_error(Vec::from("Error\r$").as_mut_slice()).is_err(), "expected Err");
}

#[test]
fn test_parse_int() {
    let ParseResult { value, value_src_len }
        = parse_int(Vec::from("-12345\r\n").as_mut_slice()).unwrap().unwrap();
    assert_eq!(RespValue::Int(-12345i64), value);

    assert!(parse_int(Vec::from("-12345\r").as_mut_slice()).unwrap().is_none(), "expected Ok(None)");

    assert!(parse_int(Vec::from("-12345\r$").as_mut_slice()).is_err(), "expected Err");

    assert!(parse_int(Vec::from("-12X45\r\n").as_mut_slice()).is_err(), "expected Err");
}
