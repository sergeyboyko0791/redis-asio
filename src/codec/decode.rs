use crate::{RedisCoreError, RedisErrorKind};
use crate::codec::RespInternalValue;
use std::io::Cursor;
use std::error::Error;
use byteorder::ReadBytesExt;

pub struct ParseResult<T> {
    pub value: T,
    pub value_src_len: usize,
}

pub type OptParseResult<T> = Option<ParseResult<T>>;

pub fn parse_resp_value(data: &[u8]) -> Result<OptParseResult<RespInternalValue>, RedisCoreError> {
    let value_id = match Cursor::new(data).read_u8() {
        Ok(x) => x,
        Err(_) => return Ok(None)
    };

    let data = &data[1..];

    let opt_parse_result = match value_id {
        resp_start_bytes::ERROR => parse_error(data),
        resp_start_bytes::STATUS => parse_status(data),
        resp_start_bytes::INT => parse_int(data),
        resp_start_bytes::BULK_STRING => parse_bulkstring(data),
        resp_start_bytes::ARRAY => parse_array(data),
        _ => Err(RedisCoreError::from(
            RedisErrorKind::ParseError,
            format!("Unknown RESP start byte {}", value_id)))
    }?;

    Ok(opt_parse_result
        .map(
            |ParseResult { value, value_src_len }| {
                let value_src_len = value_src_len + 1;
                ParseResult { value, value_src_len }
            }
        )
    )
}


mod resp_start_bytes {
    pub const ERROR: u8 = b'-';
    pub const STATUS: u8 = b'+';
    pub const INT: u8 = b':';
    pub const BULK_STRING: u8 = b'$';
    pub const ARRAY: u8 = b'*';
}

const CRLF: (u8, u8) = (b'\r', b'\n');
// "\r\n".len() == 2
const CRLF_LEN: usize = 2;

fn parse_error(data: &[u8]) -> Result<OptParseResult<RespInternalValue>, RedisCoreError> {
    parse_simple_string(data)
        .map(|opt_parse_result|
            opt_parse_result.map(|ParseResult { value, value_src_len }| {
                let value = RespInternalValue::Error(value);
                ParseResult { value, value_src_len }
            }))
}

fn parse_status(data: &[u8]) -> Result<OptParseResult<RespInternalValue>, RedisCoreError> {
    parse_simple_string(data)
        .map(|opt_parse_result|
            opt_parse_result.map(|ParseResult { value, value_src_len }| {
                let value = RespInternalValue::Status(value);
                ParseResult { value, value_src_len }
            }))
}

fn parse_int(data: &[u8]) -> Result<OptParseResult<RespInternalValue>, RedisCoreError> {
    parse_simple_int(data)
        .map(|opt_parse_result|
            opt_parse_result.map(|ParseResult { value, value_src_len }| {
                let value = RespInternalValue::Int(value);
                ParseResult { value, value_src_len }
            }))
}

fn parse_bulkstring(data: &[u8]) -> Result<OptParseResult<RespInternalValue>, RedisCoreError> {
    let does_end_with_crlf = |data: &[u8]| data.ends_with(&[CRLF.0, CRLF.1]);
    let make_parse_error =
        || RedisCoreError::from(
            RedisErrorKind::ParseError,
            "An actual data within a bulk string does not end with the CRLF".to_string());

    let ParseResult { value, value_src_len: len_len } =
        match parse_simple_int(data)? {
            Some(x) => x,
            _ => return Ok(None),
        };

    if value < 0 {
        if data.len() < len_len + CRLF_LEN {
            // message is not complete
            return Ok(None);
        }

        match does_end_with_crlf(&data[..len_len + CRLF_LEN]) {
            true => return Ok(Some(
                ParseResult { value: RespInternalValue::Nil, value_src_len: len_len + CRLF_LEN })),
            false => return Err(make_parse_error())
        };
    }

    let string_len = value as usize;
    // eg "6\r\nfoobar\r\n" consists:
    // "6\r\n".len() = len_len - len of message within the [\r\n ... \r\n] region
    // "foobar".len() = string_len
    // "\r\n".len() = FIELD_END_BYTES_LEN
    let value_src_len = len_len + string_len + CRLF_LEN;

    if data.len() < value_src_len {
        return Ok(None);
    }

    if !data[..value_src_len].ends_with(&[CRLF.0, CRLF.1]) {
        return Err(make_parse_error());
    }

    // eg "6\r\nfoobar\r\n" consists:
    // "6\r\n" = [..len_len]
    // "foobar" = [len_len..len_len + string_len]
    // "\r\n" = [len_len + string_len..value_src_len]
    let value_data = data[len_len..len_len + string_len].to_vec();
    let value = RespInternalValue::BulkString(value_data);
    Ok(Some(ParseResult { value, value_src_len }))
}

fn parse_array(data: &[u8]) -> Result<OptParseResult<RespInternalValue>, RedisCoreError> {
    let ParseResult { value: array_len, value_src_len: len_len } =
        match parse_simple_int(data)? {
            Some(x) => x,
            _ => return Ok(None),
        };

    if array_len < 0 {
        return Err(RedisCoreError::from(
            RedisErrorKind::ParseError,
            "Array length cannot be negative".to_string()));
    }

    let array_len = array_len as usize;

    let mut pos = len_len;
    let mut result: Vec<RespInternalValue> = Vec::with_capacity(array_len);

    for _ in 0..array_len {
        let ParseResult { value, value_src_len } =
            match parse_resp_value(&data[pos..])? {
                Some(x) => x,
                _ => return Ok(None),
            };

        result.push(value);
        pos = pos + value_src_len;
    };

    Ok(Some(ParseResult { value: RespInternalValue::Array(result), value_src_len: pos }))
}

fn parse_simple_string(data: &[u8]) -> Result<OptParseResult<String>, RedisCoreError> {
    let string_src_len
        = match data.iter().position(|x| *x == CRLF.0) {
        Some(x) => x,
        _ => return Ok(None),
    };

    if string_src_len >= data.len() - 1 {
        // the value_src_len position points to a last element in the data
        // therefore we could receive incomplete package
        return Ok(None);
    }

    if data[string_src_len + 1] != CRLF.1 {
        return Err(RedisCoreError::from(
            RedisErrorKind::ParseError,
            "A status or an Error does not contain the CRLF".to_string()));
    }

    match String::from_utf8(data[0..string_src_len].to_vec()) {
        Ok(value) => {
            // "Some status\r\n" where:
            // "Some status".len() = string_src_len,
            // "\r\n".len() = 2
            let value_src_len = string_src_len + CRLF_LEN;
            Ok(Some(ParseResult { value, value_src_len }))
        }
        Err(err) => Err(
            RedisCoreError::from(
                RedisErrorKind::ParseError,
                format!("Could not parse a status from bytes: {}", err.description()))
        )
    }
}

fn parse_simple_int(data: &[u8]) -> Result<OptParseResult<i64>, RedisCoreError> {
    let opt_parse_result = parse_simple_string(data)?;
    let ParseResult { value, value_src_len } = match opt_parse_result {
        Some(x) => x,
        _ => return Ok(None),
    };

    let value = match value.parse::<i64>() {
        Ok(x) => x,
        Err(err) => return Err(
            RedisCoreError::from(
                RedisErrorKind::ParseError,
                format!("Could not parse an i64 from the {:?}, error: {}", value, err.description()),
            )
        ),
    };

    Ok(Some(ParseResult { value, value_src_len }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_status() {
        let data = Vec::from("+Ok\r\n");
        let ParseResult { value, value_src_len }
            = parse_resp_value(data.as_slice()).unwrap().unwrap();

        assert_eq!(RespInternalValue::Status("Ok".to_string()), value);
        assert_eq!(data.len(), value_src_len);

        assert!(parse_resp_value(Vec::from("+Ok\r").as_mut_slice()).unwrap().is_none(), "expected Ok(None)");
        assert!(parse_resp_value(Vec::from("+Ok\r$").as_mut_slice()).is_err(), "expected Err");
    }

    #[test]
    fn test_parse_error() {
        let data = Vec::from("-Error\r\n");
        let ParseResult { value, value_src_len }
            = parse_resp_value(data.as_slice()).unwrap().unwrap();

        assert_eq!(RespInternalValue::Error("Error".to_string()), value);
        assert_eq!(data.len(), value_src_len);

        assert!(parse_resp_value(Vec::from("-Error\r").as_mut_slice()).unwrap().is_none(), "expected Ok(None)");
        assert!(parse_resp_value(Vec::from("-Error\r$").as_mut_slice()).is_err(), "expected Err");
    }

    #[test]
    fn test_parse_int() {
        let data = Vec::from(":-12345\r\n");
        let ParseResult { value, value_src_len }
            = parse_resp_value(data.as_slice()).unwrap().unwrap();

        assert_eq!(RespInternalValue::Int(-12345i64), value);
        assert_eq!(data.len(), value_src_len);

        assert!(parse_resp_value(Vec::from(":-12345\r").as_mut_slice()).unwrap().is_none(), "expected Ok(None)");
        assert!(parse_resp_value(Vec::from(":-12345\r$").as_mut_slice()).is_err(), "expected Err");
        assert!(parse_resp_value(Vec::from(":-12X45\r\n").as_mut_slice()).is_err(), "expected Err");
    }

    #[test]
    fn test_parse_bulkstring() {
        // $ - message type identifier
        // 8\r\n - the number of bytes composing the string (a prefixed length), terminated by CRLF.
        // foo\r\nbar - the actual string data.
        // \r\n - a final CRLF.
        let origin_msg = "foo\r\nbar".to_string();

        let mut raw_data = format!("${}\r\n{}\r\n", origin_msg.len(), origin_msg).into_bytes();
        let expected_value_len = raw_data.len();
        raw_data.append(&mut "trash".as_bytes().to_vec());

        let ParseResult { value, value_src_len }
            = parse_resp_value(raw_data.as_slice()).unwrap().unwrap();

        assert_eq!(RespInternalValue::BulkString(origin_msg.into_bytes()), value);
        assert_eq!(expected_value_len, value_src_len);

        // receive an incomplete message
        assert!(parse_resp_value(Vec::from("$7\r").as_mut_slice()).unwrap().is_none(), "expected Ok(None)");
        // receive an incomplete message
        assert!(parse_resp_value(Vec::from("$7\r\n$").as_mut_slice()).unwrap().is_none(), "expected Ok(None)");
        // receive incorrect message without CRLF:
        // 7\r\n - the number of bytes composing the string (a prefixed length), terminated by CRLF.
        // 1234567 - the actual string data, the len = 7.
        // %\n - incorrect CRLF characters (expected \r\n).
        assert!(parse_resp_value(Vec::from("$7\r\n1234567\r$").as_mut_slice()).is_err(), "expected Err");
    }

    #[test]
    fn test_parse_bulkstring_nil() {
        // $ - message type identifier
        // -10\r\n - the number of bytes composing the string (a prefixed length), terminated by CRLF.
        // actual there is no string data.
        // \r\n - a final CRLF.
        let mut raw_data = Vec::from("$-10\r\n\r\n");
        let expected_value_len = raw_data.len();
        raw_data.append(&mut "trash".as_bytes().to_vec());

        let ParseResult { value, value_src_len }
            = parse_resp_value(raw_data.as_slice()).unwrap().unwrap();

        assert_eq!(RespInternalValue::Nil, value);
        assert_eq!(expected_value_len, value_src_len);

        // receive an incomplete message
        assert!(parse_resp_value(Vec::from("$-10\r").as_mut_slice()).unwrap().is_none(), "expected Ok(None)");
        // receive an incomplete message
        assert!(parse_resp_value(Vec::from("$-10\r\n$").as_mut_slice()).unwrap().is_none(), "expected Ok(None)");
        // receive incorrect message without CRLF
        assert!(parse_resp_value(Vec::from("$-10\r\n%$").as_mut_slice()).is_err(), "expected Err");
//    assert!(parse_bulkstring(Vec::from("-12X45\r\n").as_mut_slice()).is_err(), "expected Err");
    }

    #[test]
    fn test_parse_array() {
        let mut nil_value_data = Vec::from("$-1\r\n\r\n");
        // "Error message"
        let mut error_value_data = Vec::from("-Error message\r\n");
        // "Status message"
        let mut status_value_data = Vec::from("+Status message\r\n");
        // -1423
        let mut int_value_data = Vec::from(":-1423\r\n");
        // "Bulk\r\nstring\tmessage"
        let mut bulkstring_value_data = Vec::from("$20\r\nBulk\r\nstring\tmessage\r\n");
        // [1, 2, 3]
        let mut array_value_data = Vec::from("*3\r\n:1\r\n:2\r\n:3\r\n");

        let mut array_data: Vec<u8> = Vec::from("*6\r\n");
        array_data.append(&mut nil_value_data);
        array_data.append(&mut error_value_data);
        array_data.append(&mut status_value_data);
        array_data.append(&mut int_value_data);
        array_data.append(&mut bulkstring_value_data);
        array_data.append(&mut array_value_data);

        let expected_value_len = array_data.len();

        array_data.append(&mut "trash".as_bytes().to_vec());

        let origin = RespInternalValue::Array(
            vec![RespInternalValue::Nil,
                 RespInternalValue::Error("Error message".to_string()),
                 RespInternalValue::Status("Status message".to_string()),
                 RespInternalValue::Int(-1423),
                 RespInternalValue::BulkString("Bulk\r\nstring\tmessage".as_bytes().to_vec()),
                 RespInternalValue::Array(vec![RespInternalValue::Int(1),
                                               RespInternalValue::Int(2),
                                               RespInternalValue::Int(3)])
            ]);
        let ParseResult { value, value_src_len }
            = parse_resp_value(&array_data).unwrap().unwrap();
        assert_eq!(origin, value);
        assert_eq!(expected_value_len, value_src_len);
    }

    #[test]
    fn test_parse_array_empty() {
        let mut array_data: Vec<u8> = Vec::from("0\r\n");

        let expected_value_len = array_data.len();

        array_data.append(&mut "trash".as_bytes().to_vec());

        let origin = RespInternalValue::Array(Vec::new());
        let ParseResult { value, value_src_len }
            = parse_array(&array_data).unwrap().unwrap();
        assert_eq!(origin, value);
        assert_eq!(expected_value_len, value_src_len);
    }

    #[test]
    fn test_parse_array_boundaries() {
        // receive an incomplete message
        assert!(parse_resp_value(Vec::from("*7\r").as_mut_slice()).unwrap().is_none(), "expected Ok(None)");
        // receive an incomplete message
        assert!(parse_resp_value(Vec::from("*7\r\n*").as_mut_slice()).unwrap().is_none(), "expected Ok(None)");
        // receive an incomplete message
        assert!(parse_resp_value(Vec::from("*1\r\n$").as_mut_slice()).unwrap().is_none(), "expected Ok(None)");
        // receive incorrect message: array's len ends without CRLF
        assert!(parse_resp_value(Vec::from("*1\r#$").as_mut_slice()).is_err(), "expected Err");
        // receive incorrect message: array's element ends without CRLF
        assert!(parse_resp_value(Vec::from("*1\r\n:12\r$").as_mut_slice()).is_err(), "expected Err");
    }
}

