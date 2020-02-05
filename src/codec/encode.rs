use crate::{RedisCoreError, RedisErrorKind};
use crate::codec::RespInternalValue;
use std::io::Cursor;
use std::error::Error;
use byteorder::WriteBytesExt;
use bytes::BytesMut;

fn encode_resp_value(value: RespInternalValue) -> Vec<u8> {
    match value {
        RespInternalValue::Nil => "$-1\r\n".as_bytes().to_vec(),
        RespInternalValue::Error(x) => format!("-{}\r\n", x).into_bytes(),
        RespInternalValue::Status(x) => format!("+{}\r\n", x).into_bytes(),
        RespInternalValue::Int(x) => format!(":{}\r\n", x.to_string()).into_bytes(),
        RespInternalValue::BulkString(mut x) => {
            let mut res = format!("${}\r\n", x.len()).into_bytes();
            res.append(&mut x);
            res.append(&mut "\r\n".as_bytes().to_vec());
            res
        }
        RespInternalValue::Array(x) => {
            let mut res = format!("*{}\r\n", x.len()).into_bytes();
            for val in x.into_iter() {
                res.append(&mut encode_resp_value(val))
            }
            res
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode() {
        assert_eq!("$-1\r\n".as_bytes().to_vec(), encode_resp_value(RespInternalValue::Nil));
        assert_eq!("-Error message\r\n".as_bytes().to_vec(),
                   encode_resp_value(RespInternalValue::Error("Error message".to_string())));
        assert_eq!(":1000\r\n".as_bytes().to_vec(),
                   encode_resp_value(RespInternalValue::Int(1000)));
        assert_eq!("$8\r\nfoo\r\nbar\r\n".as_bytes().to_vec(),
                   encode_resp_value(RespInternalValue::BulkString("foo\r\nbar".as_bytes().to_vec())));
        assert_eq!("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n".as_bytes().to_vec(),
                   encode_resp_value(
                       RespInternalValue::Array(
                           vec![RespInternalValue::BulkString("foo".as_bytes().to_vec()),
                                RespInternalValue::BulkString("bar".as_bytes().to_vec())]
                       )
                   )
        );
    }
}
