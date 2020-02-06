use crate::{RedisCoreError, RedisErrorKind};
use tokio_io::codec::{Encoder, Decoder};
use bytes::BytesMut;

mod encode;
mod decode;

use encode::encode_resp_value;
use decode::{ParseResult, parse_resp_value};


#[derive(PartialEq, Eq, Debug)]
pub enum RespInternalValue {
    Nil,
    Error(String),
    Status(String),
    Int(i64),
    BulkString(Vec<u8>),
    Array(Vec<RespInternalValue>),
}

pub struct RedisCodec {}

impl Encoder for RedisCodec {
    type Item = RespInternalValue;
    type Error = RedisCoreError;

    fn encode(&mut self, item: Self::Item, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(encode_resp_value(item).as_ref());
        Ok(())
    }
}

impl Decoder for RedisCodec {
    type Item = RespInternalValue;
    type Error = RedisCoreError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let ParseResult { value, value_src_len } =
            match parse_resp_value(buf.as_ref())? {
                Some(x) => x,
                _ => return Ok(None),
            };

        assert!(value_src_len <= buf.len());
        let _ = buf.split_to(value_src_len);

        Ok(Some(value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode() {
        let mut codec = RedisCodec {};
        let mut buf = BytesMut::new();
        codec.encode(RespInternalValue::Status("Ok".to_string()), &mut buf).unwrap();
        assert_eq!("+Ok\r\n".as_bytes(), buf.as_ref());
    }

    #[test]
    fn test_decode() {
        let mut codec = RedisCodec {};
        let mut buf = BytesMut::from("+Ok\r\ntrash".as_bytes().to_vec());
        assert_eq!(RespInternalValue::Status("Ok".to_string()),
                   codec.decode(&mut buf).unwrap().unwrap());
        assert_eq!("trash".as_bytes().to_vec(), buf.as_ref());

        let mut buf = BytesMut::from("+Ok\r".as_bytes().to_vec());
        // receive an incomplete message
        assert!(codec.decode(&mut buf).unwrap().is_none());

        // receive an empty message
        let mut buf = BytesMut::from(Vec::new());
        assert!(codec.decode(&mut buf).unwrap().is_none());

        let mut buf = BytesMut::from("+Ok\r$".as_bytes().to_vec());
        assert!(codec.decode(&mut buf).is_err());
    }
}
