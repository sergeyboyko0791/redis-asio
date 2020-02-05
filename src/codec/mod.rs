use crate::{RedisCoreError, RedisErrorKind};
use tokio_io::codec::{Encoder, Decoder};
use bytes::BytesMut;

mod encode;
mod decode;


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
        Ok(())
    }
}

impl Decoder for RedisCodec {
    type Item = RespInternalValue;
    type Error = RedisCoreError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let data = buf.as_ref();
        Ok(None)
    }
}
