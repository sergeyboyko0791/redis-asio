use tokio_io::{AsyncRead, AsyncWrite};
use futures::{Future, Stream, Sink, Poll, Async, future, try_ready, task};
use crate::{RespInternalValue, RedisCodec, RedisCoreError, RedisErrorKind};
use tokio_tcp::TcpStream;
use std::net::SocketAddr;
use core::marker::Send as SendMarker;


pub struct RedisCoreConnection {
    pub(crate) sender: Box<dyn Sink<SinkItem=RespInternalValue, SinkError=RedisCoreError> + SendMarker + 'static>,
    pub(crate) receiver: Box<dyn Stream<Item=RespInternalValue, Error=RedisCoreError> + SendMarker + 'static>,
}

impl RedisCoreConnection {
    pub(crate) fn new<S, R>(sender: S, receiver: R) -> RedisCoreConnection
        where S: Sink<SinkItem=RespInternalValue, SinkError=RedisCoreError> + SendMarker + 'static,
              R: Stream<Item=RespInternalValue, Error=RedisCoreError> + SendMarker + 'static {
        let sender = Box::new(sender);
        let receiver = Box::new(receiver);
        RedisCoreConnection { sender, receiver }
    }

    pub fn send(self, req: RespInternalValue) -> Send {
        Send::new(self, req)
    }
}

pub struct Send {
    sender: Option<Box<dyn Sink<SinkItem=RespInternalValue, SinkError=RedisCoreError> + SendMarker + 'static>>,
    receiver: Option<Box<dyn Stream<Item=RespInternalValue, Error=RedisCoreError> + SendMarker + 'static>>,
    request: Option<RespInternalValue>,
    is_sent: bool,
}

impl Send {
    fn new(inner: RedisCoreConnection, request: RespInternalValue) -> Send {
        let sender = Some(inner.sender);
        let receiver = Some(inner.receiver);
        let request = Some(request);
        let is_sent = false;
        Send { sender, receiver, request, is_sent }
    }
}

impl Future for Send {
    type Item = (RedisCoreConnection, RespInternalValue);
    type Error = RedisCoreError;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        println!("poll()");
        let sender = self.sender.as_mut().unwrap();
        let receiver = self.receiver.as_mut().unwrap();

        if let Some(req) = self.request.take() {
            if sender.start_send(req)?.is_not_ready() {
                return Ok(Async::NotReady);
            }
        }

        if !self.is_sent {
            try_ready!(sender.poll_complete());
            self.is_sent = true;
        }

        // Request is sent already, lets read from receiver

        match try_ready!(receiver.poll()) {
            Some(response) => {
                let con =
                    RedisCoreConnection::new(self.sender.take().unwrap(), self.receiver.take().unwrap());
                Ok(Async::Ready((con, response)))
            }
            _ => Err(RedisCoreError::from(RedisErrorKind::ConnectionError,
                                          "Connection has closed before an answer came".to_string()))
        }
    }
}

// TODO implement effective tests
#[test]
fn tessssss() {
    let stream = TcpStream::connect(&"127.0.0.1:6379".parse::<SocketAddr>().unwrap());

    let req = RespInternalValue::Array(vec![
        RespInternalValue::BulkString("set".into()),
        RespInternalValue::BulkString("foo".into()),
        RespInternalValue::BulkString(vec![1, 2, 3, 4, 5])]);

    let req2 = RespInternalValue::Array(vec![
        RespInternalValue::BulkString("get".into()),
        RespInternalValue::BulkString("foo".into())]);

    let fut = stream
        .map_err(|err| RedisCoreError::new_some())
        .and_then(move |stream| {
            let framed = stream.framed(RedisCodec);
            let (sender, receiver) = framed.split();
            let cln = RedisCoreConnection::new(sender, receiver);
            cln.send(req)
        })
        .and_then(|(con, resp)| {
            println!("First response: {:?}", resp);
            con.send(req2)
        })
        .map(|(stream, resp)| println!("Second response: {:?}", resp))
        .map_err(|err| println!("Error!!! {:?}", err));
    tokio::run(fut);

    //.map(|(stream, resp)| println!("Received response!!! {:?}", resp))
    //                .map_err(|err| println!("Error!!! {:?}", err))
}
