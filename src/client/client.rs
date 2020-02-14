use tokio_io::{AsyncRead, AsyncWrite};
use futures::{Future, Stream, Sink, Poll, Async, future, try_ready, task};
use crate::{RespInternalValue, RedisCodec, RedisCoreError, RedisErrorKind};
use tokio_tcp::TcpStream;
use std::net::SocketAddr;


pub struct RedisClient {
    pub(crate) sender: Box<dyn Sink<SinkItem=RespInternalValue, SinkError=RedisCoreError> + Send + 'static>,
    pub(crate) receiver: Box<dyn Stream<Item=RespInternalValue, Error=RedisCoreError> + Send + 'static>,
}

impl RedisClient {
    pub(crate) fn new<S, R>(sender: S, receiver: R) -> RedisClient
        where S: Sink<SinkItem=RespInternalValue, SinkError=RedisCoreError> + Send + 'static,
              R: Stream<Item=RespInternalValue, Error=RedisCoreError> + Send + 'static {
        let sender = Box::new(sender);
        let receiver = Box::new(receiver);
        RedisClient { sender, receiver }
    }

    pub fn send(self, req: RespInternalValue) -> SendRead {
        SendRead::new(self, req)
    }
}

pub struct SendRead {
    sender: Option<Box<dyn Sink<SinkItem=RespInternalValue, SinkError=RedisCoreError> + Send + 'static>>,
    receiver: Option<Box<dyn Stream<Item=RespInternalValue, Error=RedisCoreError> + Send + 'static>>,
    request: Option<RespInternalValue>,
    is_sent: bool,
}

impl SendRead {
    fn new(inner: RedisClient, request: RespInternalValue) -> SendRead {
        let sender = Some(inner.sender);
        let receiver = Some(inner.receiver);
        let request = Some(request);
        let is_sent = false;
        SendRead { sender, receiver, request, is_sent }
    }
}

impl Future for SendRead {
    type Item = (RedisClient, RespInternalValue);
    type Error = RedisCoreError;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        println!("poll()");
        let sender = self.sender.as_mut().unwrap();
        let receiver = self.receiver.as_mut().unwrap();

        if let Some(req) = self.request.take() {
            if sender.start_send(req)?.is_not_ready() {
//                task::current().notify();
                return Ok(Async::NotReady);
            }
        }

        if !self.is_sent {
            try_ready!(sender.poll_complete());
            self.is_sent = true;
//                task::current().notify();
        }

        // Request is sent already, lets read from receiver

        match try_ready!(receiver.poll()) {
            Some(response) => {
                let con =
                    RedisClient::new(self.sender.take().unwrap(), self.receiver.take().unwrap());
                Ok(Async::Ready((con, response)))
            }
            _ => Err(RedisCoreError::from(RedisErrorKind::ConnectionError,
                                          "Connection has closed before an answer came".to_string()))
        }
    }
}

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
            let cln = RedisClient::new(sender, receiver);
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


#[test]
fn test_tcp_future() {
//    TcpStream::connect(&"127.0.0.1:6379".parse::<SocketAddr>().unwrap())
//        .map_err(|err|
//            RedisCoreError::from(RedisErrorKind::ConnectionError,
//                                 format!("Could not connect to the {:?}", err)))
//        .and_then(move |stream| {
//            let (send, recv) = stream.framed(RedisCodec).split();
//            let stream = stream.framed(RedisCodec).poll();
//            let s = RedisCodec;
//
////            let recv: Box<impl Stream<Item=RespInternalValue, Error=RedisCoreError> + Send + 'static> = Box::new(recv);
//            let mut buff = Vec::new();
//            let res = tokio::io::read_until(stream,b'\n', buff)
//                .map(|(stream,buf)|
//                );
//            Ok(())
//        });
}
