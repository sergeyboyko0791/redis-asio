use tokio_io::{AsyncRead, AsyncWrite};
use futures::{Future, Stream, Sink, Poll, Async, future, try_ready, task};
use crate::{RedisValue, RedisCommand, RespInternalValue, RedisCodec, RedisError, RedisErrorKind, command};
use tokio_tcp::TcpStream;
use std::net::SocketAddr;
use core::marker::Send as SendMarker;
use std::error::Error;


pub struct RedisCoreConnection {
    pub(crate) sender: Box<dyn Sink<SinkItem=RedisCommand, SinkError=RedisError> + SendMarker + 'static>,
    pub(crate) receiver: Box<dyn Stream<Item=RespInternalValue, Error=RedisError> + SendMarker + 'static>,
}

impl RedisCoreConnection {
    pub fn connect(addr: &SocketAddr) -> impl Future<Item=Self, Error=RedisError> {
        TcpStream::connect(addr)
            .map_err(|err| RedisError::from(RedisErrorKind::ConnectionError, err.description().into()))
            .map(|stream| {
                let (tx, rx) = stream.framed(RedisCodec).split();
                Self::new(tx, rx)
            })
    }

    pub(crate) fn new<S, R>(sender: S, receiver: R) -> RedisCoreConnection
        where S: Sink<SinkItem=RedisCommand, SinkError=RedisError> + SendMarker + 'static,
              R: Stream<Item=RespInternalValue, Error=RedisError> + SendMarker + 'static {
        let sender = Box::new(sender);
        let receiver = Box::new(receiver);
        RedisCoreConnection { sender, receiver }
    }

    pub fn send(self, req: RedisCommand) -> Send {
        Send::new(self, req)
    }
}

pub struct Send {
    sender: Option<Box<dyn Sink<SinkItem=RedisCommand, SinkError=RedisError> + SendMarker + 'static>>,
    receiver: Option<Box<dyn Stream<Item=RespInternalValue, Error=RedisError> + SendMarker + 'static>>,
    request: Option<RedisCommand>,
    is_sent: bool,
}

impl Send {
    fn new(inner: RedisCoreConnection, request: RedisCommand) -> Send {
        let sender = Some(inner.sender);
        let receiver = Some(inner.receiver);
        let request = Some(request);
        let is_sent = false;
        Send { sender, receiver, request, is_sent }
    }
}

impl Future for Send {
    type Item = (RedisCoreConnection, RedisValue);
    type Error = RedisError;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
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
                let redis_response = response.into_redis_value()?;
                let con =
                    RedisCoreConnection::new(self.sender.take().unwrap(), self.receiver.take().unwrap());
                Ok(Async::Ready((con, redis_response)))
            }
            _ => Err(RedisError::from(RedisErrorKind::ConnectionError,
                                      "Connection has closed before an answer came".to_string()))
        }
    }
}

// TODO implement effective tests
#[test]
fn tessssss() {
    let stream = TcpStream::connect(&"127.0.0.1:6379".parse::<SocketAddr>().unwrap());

    let set_req = command("set").arg("foo").arg(vec![1, 2, 3, 4, 5]);
    let get_req = command("get").arg("foo");

    let ft = RedisCoreConnection::connect(&"127.0.0.1:6379".parse::<SocketAddr>().unwrap())
        .and_then(move |con| con.send(set_req))
        .and_then(move |(con, resp)| {
            println!("Answer on \"set foo [1, 2, 3, 4, 5]\": {:?}", resp);
            con.send(get_req)
        })
        .map(|(stream, resp)|
            println!("Answer on \"get foo\": {:?}", resp))
        .map_err(|err| println!("Error!!! {:?}", err));
    tokio::run(ft);
}
