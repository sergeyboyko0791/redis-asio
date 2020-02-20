use tokio_codec::Decoder;
use tokio_tcp::TcpStream;
use futures::{Future, Stream, Sink, Async, try_ready};
use crate::{RedisValue, RedisCommand, RespInternalValue, RedisCodec, RedisError, RedisErrorKind};
use std::net::SocketAddr;
use core::marker::Send as SendMarker;
use std::error::Error;


/// Actual Redis connection converts packets from `RESP` packets into `RedisValue`
/// and from `RedisCommand` into `RESP` packets.
///
/// # Example
/// ```
/// use std::net::SocketAddr;
/// use futures::Future;
/// use redis_asio::{RedisCoreConnection, RedisValue, from_redis_value};
///
/// let address = &"127.0.0.1:6379".parse::<SocketAddr>().unwrap();
///
/// let set_req = command("SET").arg("foo").arg(123);
/// let get_req = command("GET").arg("foo");
///
/// let future = RedisCoreConnection::connect(address)
///     .and_then(move |con| con.send(set_req))
///     .and_then(|(con, response)| {
///         assert_eq!(RedisValue::Ok, response, "Expect Ok");
///         con.send(get_req)
///     })
///     .map(move |(_, response)|
///         assert_eq!(123, from_redis_value(&response).unwrap()))
///     .map_err(|_| unreachable!());
///  tokio::run(future);
/// ```
pub struct RedisCoreConnection {
    pub(crate) sender: Box<dyn Sink<SinkItem=RedisCommand, SinkError=RedisError> + SendMarker + 'static>,
    pub(crate) receiver: Box<dyn Stream<Item=RespInternalValue, Error=RedisError> + SendMarker + 'static>,
}

impl RedisCoreConnection {
    /// Open a connection to Redis server and wrap it into `RedisCoreConnection`,
    /// that will be available in the future.
    pub fn connect(addr: &SocketAddr) -> impl Future<Item=Self, Error=RedisError> {
        TcpStream::connect(addr)
            .map_err(|err| RedisError::new(RedisErrorKind::ConnectionError, err.description().to_string()))
            .map(|stream| {
                let codec = RedisCodec;
                let (tx, rx) = codec.framed(stream).split();
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

    /// Send request as a `RedisCommand` and return `Send` represents the future
    /// `Future<Item=(RedisCoreConnection, RedisValue), Error=RedisError>`
    pub fn send(self, req: RedisCommand) -> Send {
        Send::new(self, req)
    }
}

/// The `Future<Item=(RedisCoreConnection, RedisValue), Error=RedisError>` wrapper
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
            _ => Err(RedisError::new(RedisErrorKind::ConnectionError,
                                     "Connection has closed before an answer came".to_string()))
        }
    }
}
