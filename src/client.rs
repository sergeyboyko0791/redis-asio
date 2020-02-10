// todo remove it to tests
extern crate tokio;

use crate::{RedisCodec, RespInternalValue, RedisCoreError, RedisErrorKind, RedisValue};
use tokio_tcp::TcpStream;
use std::net::SocketAddr;
use futures::{Future, Stream, Sink};
use futures::*;
use tokio_core::io::Framed;
use tokio_io::AsyncRead;
use tokio_io::io;
use futures::sync::mpsc::unbounded;
use futures::sync::mpsc;
use futures::sync::mpsc::{Sender, Receiver};
use bytes::BytesMut;
use std::error::Error;

struct RedisCoreClient<S>
    where S: Stream {
    stream: S
}

//impl<S> RedisCoreClient<S>
//    where S: Stream {
//    pub fn new(stream: S) -> RedisCoreClient<S> {
//        RedisCoreClient { stream }
//    }
//}

impl<S> RedisCoreClient<S>
    where S: Stream<Item=RespInternalValue, Error=RedisCoreError> {
    pub fn new(stream: S) -> RedisCoreClient<S> {
        RedisCoreClient { stream }
    }
}

//impl Stream for RedisCoreClient {
//    type Item = RespInternalValue;
//}

pub fn connect(addr: SocketAddr) -> impl Future<Item=(), Error=()> + Send + 'static {
//    let stream = TcpStream::connect(&addr)
//        .map_err(|err|
//            RedisCoreError::from(RedisErrorKind::ConnectionError,
//                                 format!("Could not connect to the {}", &addr)))
//        .map(|stream| {
//            let framed = stream.framed(RedisCodec {});
//            let f = framed.and_then(|res| {
//                from_resp_internal_value(res)
//            });
//            f
//        });
//    let s: Box<Future<Item=_, Error=_>> = Box::new(stream);
//    stream;


    TcpStream::connect(&addr)
        .map_err(|err| println!("Error 1: {:?}", err))
//        .map_err(|err|
//            RedisCoreError::from(RedisErrorKind::ConnectionError,
//                                 format!("Could not connect to the {}", &addr)))
        .and_then(process_stream)
}

pub fn from_resp_internal_value(resp_value: RespInternalValue) -> Result<RedisValue, RedisCoreError> {
    match resp_value {
        RespInternalValue::Nil => Ok(RedisValue::Nil),
        RespInternalValue::Error(x) => Err(RedisCoreError::from(RedisErrorKind::ReceiveError, x)),
        RespInternalValue::Status(x) => match x.as_str() {
            "Ok" => Ok(RedisValue::Ok),
            _ => Ok(RedisValue::Status(x))
        },
        RespInternalValue::Int(x) => Ok(RedisValue::Int(x)),
        RespInternalValue::BulkString(x) => Ok(RedisValue::BulkString(x)),
        RespInternalValue::Array(x) => {
            let mut res: Vec<RedisValue> = Vec::with_capacity(x.len());
            for val in x.into_iter() {
                res.push(from_resp_internal_value(val)?);
            }
            Ok(RedisValue::Array(res))
        }
    }
}

fn process_stream(stream: TcpStream) -> impl Future<Item=(), Error=()> + Send + 'static {
    let framed = stream.framed(RedisCodec {});
    let (tx, rx) = framed.split();
    let from_srv: Box<dyn Stream<Item=RespInternalValue, Error=RedisCoreError> + Send + 'static> = Box::new(rx);
    let to_srv: Box<dyn Sink<SinkItem=RespInternalValue, SinkError=RedisCoreError> + Send + 'static> = Box::new(tx);

    to_srv
        .send(listen_until())
        .map_err(|err| ()) // TODO delete it
        .and_then(move |mut to_srv|
            receive_and_send_recursive(from_srv, to_srv)
        )
}

fn receive_and_send_recursive<F, T>(from_srv: F, to_srv: T)
                                    -> impl Future<Item=(), Error=()> + Send + 'static
    where F: Stream<Item=RespInternalValue, Error=RedisCoreError> + Send + 'static,
          T: Sink<SinkItem=RespInternalValue, SinkError=RedisCoreError> + Send + 'static {
    let (tx,
        rx)
        = mpsc::channel::<RespInternalValue>(16);

    let output = forward_from_channel_to_srv(to_srv, rx);
    let input = process_from_srv_and_notify_channel(from_srv, tx);
    input.select(output).map(|(_, _)| ()).map_err(|(err, _)| ())
}

fn forward_from_channel_to_srv<T>(to_srv: T, rx: Receiver<RespInternalValue>)
                                  -> impl Future<Item=(), Error=()> + Send + 'static
    where T: Sink<SinkItem=RespInternalValue, SinkError=RedisCoreError> + Send + 'static {
    rx
        .map_err(|err| redis_core_error_new())
        .fold(to_srv, |to_srv, msg| {
            to_srv.send(msg)
        })
        .map(|_| ())
        .map_err(|_| ())
}

fn process_from_srv_and_notify_channel<F>(from_srv: F, tx: Sender<RespInternalValue>)
                                          -> impl Future<Item=(), Error=()> + Send + 'static
    where F: Stream<Item=RespInternalValue, Error=RedisCoreError> + Send + 'static
{
    from_srv
        .map_err(|err| ())
        .and_then(move |msg| {
            println!("Receive message: {:?}", &msg);
            // TODO change the type of channel from RespInternalValue to an flag
            tx.clone().send(listen_until())
                .map(|x| msg)
                .map_err(|err| ())
        }).for_each(|msg| Ok(()))
}

/// TODO delete it after debug
fn redis_core_error_from<E>(err: E) -> RedisCoreError
    where E: std::error::Error
{
    RedisCoreError::from(RedisErrorKind::ConnectionError,
                         format!("Handled an error: {}", err.description()))
}

/// TODO delete it after debug
fn redis_core_error_new() -> RedisCoreError
{
    RedisCoreError::from(RedisErrorKind::ConnectionError,
                         format!("Something went wrong"))
}

/// TODO delete it after debug
fn listen_until() -> RespInternalValue
{
    RespInternalValue::Array(
        vec![
            RespInternalValue::BulkString("xread".as_bytes().to_vec()),
            RespInternalValue::BulkString("block".as_bytes().to_vec()),
            RespInternalValue::BulkString("0".as_bytes().to_vec()),
            RespInternalValue::BulkString("streams".as_bytes().to_vec()),
            RespInternalValue::BulkString("test_stream".as_bytes().to_vec()),
            RespInternalValue::BulkString("$".as_bytes().to_vec())
        ])
}

#[test]
fn test_connect() {
    tokio::run(connect("127.0.0.1:6379".parse::<SocketAddr>().unwrap()));
}


// subscribe the to_srv on the unbounded_receiver
//                    unbounded_receiver.fold(to_srv, |to_srv, msg| to_srv.send(msg));
//
//                    from_srv.for_each(|message| {
//                        println!("Response: {:?}", &message);
//                        unbounded_sender.clone().send_all(listen_until()).then(|res| Ok(()))
//                    })
//                }).map_err(|err| println!("Error: {:?}", err))
//        })
//        .map_err(|err| ())
