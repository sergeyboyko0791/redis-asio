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
    TcpStream::connect(&addr)
        .map_err(|err| println!("Error 1: {:?}", err))
//        .map_err(|err|
//            RedisCoreError::from(RedisErrorKind::ConnectionError,
//                                 format!("Could not connect to the {}", &addr)))
        .and_then(on_stream_established)
}

pub fn redis_value_from_resp(resp_value: RespInternalValue) -> Result<RedisValue, RedisCoreError> {
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
                res.push(redis_value_from_resp(val)?);
            }
            Ok(RedisValue::Array(res))
        }
    }
}

// Send first subscribe request and run recursive server message processing
fn on_stream_established(stream: TcpStream)
                         -> impl Future<Item=(), Error=()> + Send + 'static {
    let framed = stream.framed(RedisCodec {});
    let (to_srv, from_srv) = framed.split();

    // send first subscription request
    to_srv
        .send(listen_until())
        .map_err(|err| ()) // TODO delete it
        .and_then(
            move |mut to_srv| {
                // run recursive server message processing
                receive_and_send_recursive(from_srv, to_srv)
                    .for_each(|msg| Ok(println!("Received message: {:?}", msg)))
                    .map_err(|err| ())
            }
        )
}

fn receive_and_send_recursive<F, T>(from_srv: F, to_srv: T)
                                    -> impl Stream<Item=RedisValue, Error=RedisCoreError> + Send + 'static
    where F: Stream<Item=RespInternalValue, Error=RedisCoreError> + Send + 'static,
          T: Sink<SinkItem=RespInternalValue, SinkError=RedisCoreError> + Send + 'static {
    let (tx,
        rx)
        = mpsc::channel::<RespInternalValue>(16);

    let output = fwd_from_channel_to_srv(to_srv, rx);
    let input = process_from_srv_and_notify_channel(from_srv, tx);

    // We have the following conditions:
    // 1) a return stream should include both output future and input stream
    // 2) select() method requires equal types of Item within two merging streams,
    // 3) a return stream should has Item = RedisValue
    // 4) output future should not influence a return stream
    //
    // change Item to Option<RedisValue> within the input stream and output future
    // where output future will not influence a selected stream (via filter_map())

    let output = output.map(|_| None);
    let input = input.map(|x| Some(x));

    input.select(output.into_stream()).filter_map(|x| x)
}

fn fwd_from_channel_to_srv<T>(to_srv: T, rx: Receiver<RespInternalValue>)
                              -> impl Future<Item=(), Error=RedisCoreError> + Send + 'static
    where T: Sink<SinkItem=RespInternalValue, SinkError=RedisCoreError> + Send + 'static {
    rx
        .map_err(|err| redis_core_error_new())
        .fold(to_srv, |to_srv, msg| {
            to_srv.send(msg)
        })
        .map(|_| ())
}

fn process_from_srv_and_notify_channel<F>(from_srv: F, tx: Sender<RespInternalValue>)
                                          -> impl Stream<Item=RedisValue, Error=RedisCoreError> + Send + 'static
    where F: Stream<Item=RespInternalValue, Error=RedisCoreError> + Send + 'static
{
    from_srv
        .and_then(move |msg| {
            // TODO change the type of channel from RespInternalValue to an flag
            tx.clone().send(listen_until())
                .then(|res| {
                    match res {
                        Ok(_) => (),
                        Err(err) => return Err(redis_core_error_from(err))
                    }
                    // convert RespInternalValue to RedisValue
                    // note: the function returns an error if the Resp value is Error
                    //       else returns RedisValue
                    redis_value_from_resp(msg)
                })
        })
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

/// TODO refactor
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

