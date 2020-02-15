// todo remove it to tests
extern crate tokio;

use crate::*;
use super::{RedisStreamOptions, RedisGroup};

use tokio_tcp::TcpStream;
use std::net::SocketAddr;
use futures::{Future, Stream, Sink};
use futures::*;
use tokio_core::io::Framed;
use tokio_io::AsyncRead;
use tokio_io::io;
use futures::sync::mpsc::{channel, Sender, Receiver};
use bytes::BytesMut;


pub struct RedisStreamConsumer {
    stream: Box<dyn Stream<Item=RedisValue, Error=RedisError> + Send + 'static>,
}

impl RedisStreamConsumer {
    pub fn subscribe(info: RedisStreamOptions, addr: SocketAddr)
                     -> impl Future<Item=RedisStreamConsumer, Error=RedisError> + Send + 'static {
        TcpStream::connect(&addr)
            .map_err(|err|
                RedisError::new(RedisErrorKind::ConnectionError,
                                format!("Could not connect to the {:?}", err)))
            .and_then(move |stream| on_stream_established(stream, info))
    }

    fn new(stream: impl Stream<Item=RedisValue, Error=RedisError> + Send + 'static) -> RedisStreamConsumer {
        let stream = Box::new(stream);
        RedisStreamConsumer { stream }
    }
}

enum StreamInternalCommand {
    ListenNextMessage,
}

impl Stream for RedisStreamConsumer {
    type Item = RedisValue;
    type Error = RedisError;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        self.stream.poll()
    }
}

pub fn redis_value_from_resp(resp_value: RespInternalValue) -> RedisResult<RedisValue> {
    match resp_value {
        RespInternalValue::Nil => Ok(RedisValue::Nil),
        RespInternalValue::Error(x) => Err(RedisError::new(RedisErrorKind::ReceiveError, x)),
        RespInternalValue::Status(x) => match x.as_str() {
            "OK" => Ok(RedisValue::Ok),
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
fn on_stream_established(stream: TcpStream, stream_info: RedisStreamOptions)
                         -> impl Future<Item=RedisStreamConsumer, Error=RedisError> + Send + 'static {
    let framed = stream.framed(RedisCodec {});
    let (to_srv, from_srv) = framed.split();

    // send first subscription request
    to_srv
        .send(listen_until(stream_info.clone()))
        .map(move |to_srv| {
            // run recursive server message processing
            RedisStreamConsumer::new(receive_and_send_recursive(from_srv, to_srv, stream_info))
        }).map_err(|err| RedisError::new(RedisErrorKind::ConnectionError,
                                         format!("Could not send listen request: {:?}", err)))
}

fn receive_and_send_recursive<F, T>(from_srv: F, to_srv: T, stream_info: RedisStreamOptions)
                                    -> impl Stream<Item=RedisValue, Error=RedisError> + Send + 'static
    where F: Stream<Item=RespInternalValue, Error=RedisError> + Send + 'static,
          T: Sink<SinkItem=RedisCommand, SinkError=RedisError> + Send + 'static {
    // Redis Streams protocol is a simple request-response protocol,
    // and we should not receive more than one packet before the rx Receiver<StreamInternalCommand>
    const BUFFER_SIZE: usize = 1;
    let (tx, rx) =
        channel::<StreamInternalCommand>(BUFFER_SIZE);

    let output = fwd_from_channel_to_srv(to_srv, rx, stream_info);
    let input
        = process_from_srv_and_notify_channel(from_srv, tx);

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

fn fwd_from_channel_to_srv<T>(to_srv: T,
                              rx: Receiver<StreamInternalCommand>,
                              stream_info: RedisStreamOptions)
                              -> impl Future<Item=(), Error=RedisError> + Send + 'static
    where T: Sink<SinkItem=RedisCommand, SinkError=RedisError> + Send + 'static {
    rx
        .map_err(|err| RedisError::new(RedisErrorKind::InternalError,
                                       "Cannot read from internal channel".into()))
        .fold(to_srv, move |to_srv, msg| {
            match msg {
                StreamInternalCommand::ListenNextMessage =>
                    to_srv.send(listen_until(stream_info.clone()))
            }
        })
        .map(|_| ())
}

fn process_from_srv_and_notify_channel<F>(from_srv: F,
                                          tx: Sender<StreamInternalCommand>)
                                          -> impl Stream<Item=RedisValue, Error=RedisError> + Send + 'static
    where F: Stream<Item=RespInternalValue, Error=RedisError> + Send + 'static
{
    from_srv
        .and_then(move |msg| {
            tx.clone().send(StreamInternalCommand::ListenNextMessage)
                .then(|res| {
                    match res {
                        Ok(_) => (),
                        Err(err) =>
                            return Err(RedisError::new(RedisErrorKind::ConnectionError,
                                                       format!("Could not send listen request: {:?}", err)))
                    }
                    // convert RespInternalValue to RedisValue
                    // note: the function returns an error if the Resp value is Error
                    //       else returns RedisValue
                    redis_value_from_resp(msg)
                })
        })
}

fn listen_until(stream_info: RedisStreamOptions) -> RedisCommand
{
    let RedisStreamOptions { stream, group } = stream_info;
    let cmd =
        match &group {
            Some(_) => "XREADGROUP",
            _ => "XREAD",
        };

    let mut cmd = command(cmd);
    cmd =
        match &group {
            Some(RedisGroup { group, consumer }) =>
                cmd.arg("GROUP")
                    .arg(group.as_str())
                    .arg(consumer.as_str()),
            _ => cmd
        };

    cmd.arg("BLOCK")
        .arg("0") // block until next pkt
        .arg("STREAMS")
        .arg(stream.as_str())
        .arg("$") // receive only new messages
}

#[test]
fn test_connect() {
    let info = RedisStreamOptions::new("test_stream".to_string());

    tokio::run(
        RedisStreamConsumer::subscribe(info, "127.0.0.1:6379".parse::<SocketAddr>().unwrap())
            .and_then(move |stream|
                stream.for_each(|msg| Ok(println!("Received message: {:?}", msg))))
            .map_err(|err| println!("On error: {:?}", err))
    );
}
