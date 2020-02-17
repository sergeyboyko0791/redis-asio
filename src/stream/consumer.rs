// TODO
use crate::*;
use super::{SubscribeOptions,
            RedisGroup,
            StreamEntry,
            RangeEntry,
            ReadExplicitOptions,
            RangeOptions,
            RangeType,
            parse_stream_entries,
            parse_range_entries};

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
    connection: RedisCoreConnection,
}

impl RedisStreamConsumer {
    pub fn connect(addr: &SocketAddr)
                   -> impl Future<Item=RedisStreamConsumer, Error=RedisError> + Send + 'static {
        RedisCoreConnection::connect(addr)
            .map(|connection| Self { connection })
    }

    pub fn read_explicit(self, options: ReadExplicitOptions)
                         -> impl Future<
                             Item=(RedisStreamConsumer, Vec<StreamEntry>),
                             Error=RedisError>
                         + Send + 'static {
        self.connection.send(read_explicit_cmd(options))
            .and_then(|(connection, response)|
                Ok((RedisStreamConsumer { connection }, parse_stream_entries(response)?))
            )
    }

    pub fn range(self, options: RangeOptions)
                 -> impl Future<
                     Item=(RedisStreamConsumer, Vec<RangeEntry>),
                     Error=RedisError>
                 + Send + 'static {
        self.connection.send(range_cmd(options))
            .and_then(|(connection, response)|
                Ok((RedisStreamConsumer { connection }, parse_range_entries(response)?))
            )
    }

    pub fn subscribe(self, options: SubscribeOptions)
                     -> impl Future<Item=Subscribe, Error=RedisError> + Send + 'static {
        let RedisCoreConnection { sender, receiver } = self.connection;

        // send first subscription request
        sender
            .send(subscribe_cmd(options.clone()))
            .map(move |sender| {
                // run recursive server message processing
                Subscribe {
                    stream: Box::new(receive_and_send_recursive(receiver, sender, options))
                }
            }).map_err(|err| RedisError::new(RedisErrorKind::ConnectionError,
                                             format!("Could not send listen request: {:?}", err)))
    }
}

pub struct Subscribe {
    stream: Box<dyn Stream<Item=RedisValue, Error=RedisError> + Send + 'static>,
}

impl Stream for Subscribe {
    type Item = Vec<StreamEntry>;
    type Error = RedisError;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        self.stream.poll()
            .and_then(|value| {
                let value = match value {
                    Async::Ready(x) => x,
                    _ => return Ok(Async::NotReady)
                };
                let value = match value {
                    Some(x) => x,
                    _ => return Ok(Async::Ready(None)),
                };

                parse_stream_entries(value)
                    .map(|stream_entries| Async::Ready(Some(stream_entries)))
            })
    }
}

enum StreamInternalCommand {
    ListenNextMessage,
}

fn receive_and_send_recursive<F, T>(from_srv: F, to_srv: T, options: SubscribeOptions)
                                    -> impl Stream<Item=RedisValue, Error=RedisError> + Send + 'static
    where F: Stream<Item=RespInternalValue, Error=RedisError> + Send + 'static,
          T: Sink<SinkItem=RedisCommand, SinkError=RedisError> + Send + 'static {
    // Redis Streams protocol is a simple request-response protocol,
    // and we should not receive more than one packet before the rx Receiver<StreamInternalCommand>
    const BUFFER_SIZE: usize = 1;
    let (tx, rx) =
        channel::<StreamInternalCommand>(BUFFER_SIZE);

    let output = fwd_from_channel_to_srv(to_srv, rx, options);
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
                              options: SubscribeOptions)
                              -> impl Future<Item=(), Error=RedisError> + Send + 'static
    where T: Sink<SinkItem=RedisCommand, SinkError=RedisError> + Send + 'static {
    rx
        .map_err(|err| RedisError::new(RedisErrorKind::InternalError,
                                       "Cannot read from internal channel".into()))
        .fold(to_srv, move |to_srv, msg| {
            match msg {
                StreamInternalCommand::ListenNextMessage =>
                    to_srv.send(subscribe_cmd(options.clone()))
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
                    RedisValue::from_resp_value(msg)
                })
        })
}

fn xread_cmd_from(group: Option<RedisGroup>) -> RedisCommand {
    let cmd =
        match &group {
            Some(_) => "XREADGROUP",
            _ => "XREAD",
        };

    let mut cmd = command(cmd);
    match group {
        Some(RedisGroup { group, consumer }) =>
            cmd.arg("GROUP")
                .arg(group.as_str())
                .arg(consumer.as_str()),
        _ => cmd
    }
}

fn subscribe_cmd(options: SubscribeOptions) -> RedisCommand
{
    let SubscribeOptions { stream, group } = options;
    xread_cmd_from(group)
        .arg("BLOCK")
        .arg("0") // block until next pkt
        .arg("STREAMS")
        .arg(stream.as_str())
        .arg("$") // receive only new messages
}

fn read_explicit_cmd(options: ReadExplicitOptions) -> RedisCommand
{
    let ReadExplicitOptions { stream, group, count, start_id } = options;
    xread_cmd_from(group)
        .arg("COUNT")
        .arg(count as i64)
        .arg("STREAMS")
        .arg(stream.as_str())
        .arg(start_id.to_string())
}

fn range_cmd(options: RangeOptions) -> RedisCommand
{
    let RangeOptions { stream, count, range } = options;

    let (left, right) = match range {
        RangeType::GreaterThan(left) => (left.to_string(), "+".to_string()),
        RangeType::LessThan(right) => ("-".to_string(), right.to_string()),
        RangeType::GreaterLessThan(left, right) => (left.to_string(), right.to_string()),
    };

    command("XRANGE")
        .arg(stream)
        .arg(left)
        .arg(right)
        .arg("COUNT")
        .arg(count as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::EntryId;

    #[test]
    fn test_subscribe() {
        let options = SubscribeOptions::new("test_stream".to_string());

        tokio::run(
            RedisStreamConsumer::connect(&"127.0.0.1:6379".parse::<SocketAddr>().unwrap())
                .and_then(move |consumer| consumer.subscribe(options))
                .and_then(|subscription|
                    subscription.for_each(|entries| Ok(println!("Subscribe response: {:?}", entries))))
                .map_err(|err| println!("On error: {:?}", err))
        );
    }

    #[test]
    fn test_read_explicit() {
        let options = ReadExplicitOptions::new("test_stream".to_string(), 2, EntryId::new(0, 0));

        tokio::run(
            RedisStreamConsumer::connect(&"127.0.0.1:6379".parse::<SocketAddr>().unwrap())
                .and_then(move |consumer| consumer.read_explicit(options))
                .map(|(consumer, entries)|
                    println!("Read explicit response: {:?}", entries))
                .map_err(|err| println!("On error: {:?}", err))
        );
    }

    #[test]
    fn test_range() {
        let options
            = RangeOptions::new("test_stream".to_string(),
                                2,
                                RangeType::GreaterLessThan(
                                    EntryId::new(1581870414714, 0),
                                    EntryId::new(1581914804239, 0))).unwrap();

        tokio::run(
            RedisStreamConsumer::connect(&"127.0.0.1:6379".parse::<SocketAddr>().unwrap())
                .and_then(move |consumer| consumer.range(options))
                .map(|(consumer, entries)|
                    println!("Range response: {:?}", entries))
                .map_err(|err| println!("On error: {:?}", err))
        );
    }
}
