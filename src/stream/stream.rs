use crate::*;
use super::*;

use std::net::SocketAddr;
use futures::{Future, Stream, Sink};
use tokio_io::AsyncRead;
use futures::sync::mpsc::{channel, Sender, Receiver};
use std::error::Error;


pub struct RedisStream {
    connection: RedisCoreConnection,
}

impl RedisStream {
    pub fn connect(addr: &SocketAddr)
                   -> impl Future<Item=RedisStream, Error=RedisError> + Send + 'static {
        RedisCoreConnection::connect(addr)
            .map(|connection| Self { connection })
    }

    pub fn add<T>(self, options: AddOptions, key_values: Vec<(String, T)>)
                  -> impl Future<Item=(RedisStream, EntryId), Error=RedisError> + Send + 'static
        where T: ToRedisArgument {
        self.connection.send(add_command(options, key_values))
            .and_then(|(connection, response)| {
                let entry_id_string = from_redis_value(&response)?;
                let entry_id = EntryId::from_string(entry_id_string)?;
                Ok((Self { connection }, entry_id))
            })
    }

    pub fn read_explicit(self, options: ReadExplicitOptions)
                         -> impl Future<
                             Item=(RedisStream, Vec<StreamEntry>),
                             Error=RedisError>
                         + Send + 'static {
        self.connection.send(read_explicit_cmd(options))
            .and_then(|(connection, response)|
                Ok((RedisStream { connection }, parse_stream_entries(response)?))
            )
    }

    pub fn range(self, options: RangeOptions)
                 -> impl Future<
                     Item=(RedisStream, Vec<RangeEntry>),
                     Error=RedisError>
                 + Send + 'static {
        self.connection.send(range_cmd(options))
            .and_then(|(connection, response)|
                Ok((RedisStream { connection }, parse_range_entries(response)?))
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
                    stream: Box::new(subscribe(receiver, sender, options))
                }
            })
    }

    pub fn ack_entry(self, options: AckOptions)
                     -> impl Future<Item=(Self, AckResponse), Error=RedisError> + Send + 'static {
        self.connection.send(ack_entry_command(options))
            .and_then(|(connection, response)| {
                let response = match response {
                    RedisValue::Int(x) => AckResponse::new(x),
                    _ => return Err(RedisError::new(RedisErrorKind::ParseError, "Expect integer reply on XACK request".to_string())),
                };
                Ok((RedisStream { connection }, response))
            })
    }

    pub fn pending_list(self, options: PendingOptions)
                        -> impl Future<Item=(Self, Vec<PendingMessage>), Error=RedisError> + Send + 'static {
        self.connection.send(pending_list_command(options))
            .and_then(|(connection, response)| {
                Ok((RedisStream { connection }, from_redis_value(&response)?))
            })
    }

    pub fn touch_group(self, options: TouchGroupOptions)
                       -> impl Future<Item=(), Error=RedisError> + Send + 'static {
        self.connection.send(touch_group_command(options))
            .then(|res| {
                match res {
                    // do not keep the connection in anyway because we could receive BUSYGROUP from server
                    Ok((_connection, _)) => Ok(()),
                    Err(err) => {
                        if err.error == RedisErrorKind::ReceiveError
                            && err.description().contains("BUSYGROUP") {
                            return Ok(());
                        }
                        Err(err)
                    }
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        let options = AddOptions::new("test_stream".to_string());

        tokio::run(
            RedisStream::connect(&"127.0.0.1:6379".parse::<SocketAddr>().unwrap())
                .and_then(move |producer|
                    producer.add(options, vec![
                        ("key1".to_string(), "value1"),
                        ("key2".to_string(), "value2")])
                )
                .map(|(consumer, entry_id)|
                    println!("Add response: {:?}", entry_id))
                .map_err(|err| println!("On error: {:?}", err))
        );
    }

    #[test]
    fn test_subscribe() {
        let options = SubscribeOptions::new("test_stream".to_string());

        tokio::run(
            RedisStream::connect(&"127.0.0.1:6379".parse::<SocketAddr>().unwrap())
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
            RedisStream::connect(&"127.0.0.1:6379".parse::<SocketAddr>().unwrap())
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
            RedisStream::connect(&"127.0.0.1:6379".parse::<SocketAddr>().unwrap())
                .and_then(move |consumer| consumer.range(options))
                .map(|(consumer, entries)|
                    println!("Range response: {:?}", entries))
                .map_err(|err| println!("On error: {:?}", err))
        );
    }
}
