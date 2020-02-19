use crate::{RedisValue, RedisCoreConnection, RedisError, RedisErrorKind,
            ToRedisArgument, from_redis_value};
use super::*;

use std::error::Error;
use std::net::SocketAddr;
use std::collections::HashMap;
use futures::{Future, Sink};


pub struct RedisStream {
    connection: RedisCoreConnection,
}

impl RedisStream {
    pub fn connect(addr: &SocketAddr)
                   -> impl Future<Item=RedisStream, Error=RedisError> + Send + 'static {
        RedisCoreConnection::connect(addr)
            .map(|connection| Self { connection })
    }

    pub fn send_entry<T>(self, options: AddOptions, key_values: HashMap<String, T>)
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
                        -> impl Future<Item=(Self, Vec<StreamEntry>), Error=RedisError> + Send + 'static {
        self.connection.send(pending_list_command(options))
            .and_then(|(connection, response)| {
                Ok((RedisStream { connection }, parse_stream_entries(response)?))
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
