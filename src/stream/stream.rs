use crate::{RedisValue, RedisCoreConnection, RedisError, RedisErrorKind,
            IntoRedisArgument, from_redis_value};
use super::*;

use std::error::Error;
use std::net::SocketAddr;
use std::collections::HashMap;
use futures::{Future, Sink};


/// The structure represents a Redis connection that provides interface for
/// working with Redis Stream "https://redis.io/topics/streams-intro".
///
/// The structure wraps an actual `RedisCoreConnection`,
/// converts RedisValue into and from considered structures that are easier
/// to use in Redis Stream context.
///
/// See more examples in `examples` directory.
pub struct RedisStream {
    connection: RedisCoreConnection,
}

impl RedisStream {
    /// Open a connection to Redis server and wrap it into `RedisStream`,
    /// that will be available in the future.
    pub fn connect(addr: &SocketAddr)
                   -> impl Future<Item=RedisStream, Error=RedisError> + Send + 'static {
        RedisCoreConnection::connect(addr)
            .map(|connection| Self { connection })
    }

    /// Send an entry that will be constructed by options and pairs of key-values.
    ///
    /// # Example
    ///
    /// ```
    /// use std::net::SocketAddr;
    /// use std::collections::HashMap;
    /// use futures::Future;
    /// use redis_asio::{RedisArgument, IntoRedisArgument};
    /// use redis_asio::stream::{RedisStream, SendEntryOptions, EntryId};
    ///
    /// let address = &"127.0.0.1:6379".parse::<SocketAddr>().unwrap();
    /// let send_options = SendEntryOptions::new("mystream".to_string());
    ///
    /// let mut request: HashMap<String, RedisArgument> = HashMap::new();
    /// request.insert("type".to_string(), 3i32.into_redis_argument());
    /// request.insert("data".to_string(), "Hello, world!".into_redis_argument());
    ///
    /// let future = RedisStream::connect(address)
    ///     .and_then(move |stream: RedisStream| {
    ///         // HashMap<String, RedisArgument> satisfies the
    ///         // HashMap<String, ToRedisArgument>
    ///         stream.send_entry(send_options, request)
    ///     })
    ///     .map(|(_, inserted_entry_id): (RedisStream, EntryId)| {
    ///         println!("{:?} has sent", inserted_entry_id.to_string());
    ///     })
    ///     .map_err(|err| eprintln!("something went wrong: {}", err));
    /// tokio::run(future);
    /// ```
    pub fn send_entry<T>(self, options: SendEntryOptions, key_values: HashMap<String, T>)
                         -> impl Future<Item=(RedisStream, EntryId), Error=RedisError> + Send + 'static
        where T: IntoRedisArgument {
        self.connection.send(add_command(options, key_values))
            .and_then(|(connection, response)| {
                let entry_id_string = from_redis_value(&response)?;
                let entry_id = EntryId::from_string(entry_id_string)?;
                Ok((Self { connection }, entry_id))
            })
    }

    /// Read entries with IDs greater than specified `start_id`.
    ///
    /// # Example
    ///
    /// ```
    /// use std::net::SocketAddr;
    /// use std::collections::HashMap;
    /// use futures::Future;
    /// use redis_asio::stream::{RedisStream, ReadExplicitOptions, EntryId,
    ///                          StreamEntry};
    ///
    /// let address = &"127.0.0.1:6379".parse::<SocketAddr>().unwrap();
    /// // start_id = "0-0" means get any entries
    /// let mut read_options =
    ///     ReadExplicitOptions::new("stream1".to_string(), EntryId::new(0, 0), 10);
    /// read_options.add_stream("stream2".to_string(), EntryId::new(0, 0));
    ///
    /// let future = RedisStream::connect(address)
    ///     .and_then(move |stream: RedisStream| {
    ///         stream.read_explicit(read_options)
    ///     })
    ///     .map(|(_, entries): (RedisStream, Vec<StreamEntry>)| {
    ///         for entry in entries.into_iter() {
    ///             println!("Received: {:?}", entry);
    ///         }
    ///     })
    ///     .map_err(|err| eprintln!("something went wrong: {}", err));
    /// tokio::run(future);
    /// ```
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

    /// Get entries in specified range.
    ///
    /// # Example
    ///
    /// ```
    /// use std::net::SocketAddr;
    /// use futures::Future;
    /// use redis_asio::stream::{RedisStream, RangeOptions, RangeType, RangeEntry};
    ///
    /// let address = &"127.0.0.1:6379".parse::<SocketAddr>().unwrap();
    /// let range_options =
    ///     RangeOptions::new("stream1".to_string(), 10, RangeType::Any).unwrap();
    ///
    /// let future = RedisStream::connect(address)
    ///     .and_then(move |stream: RedisStream| {
    ///         stream.range(range_options)
    ///     })
    ///     .map(|(_, entries): (RedisStream, Vec<RangeEntry>)| {
    ///         for entry in entries.into_iter() {
    ///             println!("Received: {:?}", entry);
    ///         }
    ///     })
    ///     .map_err(|err| eprintln!("something went wrong: {}", err));
    /// tokio::run(future);
    /// ```
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

    /// Subscribe to a Redis stream and process all incoming entries.
    /// Redis Streams requires to send XREAD/XREADGROUP requests every time
    /// the client receives a response on the previous,
    /// in other words Redis Streams does not provide an interface to subscribe
    /// to a Redis stream.
    ///
    /// In the Crate the subscription is possible by hidden requests sending
    /// within the Crate engine.
    ///
    /// Request that will be sent to get new entries in the following example:
    /// "XREADGROUP GROUP mygroup Bob BLOCK 0 STREAMS mystream <"
    ///
    /// # Example
    ///
    /// ```
    /// use std::net::SocketAddr;
    /// use futures::{Future, Stream};
    /// use redis_asio::stream::{RedisStream, SubscribeOptions, StreamEntry,
    ///                          RedisGroup};
    ///
    /// let address = &"127.0.0.1:6379".parse::<SocketAddr>().unwrap();
    /// let group_info = RedisGroup::new("mygroup".to_string(), "Bob".to_string());
    /// let subscribe_options =
    ///     SubscribeOptions::with_group(vec!["stream1".to_string()], group_info);
    ///
    /// let future = RedisStream::connect(address)
    ///     .and_then(move |stream: RedisStream| {
    ///         stream.subscribe(subscribe_options)
    ///     })
    ///     .and_then(|subscribe| /*:Subscribe*/ {
    ///         subscribe.for_each(|entries: Vec<StreamEntry>| {
    ///             for entry in entries.into_iter() {
    ///                 println!("Received: {:?}", entry);
    ///             }
    ///             Ok(())
    ///         })
    ///     })
    ///     .map_err(|err| eprintln!("something went wrong: {}", err));
    /// tokio::run(future);
    /// ```
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

    /// Acknowledge an entry by its ID.
    ///
    /// # Example
    /// ```
    /// use std::net::SocketAddr;
    /// use futures::{Future, Stream};
    /// use redis_asio::stream::{RedisStream, AckOptions, AckResponse, EntryId};
    ///
    /// let address = &"127.0.0.1:6379".parse::<SocketAddr>().unwrap();
    ///    let ack_options =
    ///        AckOptions::new(
    ///            "mystream".to_string(),
    ///            "mygroup".to_string(),
    ///            EntryId::new(0, 0));
    ///
    ///    let future = RedisStream::connect(address)
    ///        .and_then(move |stream: RedisStream| {
    ///            stream.ack_entry(ack_options)
    ///        })
    ///        .map(|(_, response): (RedisStream, AckResponse)| {
    ///            assert_eq!(AckResponse::Ok, response);
    ///        })
    ///        .map_err(|err| eprintln!("something went wrong: {}", err));
    ///    tokio::run(future);
    /// ```
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

    /// Get entries that was not acknowledged but was sent to specified consumer.
    ///
    /// # Example
    /// ```
    /// use std::net::SocketAddr;
    /// use futures::Future;
    /// use redis_asio::stream::{RedisStream, PendingOptions, StreamEntry, EntryId};
    ///
    /// let address = &"127.0.0.1:6379".parse::<SocketAddr>().unwrap();
    ///    let pending_options =
    ///        PendingOptions::new(
    ///            "mystream".to_string(),
    ///            "mygroup".to_string(),
    ///            "Bob".to_string(),
    ///            EntryId::new(0, 0)).unwrap();
    ///
    ///    let future = RedisStream::connect(address)
    ///        .and_then(move |stream: RedisStream| {
    ///            stream.pending_entries(pending_options)
    ///        })
    ///        .map(|(_, entries): (RedisStream, Vec<StreamEntry>)| {
    ///            for entry in entries.into_iter() {
    ///                println!("Received: {:?}", entry);
    ///            }
    ///        })
    ///        .map_err(|err| eprintln!("something went wrong: {}", err));
    ///    tokio::run(future);
    /// ```
    pub fn pending_entries(self, options: PendingOptions)
                           -> impl Future<Item=(Self, Vec<StreamEntry>), Error=RedisError> + Send + 'static {
        self.connection.send(pending_list_command(options))
            .and_then(|(connection, response)| {
                Ok((RedisStream { connection }, parse_stream_entries(response)?))
            })
    }

    /// Try to create a group. If the group exists already, do not return an error.
    ///
    /// # Example
    /// ```
    /// use std::net::SocketAddr;
    /// use futures::Future;
    /// use redis_asio::stream::{RedisStream, TouchGroupOptions, StreamEntry,
    ///                          EntryId};
    ///
    /// let address = &"127.0.0.1:6379".parse::<SocketAddr>().unwrap();
    ///    let touch_options =
    ///        TouchGroupOptions::new("mystream".to_string(), "mygroup".to_string());
    ///
    ///    let future = RedisStream::connect(&address)
    ///        .and_then(move |con|
    ///            // create group if the one does not exists yet
    ///            con.touch_group(touch_options))
    ///        // ignore an error if the group exists already
    ///        .then(|_| -> Result<(), ()> { Ok(()) });
    ///    tokio::run(future);
    /// ```
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
