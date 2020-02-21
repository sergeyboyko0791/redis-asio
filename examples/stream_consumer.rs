extern crate tokio;
extern crate futures;

use std::env;
use std::io;
use std::net::SocketAddr;
use std::collections::HashMap;
use futures::{Future, Stream, Sink};
use futures::sync::mpsc::{UnboundedSender, unbounded};

use redis_asio::{RedisResult, RedisValue, RedisError, RedisErrorKind, FromRedisValue, from_redis_value};
use redis_asio::stream::{RedisStream, StreamEntry, EntryId, AckResponse, SubscribeOptions,
                         RedisGroup, TouchGroupOptions, AckOptions};

#[derive(Debug)]
struct Message(String);

/// Implements the trait to allow implicit conversion from RedisValue to Message
/// via from_redis_value()
impl FromRedisValue for Message {
    fn from_redis_value(value: &RedisValue) -> RedisResult<Self> {
        match value {
            RedisValue::BulkString(data) => {
                let string = String::from_utf8(data.clone())
                    .map_err(|err|
                        RedisError::new(RedisErrorKind::ParseError,
                                        format!("Could not parse message: {}", err))
                    )?;
                // Construct a Message from received data
                Ok(Message(string))
            }
            _ => Err(RedisError::new(RedisErrorKind::ParseError,
                                     format!("Could not parse message from invalid RedisValue {:?}", value)))
        }
    }
}

impl Message {
    /// Tries to convert a Message from HashMap<String, RedisValue> (represents a Redis Stream entry).
    /// The entry should have the following structure:
    /// "type Message data \"Some data\""
    fn from_redis_stream_entry(key_values: &HashMap<String, RedisValue>) -> RedisResult<Self> {
        if key_values.len() != 2 {
            return Err(RedisError::new(RedisErrorKind::ParseError,
                                       "Invalid packet".to_string()));
        }

        let get_value =
            |key: &str| match key_values.get(key) {
                Some(x) => Ok(x),
                _ => Err(RedisError::new(RedisErrorKind::ParseError,
                                         "Invalid packet".to_string()))
            };

        let packet_type: String = from_redis_value(get_value("type")?)?;
        match packet_type.as_str() {
            "Message" => {
                let data: Message = from_redis_value(get_value("data")?)?;
                Ok(data)
            }
            _ => Err(RedisError::new(RedisErrorKind::ParseError,
                                     "Unknown message type".to_string()))
        }
    }
}

fn main() {
    println!("Consumer example has started");
    println!("Please enter a STREAM to listen on");
    let stream_name = read_stdin();
    println!("Please enter a GROUP");
    let group_name = read_stdin();
    println!("Please enter a CONSUMER");
    let consumer_name = read_stdin();

    let redis_address = env::var("REDIS_URL")
        .unwrap_or("127.0.0.1:6379".to_string())
        .parse::<SocketAddr>().expect("Couldn't parse Redis URl");

    let touch_options = TouchGroupOptions::new(stream_name.clone(), group_name.clone());

    // Try to create a group.
    // If the group exists already, the future will not be set into an error.
    // The create_group variable is the Future<Item=(), Error=()>.
    let create_group = RedisStream::connect(&redis_address)
        .and_then(move |con|
            // Create group if the one does not exists yet
            con.touch_group(touch_options))
        .then(|_| -> RedisResult<()> { Ok(()) });

    // Start the consuming after the group has been checked.
    //
    // Note nothing will happen if the previous future has failed.
    // The consumer variable in a result is the Future<Item=(), Error=()>.
    // The Item and Error are required by tokio::run().
    let consumer = create_group
        .and_then(move |_| {
            // Create two connections to the Redis Server:
            // first will be used for managing (send Acknowledge request),
            // second will be used for receive entries from Redis Server.
            let manager = RedisStream::connect(&redis_address);
            let consumer = RedisStream::connect(&redis_address);
            consumer.join(manager)
        })
        .and_then(move |(connection, manager)| {
            // Create an unbounded channel to allow the consumer notifies the manager
            // about received and unacknowledged yet entries.
            let (tx, rx) = unbounded::<EntryId>();

            // Copy of stream_name and group_name to move it into ack_entry future.
            let stream = stream_name.clone();
            let group = group_name.clone();

            let ack_entry = rx
                .map_err(|_|
                    RedisError::new(RedisErrorKind::InternalError,
                                    "Something went wrong with UnboundedChannel".to_string()))
                // Use fold() to redirect notification from the channel receiver (rx) to the manager.
                .fold(manager, move |manager, id_to_ack|
                    ack_stream_entry(manager, stream.clone(), group.clone(), id_to_ack))
                .map(|_| ())
                .map_err(|_| ());

            // Spawn the ack_entry future to be handled separately from the process_entry future.
            tokio::spawn(ack_entry);

            let group = RedisGroup::new(group_name, consumer_name);
            let options = SubscribeOptions::with_group(vec![stream_name], group);

            // Subscribe to a Redis stream, processes any incoming entries and sends
            // entry ids of success processed entries to the manager via the channel sender (tx).
            let process_entry =
                connection.subscribe(options)
                    .and_then(move |subscribe|
                        subscribe.for_each(move |entries|
                            process_stream_entries(tx.clone(), entries)));
            // Return and run later the process_entry future.
            process_entry
        })
        .map_err(|err| eprintln!("Something went wrong: {:?}", err));

    tokio::run(consumer);
}

fn read_stdin() -> String {
    let mut value = String::new();
    io::stdin().read_line(&mut value).expect("Expect a valid string");
    if value.ends_with("\n") {
        value.pop().expect("Expect no empty string");
    }

    assert!(!value.is_empty(), "Expect no empty string");
    value
}

fn ack_stream_entry(manager: RedisStream, stream: String, group: String, id_to_ack: EntryId)
                    -> impl Future<Item=RedisStream, Error=RedisError> {
    let options = AckOptions::new(stream.clone(), group.clone(), id_to_ack.clone());

    manager.ack_entry(options)
        .map(move |(manager, response)| {
            match response {
                AckResponse::Ok => println!("{:?} is acknowledged", id_to_ack.to_string()),
                AckResponse::NotExists =>
                    eprintln!("Couldn't acknowledge {:?}", id_to_ack.to_string())
            };
            manager
        })
}

fn process_stream_entries(acknowledger: UnboundedSender<EntryId>, entries: Vec<StreamEntry>)
                          -> RedisResult<()> {
    entries.into_iter()
        .for_each(move |entry| {
            let message =
                Message::from_redis_stream_entry(&entry.values);
            match message {
                Ok(message) =>
                    println!("Received message(ID={:?}): {:?}", entry.id.to_string(), message),
                Err(err) => {
                    eprintln!("{}", err);
                    // do not acknowledge the message
                    return;
                }
            }

            // Notifies the manager about the received and processed entry.
            let future = acknowledger.clone()
                .send(entry.id)
                .map(|_| ())
                .map_err(|_| ());
            tokio::spawn(future);
        });
    Ok(())
}
