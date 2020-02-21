extern crate tokio;
extern crate futures;

use std::env;
use std::io;
use std::io::BufReader;
use std::thread;
use std::net::SocketAddr;
use std::collections::HashMap;
use futures::{Future, Stream, Sink};
use futures::sync::mpsc::{UnboundedReceiver, unbounded};

use redis_asio::{RedisResult, RedisError, RedisErrorKind, IntoRedisArgument, RedisArgument};
use redis_asio::stream::{RedisStream, TouchGroupOptions, SendEntryOptions};

struct Message(String);

/// Implements the trait to allow use the structure as a RedisArgument within RedisCommand::arg().
impl IntoRedisArgument for Message {
    fn into_redis_argument(self) -> RedisArgument {
        RedisArgument::String(self.0)
    }
}

impl Message {
    fn into_redis_stream_entry(self) -> HashMap<String, RedisArgument> {
        let mut result = HashMap::new();
        result.insert("type".to_string(), "Message".into_redis_argument());
        result.insert("data".to_string(), self.into_redis_argument());
        result
    }
}

fn main() {
    println!("Producer example has started");
    println!("Please enter a STREAM to write to it");
    let stream_name = read_stdin();
    println!("Please enter a GROUP (is used only to create if that does not exist)");
    let group_name = read_stdin();

    let redis_address = env::var("REDIS_URL")
        .unwrap_or("127.0.0.1:6379".to_string())
        .parse::<SocketAddr>().expect("Couldn't parse Redis URl");

    // Create an unbounded channel to allow the main thread notifies a child-network thread
    // of the need to send a Message to a Redis stream.
    let (tx, rx) = unbounded::<Message>();
    let child = thread::spawn(move ||
        // Spawn a child-network thread and run the producer
        start_producer(rx, stream_name, group_name, redis_address));

    println!("Please enter a message");
    let stdin = tokio::io::lines(BufReader::new(tokio::io::stdin()));
    let stdin = stdin
        .map_err(|err| eprintln!("Cannot read from stdin: {}", err))
        .for_each(move |line| {
            // Redirect the stdin stream to the channel sender (tx).
            tx.clone()
                .send(Message(line)).map(|_| ())
                .map_err(|err| eprintln!("Cannot read from stdin: {}", err))
        });

    tokio::run(stdin);
    // Wait the child thread to complete (it will happen because if we are here the tx is closed,
    // therefore rx listening will finish with an error).
    child.join().expect("Expect joined thread");
}

/// Creates and holds a connection to the Redis Server, waits new messages from
/// the channel receiver (rx) and send them to a Redis stream.
fn start_producer(rx: UnboundedReceiver<Message>,
                  stream_name: String,
                  group_name: String,
                  redis_address: SocketAddr) {
    let touch_options = TouchGroupOptions::new(stream_name.clone(), group_name.clone());

    // Try to create a group.
    // If the group exists already, the future will not be set into an error.
    // The create_group variable is the Future<Item=(), Error=()>.
    let create_group = RedisStream::connect(&redis_address)
        .and_then(move |con|
            // Create group if the one does not exists yet.
            con.touch_group(touch_options))
        .then(|_| -> RedisResult<()> { Ok(()) });

    // Creates and holds a connection to the Redis Server, waits new messages from
    // the channel receiver (rx) and send them to a Redis stream.
    //
    // Note nothing will happen if the previous future has failed.
    // The producer variable in a result is the Future<Item=(), Error=()>.
    // The Item and Error are required by tokio::run().
    let producer = create_group
        .and_then(move |_| {
            RedisStream::connect(&redis_address)
        })
        .and_then(move |producer| {
            rx
                .map_err(|_|
                    RedisError::new(RedisErrorKind::InternalError,
                                    "Something went wrong with UnboundedChannel".to_string()))
                // Use fold() to redirect messages from the channel receiver (rx) to the Redis stream.
                .fold(producer, move |producer, message| {
                    let options = SendEntryOptions::new(stream_name.clone());

                    // Serialize the message to pairs of key-value.
                    let data = message.into_redis_stream_entry();

                    producer
                        .send_entry(options, data)
                        .map(|(producer, added_entry_id)| {
                            println!("{:?} has sent", added_entry_id.to_string());
                            println!("Please enter a message");
                            producer
                        })
                })
        })
        .map(|_| ())
        .map_err(|err| println!("{}", err));

    tokio::run(producer);
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
