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
use redis_asio::stream::{RedisStream, TouchGroupOptions, AddOptions};

struct Message(String);

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
    println!("Consumer example has started");
    println!("Please enter a STREAM to write to it");
    let stream_name = read_stdin();
    println!("Please enter a GROUP (is used only to create if that does not exist)");
    let group_name = read_stdin();

    let redis_address = env::var("REDIS_URL")
        .unwrap_or("127.0.0.1:6379".to_string())
        .parse::<SocketAddr>().expect("Couldn't parse Redis URl");

    let (tx, rx) = unbounded::<Message>();
    let child = thread::spawn(move ||
        start_producer(rx, stream_name, group_name, redis_address));

    println!("Please enter a message");
    let stdin = tokio::io::lines(BufReader::new(tokio::io::stdin()));
    let stdin = stdin
        .map_err(|err| eprintln!("Cannot read from stdin: {}", err))
        .for_each(move |line| {
            tx.clone()
                .send(Message(line)).map(|_| ())
                .map_err(|err| eprintln!("Cannot read from stdin: {}", err))
        });
    tokio::run(stdin);
    child.join().expect("Expect joined thread");
}

fn start_producer(rx: UnboundedReceiver<Message>,
                  stream_name: String,
                  group_name: String,
                  redis_address: SocketAddr) {
    let touch_options = TouchGroupOptions::new(stream_name.clone(), group_name.clone());

    let create_group = RedisStream::connect(&redis_address)
        .and_then(move |con|
            // create group if the one does not exists yet
            con.touch_group(touch_options))
        // ignore an error if the group exists already
        .then(|_| -> RedisResult<()> { Ok(()) });
    let producer = create_group
        .and_then(move |_| {
            RedisStream::connect(&redis_address)
        })
        .and_then(move |producer| {
            rx
                .map_err(|_|
                    RedisError::new(RedisErrorKind::InternalError,
                                    "Something went wrong with UnboundedChannel".to_string()))
                .fold(producer, move |producer, message| {
                    let options = AddOptions::new(stream_name.clone());

                    // serialize the message to pairs of key-value
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
