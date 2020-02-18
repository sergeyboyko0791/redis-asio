extern crate tokio;
extern crate futures;

use std::env;
use std::net::SocketAddr;
use futures::{Future, Stream, Sink};
use futures::future::join_all;
use futures::sync::mpsc::{UnboundedSender, UnboundedReceiver, unbounded};

use redis_asio::{RedisCommand, FromRedisValue, RedisCoreConnection, from_redis_value, command, RedisResult, RedisError, RedisErrorKind};
use redis_asio::stream::{RedisStream, StreamEntry, PendingOptions, RangeType, PendingMessage, EntryId, AckResponse, SubscribeOptions, RedisGroup};

const STREAM: &str = "ConsumerTest";
const GROUP: &str = "MyGroup";
const CONSUMER: &str = "CustomConsumer";

fn main() {
    let redis_address = env::var("REDIS_URL")
        .unwrap_or("127.0.0.1:6379".to_string())
        .parse::<SocketAddr>().expect("Couldn't parse Redis URl");

    let pending_options =
        PendingOptions::new(STREAM.to_string(),
                            GROUP.to_string(),
                            CONSUMER.to_string(),
                            RangeType::Any).unwrap();

    let group = RedisGroup::new(GROUP.to_string(), CONSUMER.to_string());
    let subscribe_options =
        SubscribeOptions::with_group(STREAM.to_string(), group);

    let create_group = RedisStream::connect(&redis_address)
        .and_then(move |con|
            con.touch_group(STREAM.to_string(), GROUP.to_string()))
        // ignore an error if the group exists already
        .then(|_| -> RedisResult<()> { Ok(()) });

    let consumer = create_group
//        .then(|_: RedisResult<()>|
//            // create new connection because the previous future can return an error
//            RedisStream::connect(&redis_address.clone()))
//        .and_then(move |consumer| {
//            let manager = RedisStream::connect(&redis_address);
//            consumer.pending_list(pending_options)
//                .map(|(consumer, response)| {
//                    for pending_message in response.into_iter() {}
//                })
//        });
        .then(move |_| {
            let manager = RedisStream::connect(&redis_address);
            let consumer = RedisStream::connect(&redis_address);
            consumer.join(manager)
        })
        .and_then(move |(consumer, manager)| {
            let (tx, rx) = unbounded::<EntryId>();

            let ack_entry = rx
                .map_err(|err|
                    RedisError::new(RedisErrorKind::InternalError,
                                    "Something went wrong with UnboundedChannel".to_string()))
                .fold(manager, ack_stream_entry)
                .map(|_| ())
                .map_err(|_| ());
            tokio::spawn(ack_entry);

            let process_entry =
                consumer.subscribe(subscribe_options)
                    .and_then(move |subscribe|
                        subscribe.for_each(move |entries|
                            process_stream_entries(tx.clone(), entries)));
            process_entry
        })
        .map_err(|err| println!("Something went wrong: {:?}", err));

    tokio::run(consumer);
}

fn ack_stream_entry(manager: RedisStream, id_to_ack: EntryId)
                    -> impl Future<Item=RedisStream, Error=RedisError> {
    manager.ack_entry(STREAM.to_string(), GROUP.to_string(), id_to_ack.clone())
        .map(move |(manager, response)| {
            match response {
                AckResponse::Ok => println!("{:?} is acknowledged", id_to_ack.to_string()),
                AckResponse::NotExists =>
                    println!("Couldn't acknowledge {:?}", id_to_ack.to_string())
            };
            manager
        })
}

fn process_stream_entries(acknowledger: UnboundedSender<EntryId>, entries: Vec<StreamEntry>)
                          -> RedisResult<()> {
    entries.into_iter()
        .for_each(move |entry| {
            println!("Received {:?}", &entry.id);
            let future = acknowledger.clone()
                .send(entry.id)
                .map(|_| ())
                .map_err(|_| ());
            tokio::spawn(future);
        });
    Ok(())
}

//fn on_pending_messages(messages: Vec<PendingMessage>, address: &SocketAddr) {
//    let requester = RedisStream::connect(address)
//        .and_then(|requester| {
//            let (tx, rx) = mpsc::unbounded();
//        });
//}
//
//fn on_pending_message(requester: RedisStream, message: PendingMessage, messages: &[PendingMessage])
//                      -> impl Future<Item=(), Error= RedisError>{
//    requester.read_explicit()
//}

//fn on_stream_entry()
