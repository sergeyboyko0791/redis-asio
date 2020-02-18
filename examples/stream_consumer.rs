extern crate tokio;
extern crate futures;

use std::env;
use std::net::SocketAddr;
use futures::{Future, Stream, Sink};
use futures::future::join_all;
use futures::sync::mpsc::{UnboundedSender, UnboundedReceiver, unbounded};

use redis_asio::{RedisCommand, FromRedisValue, RedisCoreConnection, from_redis_value, command,
                 RedisResult, RedisError, RedisErrorKind};
use redis_asio::stream::{RedisStream, StreamEntry, PendingOptions, RangeType, PendingMessage, EntryId, AckResponse, SubscribeOptions, RedisGroup, TouchGroupOptions, AckOptions};

fn main() {
    let redis_address = env::var("REDIS_URL")
        .unwrap_or("127.0.0.1:6379".to_string())
        .parse::<SocketAddr>().expect("Couldn't parse Redis URl");

    let stream_name: String = "ConsumerTest".to_string();
    let group_name: String = "MyGroup".to_string();
    let consumer_name: String = "CustomConsumer".to_string();

    let touch_options = TouchGroupOptions::new(stream_name.clone(), group_name.clone());

    let create_group = RedisStream::connect(&redis_address)
        .and_then(move |con|
            // create group if the one does not exists yet
            con.touch_group(touch_options)
        )
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
        .and_then(move |(connection, manager)| {
            let (tx, rx) = unbounded::<EntryId>();

            // copy of stream_name and group_name to move it into ack_entry future
            let stream = stream_name.clone();
            let group = group_name.clone();

            let ack_entry = rx
                .map_err(|err|
                    RedisError::new(RedisErrorKind::InternalError,
                                    "Something went wrong with UnboundedChannel".to_string()))
                .fold(manager, move |manager, id_to_ack|
                    ack_stream_entry(manager, stream.clone(), group.clone(), id_to_ack))
                .map(|_| ())
                .map_err(|_| ());
            tokio::spawn(ack_entry);

            let group = RedisGroup::new(group_name, consumer_name);
            let options = SubscribeOptions::with_group(stream_name, group);

            let process_entry =
                connection.subscribe(options)
                    .and_then(move |subscribe|
                        subscribe.for_each(move |entries|
                            process_stream_entries(tx.clone(), entries)));
            process_entry
        })
        .map_err(|err| println!("Something went wrong: {:?}", err));

    tokio::run(consumer);
}

fn ack_stream_entry(manager: RedisStream, stream: String, group: String, id_to_ack: EntryId)
                    -> impl Future<Item=RedisStream, Error=RedisError> {
    let options = AckOptions::new(stream.clone(), group.clone(), id_to_ack.clone());

    manager.ack_entry(options)
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
