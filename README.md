# redis-asio

redis-asio is a Redis client library written in pure Rust based on
asynchronous `tokio` library.

The library provides a `base` module for low-level request sending and
response handling, and a `stream` module that contains specific interfaces
for work with Redis-Stream "https://redis.io/topics/streams-intro".

The library works with binary-safe strings that allows users to serialize
their message structures and send via
RESP protocol "https://redis.io/topics/protocol".

# Use in project

Depend on the redis-asio in project via Cargo:

```toml
[dependencies]
redis-async = "0.1"
```

Resolve the crate interfaces:

```rust
extern crate redis_asio;
```

# Motivating examples

## SET, GET value from cache
```rust
use std::net::SocketAddr;
use futures::Future;
use redis_asio::{RedisCoreConnection, RedisValue, from_redis_value};

let address = &"127.0.0.1:6379".parse::<SocketAddr>().unwrap();

let set_req = command("SET").arg("foo").arg(123);
let get_req = command("GET").arg("foo");

let future = RedisCoreConnection::connect(address)
    .and_then(move |con| {
        // send "SET foo 123" request
        con.send(set_req)
    })
    .and_then(|(con, response)| {
        // check if the value has been set
        assert_eq!(RedisValue::Ok, response);
        // send "GET foo" request
        con.send(get_req)
    })
    .map(move |(_, response)|
        // check if the received value is the same
        assert_eq!(123, from_redis_value(&response).unwrap()))
    .map_err(|_| unreachable!());
// start the Tokio runtime using the `future`
tokio::run(future);
```

## Subscribe to a Redis Stream

Subscribe to a Redis stream and process all incoming entries.
Redis Streams requires to send XREAD/XREADGROUP requests every time
the client receives a response on the previous,
in other words Redis Streams does not provide an interface to subscribe
to a Redis stream.

In the Crate the subscription is possible by hidden requests sending
within the Crate engine.

Request that will be sent to get new entries in the following example:
"XREADGROUP GROUP mygroup Bob BLOCK 0 STREAMS mystream <"

```rust
use std::net::SocketAddr;
use futures::{Future, Stream};
use redis_asio::stream::{RedisStream, SubscribeOptions, StreamEntry,
                         RedisGroup};

let address = &"127.0.0.1:6379".parse::<SocketAddr>().unwrap();
// create options to pass it into RedisStream::subscribe()
let group_info = RedisGroup::new("mygroup".to_string(), "Bob".to_string());
let subscribe_options =
    SubscribeOptions::with_group(vec!["mystream".to_string()], group_info);

let future = RedisStream::connect(address)
    .and_then(move |stream: RedisStream| {
        stream.subscribe(subscribe_options)
    })
    .and_then(|subscribe| /*:Subscribe*/ {
        // pass the closure that will be called on each incoming entries
        subscribe.for_each(|entries: Vec<StreamEntry>| {
            for entry in entries.into_iter() {
                println!("Received: {:?}", entry);
            }
            Ok(())
        })
    })
    .map_err(|err| eprintln!("something went wrong: {}", err));
// start the Tokio runtime using the `future`
tokio::run(future);
```

## Send an entry into a Redis Stream

Send an entry into a Redis stream.
Request that will be sent to push a specified entry in the following example:
"XADD mystream * type 3 data \"Hello, world\""

```rust
use std::net::SocketAddr;
use std::collections::HashMap;
use futures::Future;
use redis_asio::{RedisArgument, IntoRedisArgument};
use redis_asio::stream::{RedisStream, SendEntryOptions, EntryId};

let address = &"127.0.0.1:6379".parse::<SocketAddr>().unwrap();
// create options to pass it into RedisStream::send_entry()
let send_options = SendEntryOptions::new("mystream".to_string());

// create key-value pairs
let mut request: HashMap<String, RedisArgument> = HashMap::new();
request.insert("type".to_string(), 3i32.into_redis_argument());
request.insert("data".to_string(), "Hello, world!".into_redis_argument());

let future = RedisStream::connect(address)
    .and_then(move |stream: RedisStream| {
        // HashMap<String, RedisArgument> satisfies the
        // HashMap<String, ToRedisArgument>
        stream.send_entry(send_options, request)
    })
    .map(|(_, inserted_entry_id): (RedisStream, EntryId)| {
        println!("{:?} has sent", inserted_entry_id.to_string());
    })
    .map_err(|err| eprintln!("something went wrong: {}", err));
tokio::run(future);
```
