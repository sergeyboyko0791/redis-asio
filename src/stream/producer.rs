use super::{AddOptions, EntryId};
use std::net::SocketAddr;
use futures::Future;
use crate::{RedisCoreConnection, RedisError, RedisErrorKind, RedisValue, RedisCommand, ToRedisArgument, command, from_redis_value};

pub struct RedisStreamProducer {
    connection: RedisCoreConnection,
}

impl RedisStreamProducer {
    pub fn connect(addr: &SocketAddr)
                   -> impl Future<Item=RedisStreamProducer, Error=RedisError> + Send + 'static {
        RedisCoreConnection::connect(addr)
            .map(|connection| Self { connection })
    }

    pub fn add<T>(self, options: AddOptions, key_values: Vec<(String, T)>)
                  -> impl Future<Item=(RedisStreamProducer, EntryId), Error=RedisError> + Send + 'static
        where T: ToRedisArgument {
        self.connection.send(add_command(options, key_values))
            .and_then(|(connection, response)| {
                let entry_id_string = from_redis_value(&response)?;
                let entry_id = EntryId::from_string(entry_id_string)?;
                Ok((Self { connection }, entry_id))
            })
    }
}

fn add_command<T>(options: AddOptions, key_values: Vec<(String, T)>) -> RedisCommand
    where T: ToRedisArgument {
    let mut cmd = command("XADD").arg(options.stream);

    match options.entry_id {
        Some(entry_id) => cmd.arg_mut(entry_id.to_string()),
        _ => cmd.arg_mut("*")
    }

    for (key, value) in key_values.into_iter() {
        cmd.arg_mut(key);
        cmd.arg_mut(value);
    }

    cmd
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        let options = AddOptions::new("test_stream".to_string());

        tokio::run(
            RedisStreamProducer::connect(&"127.0.0.1:6379".parse::<SocketAddr>().unwrap())
                .and_then(move |consumer|
                    consumer.add(options, vec![
                        ("key1".to_string(), "value1"),
                        ("key2".to_string(), "value2")])
                )
                .map(|(consumer, entry_id)|
                    println!("Add response: {:?}", entry_id))
                .map_err(|err| println!("On error: {:?}", err))
        );
    }
}
