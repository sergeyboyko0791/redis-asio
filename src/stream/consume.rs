use crate::*;
use super::*;
use futures::{Stream, Future, Sink};
use futures::sync::mpsc::{channel, Sender, Receiver};
use futures::Async;

#[derive(Clone)]
pub struct SubscribeOptions {
    /// Stream name
    /// TODO change to vec
    pub(crate) stream: String,
    /// Optional group info
    pub(crate) group: Option<RedisGroup>,
}

pub struct ReadExplicitOptions {
    /// Stream name
    pub(crate) stream: String,
    /// Optional group info
    pub(crate) group: Option<RedisGroup>,
    /// Max count of entries
    pub(crate) count: u16,
    /// Get entries with ID greater than the start_id
    pub(crate) start_id: EntryId,
}

pub struct RangeOptions {
    /// Stream name
    pub(crate) stream: String,
    /// Max count of entries
    pub(crate) count: u16,
    /// Get entries with ID in the range
    pub(crate) range: RangeType,
}

#[derive(Clone)]
pub struct RedisGroup {
    /// Group name
    pub(crate) group: String,
    /// Consumer name
    pub(crate) consumer: String,
}

pub struct Subscribe {
    pub(crate) stream: Box<dyn Stream<Item=RedisValue, Error=RedisError> + Send + 'static>,
}

impl Stream for Subscribe {
    type Item = Vec<StreamEntry>;
    type Error = RedisError;

    fn poll(&mut self) -> Result<Async<Option<Self::Item>>, Self::Error> {
        self.stream.poll()
            .and_then(|value| {
                let value = match value {
                    Async::Ready(x) => x,
                    _ => return Ok(Async::NotReady)
                };
                let value = match value {
                    Some(x) => x,
                    _ => return Ok(Async::Ready(None)),
                };

                parse_stream_entries(value)
                    .map(|stream_entries| Async::Ready(Some(stream_entries)))
            })
    }
}

impl SubscribeOptions {
    pub fn new(stream: String) -> SubscribeOptions {
        let group: Option<RedisGroup> = None;
        SubscribeOptions { stream, group }
    }

    pub fn with_group(stream: String, group: RedisGroup) -> SubscribeOptions {
        let group = Some(group);
        SubscribeOptions { stream, group }
    }
}

impl ReadExplicitOptions {
    pub fn new(stream: String, count: u16, start_id: EntryId) -> ReadExplicitOptions {
        let group: Option<RedisGroup> = None;
        ReadExplicitOptions { stream, group, count, start_id }
    }

    pub fn with_group(stream: String, group: RedisGroup, count: u16, start_id: EntryId)
                      -> ReadExplicitOptions {
        let group = Some(group);
        ReadExplicitOptions { stream, group, count, start_id }
    }
}

impl RangeOptions {
    pub fn new(stream: String, count: u16, range: RangeType) -> RedisResult<RangeOptions> {
        let group: Option<RedisGroup> = None;
        if !range.is_valid() {
            return Err(
                RedisError::new(RedisErrorKind::InvalidOptions,
                                format!("Left bound should be less than right bound")));
        }

        Ok(RangeOptions { stream, count, range })
    }
}

impl RedisGroup {
    pub fn new(group: String, consumer: String) -> RedisGroup {
        RedisGroup { group, consumer }
    }
}

enum StreamInternalCommand {
    ListenNextMessage,
}

pub(crate) fn subscribe<F, T>(from_srv: F, to_srv: T, options: SubscribeOptions)
                              -> impl Stream<Item=RedisValue, Error=RedisError> + Send + 'static
    where F: Stream<Item=RespInternalValue, Error=RedisError> + Send + 'static,
          T: Sink<SinkItem=RedisCommand, SinkError=RedisError> + Send + 'static {
    // Redis Streams protocol is a simple request-response protocol,
    // and we should not receive more than one packet before the rx Receiver<StreamInternalCommand>
    const BUFFER_SIZE: usize = 1;
    let (tx, rx) =
        channel::<StreamInternalCommand>(BUFFER_SIZE);

    let output = fwd_from_channel_to_srv(to_srv, rx, options);
    let input
        = process_from_srv_and_notify_channel(from_srv, tx);

    // We have the following conditions:
    // 1) a return stream should include both output future and input stream
    // 2) select() method requires equal types of Item within two merging streams,
    // 3) a return stream should has Item = RedisValue
    // 4) output future should not influence a return stream
    //
    // change Item to Option<RedisValue> within the input stream and output future
    // where output future will not influence a selected stream (via filter_map())

    let output = output.map(|_| None);
    let input = input.map(|x| Some(x));

    input.select(output.into_stream()).filter_map(|x| x)
}

pub(crate) fn subscribe_cmd(options: SubscribeOptions) -> RedisCommand
{
    let SubscribeOptions { stream, group } = options;

    // receive only new messages (specifier is different for XREAD and XREADGROUP)
    let id_specifier = match &group {
        Some(_) => ">",
        _ => "$"
    };

    xread_cmd_from(group)
        .arg("BLOCK")
        .arg("0") // block until next pkt
        .arg("STREAMS")
        .arg(stream.as_str())
        .arg(id_specifier)
}

pub(crate) fn read_explicit_cmd(options: ReadExplicitOptions) -> RedisCommand
{
    let ReadExplicitOptions { stream, group, count, start_id } = options;
    xread_cmd_from(group)
        .arg("COUNT")
        .arg(count as i64)
        .arg("STREAMS")
        .arg(stream.as_str())
        .arg(start_id.to_string())
}

pub(crate) fn range_cmd(options: RangeOptions) -> RedisCommand
{
    let RangeOptions { stream, count, range } = options;

    let (left, right) = range.to_left_right();

    command("XRANGE")
        .arg(stream)
        .arg(left)
        .arg(right)
        .arg("COUNT")
        .arg(count as i64)
}

fn fwd_from_channel_to_srv<T>(to_srv: T,
                              rx: Receiver<StreamInternalCommand>,
                              options: SubscribeOptions)
                              -> impl Future<Item=(), Error=RedisError> + Send + 'static
    where T: Sink<SinkItem=RedisCommand, SinkError=RedisError> + Send + 'static {
    rx
        .map_err(|err| RedisError::new(RedisErrorKind::InternalError,
                                       "Cannot read from internal channel".into()))
        .fold(to_srv, move |to_srv, msg| {
            match msg {
                StreamInternalCommand::ListenNextMessage =>
                    to_srv.send(subscribe_cmd(options.clone()))
            }
        })
        .map(|_| ())
}

fn process_from_srv_and_notify_channel<F>(from_srv: F,
                                          tx: Sender<StreamInternalCommand>)
                                          -> impl Stream<Item=RedisValue, Error=RedisError> + Send + 'static
    where F: Stream<Item=RespInternalValue, Error=RedisError> + Send + 'static
{
    from_srv
        .and_then(move |msg| {
            tx.clone().send(StreamInternalCommand::ListenNextMessage)
                .then(|res| {
                    match res {
                        Ok(_) => (),
                        Err(err) =>
                            return Err(RedisError::new(RedisErrorKind::ConnectionError,
                                                       format!("Could not send listen request: {:?}", err)))
                    }
                    // convert RespInternalValue to RedisValue
                    // note: the function returns an error if the Resp value is Error
                    //       else returns RedisValue
                    RedisValue::from_resp_value(msg)
                })
        })
}

fn xread_cmd_from(group: Option<RedisGroup>) -> RedisCommand {
    let cmd =
        match &group {
            Some(_) => "XREADGROUP",
            _ => "XREAD",
        };

    let mut cmd = command(cmd);
    match group {
        Some(RedisGroup { group, consumer }) =>
            cmd.arg("GROUP")
                .arg(group.as_str())
                .arg(consumer.as_str()),
        _ => cmd
    }
}
