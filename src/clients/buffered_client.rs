use std::time::Duration;

use crate::clients::Client;
use crate::Result;

use bytes::Bytes;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;

// Enum used to message pass the requested command from the `BufferedClient` handle
#[derive(Debug)]
enum Command {
    // key
    Get(String),
    // key, value, expire
    Set(String, Bytes, Option<Duration>),
    // keys
    Del(Vec<String>),
    // key, members
    Sadd(String, Vec<String>),
    // key, member
    Sismember(String, String),
    // key
    Slength(String),
    // key, members
    Srem(String, Vec<String>),
    // key, (score, member)
    Zadd(String, Vec<(u64, String)>),
    // key, start, stop, rev, offset, count
    Zrange(String, u64, u64, bool, Option<u64>, Option<u64>),
    // key, members
    Zrem(String, Vec<String>),
}

#[derive(Debug)]
enum Response {
    Get(Result<Option<Bytes>>),
    Set(Result<()>),
    Del(Result<usize>),
    Sadd(Result<usize>),
    Sismember(Result<usize>),
    Slength(Result<usize>),
    Srem(Result<usize>),
    Zadd(Result<usize>),
    Zrange(Result<Vec<(u64, String)>>),
    Zrem(Result<usize>),
}

// Message type sent over the channel to the connection task.
//
// `Command` is the command to forward to the connection.
//
// `oneshot::Sender` is a channel type that sends a **single** value. It is used
// here to send the response received from the connection back to the original
// requester.
type Message = (Command, oneshot::Sender<Response>);

/// Receive commands sent through the channel and forward them to client. The
/// response is returned back to the caller via a `oneshot`.
async fn run(mut client: Client, mut rx: Receiver<Message>) {
    // Repeatedly pop messages from the channel. A return value of `None`
    // indicates that all `BufferedClient` handles have dropped and there will never be
    // another message sent on the channel.
    while let Some((cmd, tx)) = rx.recv().await {
        // The command is forwarded to the connection
        let response = match cmd {
            Command::Get(key) => Response::Get(client.get(&key).await),
            Command::Set(key, value, expiration) => {
                if let Some(d) = expiration {
                    Response::Set(client.set_expires(&key, value, d).await)
                } else {
                    Response::Set(client.set(&key, value).await)
                }
            }
            Command::Del(keys) => Response::Del(client.deletes(&keys).await),
            Command::Sadd(key, members) => Response::Sadd(client.sadd(&key, members).await),
            Command::Sismember(key, member) => {
                Response::Sismember(client.sismember(&key, &member).await)
            }
            Command::Slength(key) => Response::Slength(client.slength(&key).await),
            Command::Srem(key, members) => Response::Srem(client.srem(&key, members).await),
            Command::Zadd(key, entries) => Response::Zadd(client.zadd(&key, entries).await),
            Command::Zrange(key, start, stop, rev, offset, count) => {
                Response::Zrange(client.zrange(&key, start, stop, rev, offset, count).await)
            }
            Command::Zrem(key, members) => Response::Zrem(client.zrem(&key, members).await),
        };

        // Send the response back to the caller.
        //
        // Failing to send the message indicates the `rx` half dropped
        // before receiving the message. This is a normal runtime event.
        let _ = tx.send(response);
    }
}

#[derive(Clone)]
pub struct BufferedClient {
    tx: Sender<Message>,
}

impl BufferedClient {
    /// Create a new client request buffer
    ///
    /// The `Client` performs Redis commands directly on the TCP connection. Only a
    /// single request may be in-flight at a given time and operations require
    /// mutable access to the `Client` handle. This prevents using a single Redis
    /// connection from multiple Tokio tasks.
    ///
    /// The strategy for dealing with this class of problem is to spawn a dedicated
    /// Tokio task to manage the Redis connection and using "message passing" to
    /// operate on the connection. Commands are pushed into a channel. The
    /// connection task pops commands off of the channel and applies them to the
    /// Redis connection. When the response is received, it is forwarded to the
    /// original requester.
    ///
    /// The returned `BufferedClient` handle may be cloned before passing the new handle to
    /// separate tasks.
    pub fn buffer(client: Client) -> BufferedClient {
        // Setting the message limit to a hard coded value of 32. in a real-app, the
        // buffer size should be configurable, but we don't need to do that here.
        let (tx, rx) = channel(32);

        // Spawn a task to process requests for the connection.
        tokio::spawn(async move { run(client, rx).await });

        // Return the `BufferedClient` handle.
        BufferedClient { tx }
    }

    /// Get the value of a key.
    ///
    /// Same as `Client::get` but requests are **buffered** until the associated
    /// connection has the ability to send the request.
    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>> {
        // Initialize a new `Get` command to send via the channel.
        let get = Command::Get(key.into());

        // Initialize a new oneshot to be used to receive the response back from the connection.
        let (tx, rx) = oneshot::channel();

        // Send the request
        self.tx.send((get, tx)).await?;

        // Await the response
        match rx.await {
            Ok(Response::Get(res)) => res,
            Ok(_) => Err("Wrong response type received for GET.".into()),
            Err(err) => Err(err.into()),
        }
    }

    /// Set `key` to hold the given `value`.
    ///
    /// Same as `Client::set` but requests are **buffered** until the associated
    /// connection has the ability to send the request
    pub async fn set(
        &mut self,
        key: &str,
        value: Bytes,
        expiration: Option<Duration>,
    ) -> Result<()> {
        // Initialize a new `Set` command to send via the channel.
        let set = Command::Set(key.into(), value, expiration);

        // Initialize a new oneshot to be used to receive the response back from the connection.
        let (tx, rx) = oneshot::channel();

        // Send the request
        self.tx.send((set, tx)).await?;

        // Await the response
        match rx.await {
            Ok(Response::Set(res)) => res,
            Ok(_) => Err("Wrong response type received for SET.".into()),
            Err(err) => Err(err.into()),
        }
    }
}
