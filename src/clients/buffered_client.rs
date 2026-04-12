use std::time::Duration;

use crate::Result;
use crate::clients::Client;

use bytes::Bytes;
use tokio::sync::mpsc::{Receiver, Sender, channel};
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
    // key, member
    Zscore(String, String),
    // key, start, stop, rev, offset, count
    Zrange(String, u64, u64, bool, Option<u64>, Option<u64>),
    // key, member, desc
    Zrank(String, String, bool),
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
    Zscore(Result<Option<usize>>),
    Zrange(Result<Vec<(u64, String)>>),
    Zrank(Result<Option<usize>>),
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
            Command::Del(keys) => Response::Del(client.deletes(keys).await),
            Command::Sadd(key, members) => Response::Sadd(client.sadd(&key, members).await),
            Command::Sismember(key, member) => {
                Response::Sismember(client.sismember(&key, &member).await)
            }
            Command::Slength(key) => Response::Slength(client.slength(&key).await),
            Command::Srem(key, members) => Response::Srem(client.srem(&key, members).await),
            Command::Zadd(key, entries) => Response::Zadd(client.zadd(&key, entries).await),
            Command::Zscore(key, member) => Response::Zscore(client.zscore(&key, &member).await),
            Command::Zrange(key, start, stop, rev, offset, count) => {
                Response::Zrange(client.zrange(&key, start, stop, rev, offset, count).await)
            }
            Command::Zrank(key, member, desc) => {
                Response::Zrank(client.zrank(&key, &member, desc).await)
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
    pub async fn get(&self, key: &str) -> Result<Option<Bytes>> {
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
    /// connection has the ability to send the request.
    pub async fn set(&self, key: &str, value: Bytes) -> Result<()> {
        // Initialize a new `Set` command to send via the channel.
        let set = Command::Set(key.into(), value, None);

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

    /// Set `key` to hold the given `value`.
    ///
    /// Same as `Client::set_expires` but requests are **buffered** until the associated
    /// connection has the ability to send the request.
    pub async fn set_expires(&self, key: &str, value: Bytes, expire: Duration) -> Result<()> {
        // Initialize a new `Set` command to send via the channel.
        let set = Command::Set(key.into(), value, Some(expire));

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

    /// Deletes `keys`.
    ///
    /// Same as `Client::deletes` but requests are **buffered** until the associated
    /// connection has the ability to send the request.
    pub async fn deletes(&self, keys: Vec<String>) -> Result<usize> {
        // Initialize a new `Del` command to send via the channel.
        let del = Command::Del(keys);

        // Initialize a new oneshot to be used to receive the response back from the connection.
        let (tx, rx) = oneshot::channel();

        // Send the request
        self.tx.send((del, tx)).await?;

        // Await the response
        match rx.await {
            Ok(Response::Del(res)) => res,
            Ok(_) => Err("Wrong response type received for Del.".into()),
            Err(err) => Err(err.into()),
        }
    }

    /// Add `members` to `key` associated `S` structure.
    ///
    /// Same as `Client::sadd` but requests are **buffered** until the associated
    /// connection has the ability to send the request.
    pub async fn sadd(&self, key: &str, members: Vec<String>) -> Result<usize> {
        // Initialize a new `Sadd` command to send via the channel.
        let sadd = Command::Sadd(key.into(), members);

        // Initialize a new oneshot to be used to receive the response back from the connection.
        let (tx, rx) = oneshot::channel();

        // Send the request
        self.tx.send((sadd, tx)).await?;

        // Await the response
        match rx.await {
            Ok(Response::Sadd(res)) => res,
            Ok(_) => Err("Wrong response type received for Sadd.".into()),
            Err(err) => Err(err.into()),
        }
    }

    /// Check whether `key` associated `S` structure contains `member`.
    ///
    /// Same as `Client::sismember` but requests are **buffered** until the associated
    /// connection has the ability to send the request.
    pub async fn sismember(&self, key: &str, member: &str) -> Result<usize> {
        // Initialize a new `Sismember` command to send via the channel.
        let sismember = Command::Sismember(key.into(), member.into());

        // Initialize a new oneshot to be used to receive the response back from the connection.
        let (tx, rx) = oneshot::channel();

        // Send the request
        self.tx.send((sismember, tx)).await?;

        // Await the response
        match rx.await {
            Ok(Response::Sismember(res)) => res,
            Ok(_) => Err("Wrong response type received for Sismember.".into()),
            Err(err) => Err(err.into()),
        }
    }

    /// Returns the length of `key` associated `S` structure.
    ///
    /// Same as `Client::slength` but requests are **buffered** until the associated
    /// connection has the ability to send the request.
    pub async fn slength(&self, key: &str) -> Result<usize> {
        // Initialize a new `Slength` command to send via the channel.
        let slength = Command::Slength(key.into());

        // Initialize a new oneshot to be used to receive the response back from the connection.
        let (tx, rx) = oneshot::channel();

        // Send the request
        self.tx.send((slength, tx)).await?;

        // Await the response
        match rx.await {
            Ok(Response::Slength(res)) => res,
            Ok(_) => Err("Wrong response type received for Slength.".into()),
            Err(err) => Err(err.into()),
        }
    }

    /// Removes `members` from `key` associated `S` structure.
    ///
    /// Same as `Client::srem` but requests are **buffered** until the associated
    /// connection has the ability to send the request.
    pub async fn srem(&self, key: &str, members: Vec<String>) -> Result<usize> {
        // Initialize a new `Srem` command to send via the channel.
        let srem = Command::Srem(key.into(), members);

        // Initialize a new oneshot to be used to receive the response back from the connection.
        let (tx, rx) = oneshot::channel();

        // Send the request
        self.tx.send((srem, tx)).await?;

        // Await the response
        match rx.await {
            Ok(Response::Srem(res)) => res,
            Ok(_) => Err("Wrong response type received for Srem.".into()),
            Err(err) => Err(err.into()),
        }
    }

    /// Add `entries` to `key` associated `Z` structure.
    ///
    /// Same as `Client::zadd` but requests are **buffered** until the associated
    /// connection has the ability to send the request.
    pub async fn zadd(&self, key: &str, entries: Vec<(u64, String)>) -> Result<usize> {
        // Initialize a new `Zadd` command to send via the channel.
        let zadd = Command::Zadd(key.into(), entries);

        // Initialize a new oneshot to be used to receive the response back from the connection.
        let (tx, rx) = oneshot::channel();

        // Send the request
        self.tx.send((zadd, tx)).await?;

        // Await the response
        match rx.await {
            Ok(Response::Zadd(res)) => res,
            Ok(_) => Err("Wrong response type received for Zadd.".into()),
            Err(err) => Err(err.into()),
        }
    }

    /// Get the score of `member` from `key` associated `Z` structure.
    pub async fn zscore(&self, key: &str, member: &str) -> Result<Option<usize>> {
        // Initialize a new `Zscore` command to send via the channel.
        let zscore = Command::Zscore(key.into(), member.into());

        // Initialize a new oneshot to be used to receive the response back from the connection.
        let (tx, rx) = oneshot::channel();

        // Send the request
        self.tx.send((zscore, tx)).await?;

        match rx.await {
            Ok(Response::Zscore(res)) => res,
            Ok(_) => Err("Wrong response type received for Zscore.".into()),
            Err(err) => Err(err.into()),
        }
    }

    /// Retrieve the entries (score: u64, member: String) `key` associated values,
    /// satisfying `start` <= `score` <= `stops`.
    /// `rev` will reverse the order of value.
    /// `offset` the number of valid entries to discard.
    /// `count` sets a maximum value to return (all following values are discarded).
    ///
    /// Same as `Client::zrange` but requests are **buffered** until the associated
    /// connection has the ability to send the request.
    pub async fn zrange(
        &self,
        key: &str,
        start: u64,
        stop: u64,
        rev: bool,
        offset: Option<u64>,
        count: Option<u64>,
    ) -> Result<Vec<(u64, String)>> {
        // Initialize a new `Zrange` command to send via the channel.
        let zrange = Command::Zrange(key.into(), start, stop, rev, offset, count);

        // Initialize a new oneshot to be used to receive the response back from the connection.
        let (tx, rx) = oneshot::channel();

        // Send the request
        self.tx.send((zrange, tx)).await?;

        // Await the response
        match rx.await {
            Ok(Response::Zrange(res)) => res,
            Ok(_) => Err("Wrong response type received for Zrange.".into()),
            Err(err) => Err(err.into()),
        }
    }

    /// Returns the rank of the member in the sorted set stored at key.
    /// The rank is 0-based.
    ///
    /// If desc is true, return the rank in decreasing order.
    ///
    /// Same as `Client::zrem` but requests are **buffered** until the associated
    /// connection has the ability to send the request.
    pub async fn zrank(&self, key: &str, member: &str, desc: bool) -> Result<Option<usize>> {
        // Initialize a new `Zrank` command to send via the channel.
        let zrem = Command::Zrank(key.into(), member.into(), desc);

        // Initialize a new oneshot to be used to receive the response back from the connection.
        let (tx, rx) = oneshot::channel();

        // Send the request
        self.tx.send((zrem, tx)).await?;

        // Await the response
        match rx.await {
            Ok(Response::Zrank(res)) => res,
            Ok(_) => Err("Wrong response type received for Zrem.".into()),
            Err(err) => Err(err.into()),
        }
    }

    /// Remove `members` to `key` associated `Z` structure.
    ///
    /// Same as `Client::zrem` but requests are **buffered** until the associated
    /// connection has the ability to send the request.
    pub async fn zrem(&self, key: &str, members: Vec<String>) -> Result<usize> {
        // Initialize a new `Zrem` command to send via the channel.
        let zrem = Command::Zrem(key.into(), members);

        // Initialize a new oneshot to be used to receive the response back from the connection.
        let (tx, rx) = oneshot::channel();

        // Send the request
        self.tx.send((zrem, tx)).await?;

        // Await the response
        match rx.await {
            Ok(Response::Zrem(res)) => res,
            Ok(_) => Err("Wrong response type received for Zrem.".into()),
            Err(err) => Err(err.into()),
        }
    }
}
