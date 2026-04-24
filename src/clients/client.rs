//! Minimal Redis client implementation
//!
//! Provides an async connect and methods for issuing the supported commands.

use crate::cmd::{
    Del, Get, Len, Ping, Publish, Sadd, Set, Sismember, Slength, Srem, Subscribe, Unsubscribe,
    Zadd, Zlength, Zrange, Zrank, Zrem, Zscore,
};
use crate::{Connection, Frame};

use async_stream::try_stream;
use bytes::Bytes;
use std::io::{Error, ErrorKind};
use std::time::Duration;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_stream::Stream;
use tracing::{debug, instrument};

/// Established connection with a Redis server.
///
/// Backed by a single `TcpStream`, `Client` provides basic network client
/// functionality (no pooling, retrying, ...). Connections are established using
/// the [`connect`](fn@connect) function.
///
/// Requests are issued using the various methods of `Client`.
pub struct Client {
    /// The TCP connection decorated with the redis protocol encoder / decoder
    /// implemented using a buffered `TcpStream`.
    ///
    /// When `Listener` receives an inbound connection, the `TcpStream` is
    /// passed to `Connection::new`, which initializes the associated buffers.
    /// `Connection` allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated in `Connection`.
    connection: Connection,
}

/// A client that has entered pub/sub mode.
///
/// Once clients subscribe to a channel, they may only perform pub/sub related
/// commands. The `Client` type is transitioned to a `Subscriber` type in order
/// to prevent non-pub/sub methods from being called.
pub struct Subscriber {
    /// The subscribed client.
    client: Client,

    /// The set of channels to which the `Subscriber` is currently subscribed.
    subscribed_channels: Vec<String>,
}

/// A message received on a subscribed channel.
#[derive(Debug, Clone)]
pub struct Message {
    pub channel: String,
    pub content: Bytes,
}

impl Client {
    /// Establish a connection with the Redis server located at `addr`.
    ///
    /// `addr` may be any type that can be asynchronously converted to a
    /// `SocketAddr`. This includes `SocketAddr` and strings. The `ToSocketAddrs`
    /// trait is the Tokio version and not the `std` version.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use miniredis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let client = match Client::connect("localhost:6379").await {
    ///         Ok(client) => client,
    ///         Err(_) => panic!("failed to establish connection"),
    ///     };
    /// # drop(client);
    /// }
    /// ```
    ///
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Client> {
        // The `addr` argument is passed directly to `TcpStream::connect`. This
        // performs any asynchronous DNS lookup and attempts to establish the TCP
        // connection. An error at either step returns an error, which is then
        // bubbled up to the caller of `miniredis` connect.
        let socket = TcpStream::connect(addr).await?;

        // Initialize the connection state. This allocates read/write buffers to
        // perform redis protocol frame parsing.
        let connection = Connection::new(socket);

        Ok(Client { connection })
    }

    /// Ping to the server.
    ///
    /// Returns PONG if no argument is provided, otherwise
    /// return a copy of the argument as a bulk.
    ///
    /// This command is often used to test if a connection
    /// is still alive, or to measure latency.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    /// ```no_run
    /// use miniredis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let pong = client.ping(None).await.unwrap();
    ///     assert_eq!(b"PONG", &pong[..]);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn ping(&mut self, msg: Option<Bytes>) -> crate::Result<Bytes> {
        let frame = Ping::new(msg).into_frame();
        debug!(request = ?frame);
        let resp_frame = frame.encode_resp()?;
        self.connection.write_frame(resp_frame).await?;

        match self.read_response().await? {
            Frame::Simple(value) => Ok(value.into()),
            Frame::Bulk(value) => Ok(value),
            frame => Err(frame.to_error()),
        }
    }

    /// Get the value of key.
    ///
    /// If the key does not exist the special value `None` is returned.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use miniredis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let val = client.get("foo").await.unwrap();
    ///     println!("Got = {:?}", val);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        // Create a `Get` command for the `key` and convert it to a frame.
        let frame = Get::new(key).into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        let resp_frame = frame.encode_resp()?;
        self.connection.write_frame(resp_frame).await?;

        // Wait for the response from the server
        //
        // Both `Simple` and `Bulk` frames are accepted. `Null` represents the
        // key not being present and `None` is returned.
        match self.read_response().await? {
            Frame::Simple(value) => Ok(Some(value.into())),
            Frame::Bulk(value) => Ok(Some(value)),
            Frame::Null => Ok(None),
            frame => Err(frame.to_error()),
        }
    }

    /// Set `key` to hold the given `value`.
    ///
    /// The `value` is associated with `key` until it is overwritten by the next
    /// call to `set` or it is removed.
    ///
    /// If key already holds a value, it is overwritten. Any previous time to
    /// live associated with the key is discarded on successful SET operation.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use miniredis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     client.set("foo", "bar".into()).await.unwrap();
    ///
    ///     // Getting the value immediately works
    ///     let val = client.get("foo").await.unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()> {
        // Create a `Set` command and pass it to `set_cmd`. A separate method is
        // used to set a value with an expiration. The common parts of both
        // functions are implemented by `set_cmd`.
        self.set_cmd(Set::new(key, value, None)).await
    }

    /// Set `key` to hold the given `value`. The value expires after `expiration`
    ///
    /// The `value` is associated with `key` until one of the following:
    /// - it expires.
    /// - it is overwritten by the next call to `set`.
    /// - it is removed.
    ///
    /// If key already holds a value, it is overwritten. Any previous time to
    /// live associated with the key is discarded on a successful SET operation.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage. This example is not **guaranteed** to always
    /// work as it relies on time based logic and assumes the client and server
    /// stay relatively synchronized in time. The real world tends to not be so
    /// favorable.
    ///
    /// ```no_run
    /// use miniredis::clients::Client;
    /// use tokio::time;
    /// use std::time::Duration;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let ttl = Duration::from_millis(500);
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     client.set_expires("foo", "bar".into(), ttl).await.unwrap();
    ///
    ///     // Getting the value immediately works
    ///     let val = client.get("foo").await.unwrap().unwrap();
    ///     assert_eq!(val, "bar");
    ///
    ///     // Wait for the TTL to expire
    ///     time::sleep(ttl).await;
    ///
    ///     let val = client.get("foo").await.unwrap();
    ///     assert!(val.is_some());
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn set_expires(
        &mut self,
        key: &str,
        value: Bytes,
        expiration: Duration,
    ) -> crate::Result<()> {
        // Create a `Set` command and pass it to `set_cmd`. A separate method is
        // used to set a value with an expiration. The common parts of both
        // functions are implemented by `set_cmd`.
        self.set_cmd(Set::new(key, value, Some(expiration))).await
    }

    /// The core `SET` logic, used by both `set` and `set_expires.
    async fn set_cmd(&mut self, cmd: Set) -> crate::Result<()> {
        // Convert the `Set` command into a frame
        let frame = cmd.into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        let resp_frame = frame.encode_resp()?;
        self.connection.write_frame(resp_frame).await?;

        // Wait for the response from the server. On success, the server
        // responds simply with `OK`. Any other response indicates an error.
        match self.read_response().await? {
            Frame::Simple(response) if response == "OK" => Ok(()),
            frame => Err(frame.to_error()),
        }
    }

    /// Delete the key-value pair(s).
    ///
    /// Return the number of key that were removed.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use miniredis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let to_remove = vec![String::from("foo1"), String::from("foo2")];
    ///
    ///     let removed = client.del(to_remove).await.unwrap();
    ///     println!("Number of key removed: {:?}", removed);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn del(&mut self, keys: Vec<String>) -> crate::Result<usize> {
        let frame = Del::new(keys).into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        let resp_frame = frame.encode_resp()?;
        self.connection.write_frame(resp_frame).await?;

        // Wait for the response from the server
        //
        // Only Integer frame is accepted, telling how many keys were removed
        match self.read_response().await? {
            Frame::Integer(removed) => Ok(removed as usize),
            frame => Err(frame.to_error()),
        }
    }

    /// Delete the key-value pair(s).
    ///
    /// Return the number of key that were removed.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use miniredis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     client.set("foo1", "bar1".into()).await.unwrap();
    ///     client.set("foo2", "bar2".into()).await.unwrap();
    ///
    ///     let len = client.len().await.unwrap();
    ///     println!("Length: {:?}", len);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn len(&mut self) -> crate::Result<usize> {
        let frame = Len.into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        let resp_frame = frame.encode_resp()?;
        self.connection.write_frame(resp_frame).await?;

        // Wait for the response from the server
        //
        // Only Integer frame is accepted, telling the length of get/set underlying struct.
        match self.read_response().await? {
            Frame::Integer(len) => Ok(len as usize),
            frame => Err(frame.to_error()),
        }
    }

    /// Add member to the set associated key.
    ///
    /// Return the number of newly added members.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use miniredis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let key = "key_test";
    ///     let members = vec![String::from("player1"), String::from("player2")];
    ///
    ///     let added = client.sadd(key, members).await.unwrap();
    ///     println!("Number of members added: {:?}", added);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn sadd(&mut self, key: &str, members: Vec<String>) -> crate::Result<usize> {
        let frame = Sadd::new(key.to_string(), members).into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        let resp_frame = frame.encode_resp()?;
        self.connection.write_frame(resp_frame).await?;

        // Wait for the response from the server
        //
        // Only Integer frame is accepted, telling how many keys were added
        match self.read_response().await? {
            Frame::Integer(added) => Ok(added as usize),
            frame => Err(frame.to_error()),
        }
    }

    /// Returns the length of `S` set associated key.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use miniredis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let key = "key_test";
    ///     let members = vec![String::from("player1"), String::from("player2")];
    ///     let added = client.sadd(key, members).await.unwrap();
    ///
    ///     let length = client.slength(key).await.unwrap();
    ///     println!("Length: {:?}", length);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn slength(&mut self, key: &str) -> crate::Result<usize> {
        let frame = Slength::new(key.to_string()).into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        let resp_frame = frame.encode_resp()?;
        self.connection.write_frame(resp_frame).await?;

        // Wait for the response from the server
        //
        // Integer frame only, telling the length if the set if present, otherwise 0
        match self.read_response().await? {
            Frame::Integer(length) => Ok(length as usize),
            frame => Err(frame.to_error()),
        }
    }

    /// Returns 1 if member is present, 0 otherwise.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use miniredis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let key = "key_test";
    ///     let member = "player1";
    ///
    ///     let is_member = client.sismember(key, member).await.unwrap();
    ///     println!("is member: {:?}", is_member);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn sismember(&mut self, key: &str, member: &str) -> crate::Result<usize> {
        let frame = Sismember::new(key.to_string(), member.to_string()).into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        let resp_frame = frame.encode_resp()?;
        self.connection.write_frame(resp_frame).await?;

        // Wait for the response from the server
        //
        // Only Integer frame is accepted, 0 if not present and 1 if present
        match self.read_response().await? {
            Frame::Integer(is_member) => Ok(is_member as usize),
            frame => Err(frame.to_error()),
        }
    }

    /// Remove members to the set associated key.
    ///
    /// Return the number of actually removed members.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use miniredis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let key = "key_test";
    ///     let members = vec![String::from("player1"), String::from("player2")];
    ///
    ///     let removed = client.srem(key, members).await.unwrap();
    ///     println!("Number of removed members: {:?}", removed);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn srem(&mut self, key: &str, members: Vec<String>) -> crate::Result<usize> {
        let frame = Srem::new(key.to_string(), members).into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        let resp_frame = frame.encode_resp()?;
        self.connection.write_frame(resp_frame).await?;

        // Wait for the response from the server
        //
        // Only Integer frame is accepted, telling how many keys were removed
        match self.read_response().await? {
            Frame::Integer(removed) => Ok(removed as usize),
            frame => Err(frame.to_error()),
        }
    }

    /// Add entries (score-member) to the sorted set associated key.
    /// If member is already present, its score is updated.
    ///
    /// Return the number of key that were added.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use miniredis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let key = "key_test";
    ///     let entries = vec![(10, String::from("player1")), (5, String::from("player2"))];
    ///
    ///     let added = client.zadd(key, entries).await.unwrap();
    ///     println!("Number of entries added: {:?}", added);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn zadd(&mut self, key: &str, entries: Vec<(u64, String)>) -> crate::Result<usize> {
        let frame = Zadd::new(key.to_string(), entries).into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        let resp_frame = frame.encode_resp()?;
        self.connection.write_frame(resp_frame).await?;

        // Wait for the response from the server
        //
        // Only Integer frame is accepted, telling how many keys were added
        match self.read_response().await? {
            Frame::Integer(added) => Ok(added as usize),
            frame => Err(frame.to_error()),
        }
    }

    /// Returns the length of `Z` associated key struct.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use miniredis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let key = "key_test";
    ///
    ///     let entries = vec![(10, String::from("player1")), (5, String::from("player2"))];
    ///     let added = client.zadd(key, entries).await.unwrap();
    ///
    ///     let length = client.zlength(key).await.unwrap();
    ///     println!("Length: {:?}", length);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn zlength(&mut self, key: &str) -> crate::Result<usize> {
        let frame = Zlength::new(key.to_string()).into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        let resp_frame = frame.encode_resp()?;
        self.connection.write_frame(resp_frame).await?;

        // Wait for the response from the server
        //
        // Integer frame only, telling the length of the set if present, otherwise 0
        match self.read_response().await? {
            Frame::Integer(length) => Ok(length as usize),
            frame => Err(frame.to_error()),
        }
    }

    /// Returns the score of member associated key.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use miniredis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let key = "key_test";
    ///     let member = "player1";
    ///
    ///     let score = client.zscore(key, member).await.unwrap();
    ///     match score {
    ///        Some(s) => println!("Score: {:?}", s),
    ///        None => println!("member does not exist"),
    ///     }
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn zscore(&mut self, key: &str, member: &str) -> crate::Result<Option<usize>> {
        let frame = Zscore::new(key.to_string(), member.to_string()).into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        let resp_frame = frame.encode_resp()?;
        self.connection.write_frame(resp_frame).await?;

        // Wait for the response from the server
        //
        // Integer and Null frame are accepted, telling the score if it is present
        match self.read_response().await? {
            Frame::Integer(score) => Ok(Some(score as usize)),
            Frame::Null => Ok(None),
            frame => Err(frame.to_error()),
        }
    }

    /// Get the range (score, member) where start >= score >= stop.
    ///
    /// Return the number of key that were added.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use miniredis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let key = "key_test";
    ///     let entries = vec![(10, String::from("player1")), (5, String::from("player2"))];
    ///
    ///     let added = client.zadd(key, entries).await.unwrap();
    ///     println!("Number of entries added: {:?}", added);
    ///
    ///     let entries = client.zrange(key, 0, 5, false, None, None).await.unwrap();
    ///     // Member: "player2", with score = 5
    ///     for (score, member) in entries.into_iter() {
    ///          println!("Member: \"{:?}\", with score = {:?}", score, member);
    ///     }
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn zrange(
        &mut self,
        key: &str,
        start: u64,
        stop: u64,
        rev: bool,
        offset: Option<u64>,
        count: Option<u64>,
    ) -> crate::Result<Vec<(u64, String)>> {
        let frame = Zrange::new(key.to_string(), start, stop, rev, offset, count).into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        let resp_frame = frame.encode_resp()?;
        self.connection.write_frame(resp_frame).await?;

        // Wait for the response from the server
        //
        // Expects an even number of frames.
        // Each entry is represented in 2 consecutives frames
        match self.read_response().await? {
            Frame::Array(frames) => {
                let mut response = Vec::new();

                for chunk in frames.chunks(2) {
                    match chunk {
                        [Frame::Integer(score), Frame::Bulk(member)] => {
                            let member = std::str::from_utf8(member)
                                .map_err(|_| "invalid utf-8")?
                                .to_string();

                            response.push((*score, member));
                        }
                        _ => return Err("invalid ZRANGE response format".into()),
                    }
                }

                Ok(response)
            }
            frame => Err(frame.to_error()),
        }
    }

    /// Returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
    /// The rank (or index) is 0-based, which means that the member with the lowest score has rank 0.
    ///
    /// The optional WITHSCORE argument supplements the command's reply with the score of the element returned.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use miniredis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let key = "key_test";
    ///     let members = vec![String::from("player1"), String::from("player2")];
    ///
    ///     let removed = client.zrem(key, members).await.unwrap();
    ///     println!("Number of removed members: {:?}", removed);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn zrem(&mut self, key: &str, members: Vec<String>) -> crate::Result<usize> {
        let frame = Zrem::new(key.to_string(), members).into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        let resp_frame = frame.encode_resp()?;
        self.connection.write_frame(resp_frame).await?;

        // Wait for the response from the server
        //
        // Only Integer frame is accepted, telling how many keys were removed
        match self.read_response().await? {
            Frame::Integer(removed) => Ok(removed as usize),
            frame => Err(frame.to_error()),
        }
    }

    /// Remove members to the sorted set associated key.
    ///
    /// Return the number of actually removed members.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use miniredis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let key = "key_test";
    ///     let entries = vec![(10, String::from("player1")), (5, String::from("player2"))];
    ///
    ///     let added = client.zadd(key, entries).await.unwrap();
    ///     let rank = client.zrank(key, "player1", true).await.unwrap();
    ///
    ///     // Rank of member: 0
    ///     println!("Rank of member: {:?}", rank);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn zrank(
        &mut self,
        key: &str,
        member: &str,
        desc: bool,
    ) -> crate::Result<Option<usize>> {
        let frame = Zrank::new(key.to_string(), member.to_string(), desc).into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket. This writes the full frame to the
        // socket, waiting if necessary.
        let resp_frame = frame.encode_resp()?;
        self.connection.write_frame(resp_frame).await?;

        // Wait for the response from the server
        //
        // Integer, and Null frames are accepted, telling the rank if member is present
        match self.read_response().await? {
            Frame::Integer(rank) => Ok(Some(rank as usize)),
            Frame::Null => Ok(None),
            frame => Err(frame.to_error()),
        }
    }

    /// Posts `message` to the given `channel`.
    ///
    /// Returns the number of subscribers currently listening on the channel.
    /// There is no guarantee that these subscribers receive the message as they
    /// may disconnect at any time.
    ///
    /// # Examples
    ///
    /// Demonstrates basic usage.
    ///
    /// ```no_run
    /// use miniredis::clients::Client;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut client = Client::connect("localhost:6379").await.unwrap();
    ///
    ///     let val = client.publish("foo", "bar".into()).await.unwrap();
    ///     println!("Got = {:?}", val);
    /// }
    /// ```
    #[instrument(skip(self))]
    pub async fn publish(&mut self, channel: &str, message: Bytes) -> crate::Result<u64> {
        // Convert the `Publish` command into a frame
        let frame = Publish::new(channel, message).into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket
        let resp_frame = frame.encode_resp()?;
        self.connection.write_frame(resp_frame).await?;

        // Read the response
        match self.read_response().await? {
            Frame::Integer(response) => Ok(response),
            frame => Err(frame.to_error()),
        }
    }

    /// Subscribes the client to the specified channels.
    ///
    /// Once a client issues a subscribe command, it may no longer issue any
    /// non-pub/sub commands. The function consumes `self` and returns a `Subscriber`.
    ///
    /// The `Subscriber` value is used to receive messages as well as manage the
    /// list of channels the client is subscribed to.
    #[instrument(skip(self))]
    pub async fn subscribe(mut self, channels: Vec<String>) -> crate::Result<Subscriber> {
        // Issue the subscribe command to the server and wait for confirmation.
        // The client will then have been transitioned into the "subscriber"
        // state and may only issue pub/sub commands from that point on.
        self.subscribe_cmd(&channels).await?;

        // Return the `Subscriber` type
        Ok(Subscriber {
            client: self,
            subscribed_channels: channels,
        })
    }

    /// The core `SUBSCRIBE` logic, used by misc subscribe fns
    async fn subscribe_cmd(&mut self, channels: &[String]) -> crate::Result<()> {
        // Convert the `Subscribe` command into a frame
        let frame = Subscribe::new(channels.to_vec()).into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket
        let resp_frame = frame.encode_resp()?;
        self.connection.write_frame(resp_frame).await?;

        // For each channel being subscribed to, the server responds with a
        // message confirming subscription to that channel.
        for channel in channels {
            // Read the response
            let response = self.read_response().await?;

            // Verify it is confirmation of subscription.
            match response {
                Frame::Array(ref frame) => match frame.as_slice() {
                    // The server responds with an array frame in the form of:
                    //
                    // ```
                    // [ "subscribe", channel, num-subscribed ]
                    // ```
                    //
                    // where channel is the name of the channel and
                    // num-subscribed is the number of channels that the client
                    // is currently subscribed to.
                    [subscribe, schannel, ..]
                        if *subscribe == "subscribe" && *schannel == channel => {}
                    _ => return Err(response.to_error()),
                },
                frame => return Err(frame.to_error()),
            };
        }

        Ok(())
    }

    /// Reads a response frame from the socket.
    ///
    /// If an `Error` frame is received, it is converted to `Err`.
    async fn read_response(&mut self) -> crate::Result<Frame> {
        let response = self.connection.read_frame().await?;

        debug!(?response);

        match response {
            // Error frames are converted to `Err`
            Some(Frame::Error(msg)) => Err(msg.into()),
            Some(frame) => Ok(frame),
            None => {
                // Receiving `None` here indicates the server has closed the
                // connection without sending a frame. This is unexpected and is
                // represented as a "connection reset by peer" error.
                let err = Error::new(ErrorKind::ConnectionReset, "connection reset by server");

                Err(err.into())
            }
        }
    }
}

impl Subscriber {
    /// Returns the set of channels currently subscribed to.
    pub fn get_subscribed(&self) -> &[String] {
        &self.subscribed_channels
    }

    /// Receive the next message published on a subscribed channel, waiting if
    /// necessary.
    ///
    /// `None` indicates the subscription has been terminated.
    pub async fn next_message(&mut self) -> crate::Result<Option<Message>> {
        match self.client.connection.read_frame().await? {
            Some(mframe) => {
                debug!(?mframe);

                match mframe {
                    Frame::Array(ref frame) => match frame.as_slice() {
                        [message, channel, content] if *message == "message" => Ok(Some(Message {
                            channel: channel.to_string(),
                            content: Bytes::from(content.to_string()),
                        })),
                        _ => Err(mframe.to_error()),
                    },
                    frame => Err(frame.to_error()),
                }
            }
            None => Ok(None),
        }
    }

    /// Convert the subscriber into a `Stream` yielding new messages published
    /// on subscribed channels.
    ///
    /// `Subscriber` does not implement stream itself as doing so with safe code
    /// is non trivial. The usage of async/await would require a manual Stream
    /// implementation to use `unsafe` code. Instead, a conversion function is
    /// provided and the returned stream is implemented with the help of the
    /// `async-stream` crate.
    pub fn into_stream(mut self) -> impl Stream<Item = crate::Result<Message>> {
        // Uses the `try_stream` macro from the `async-stream` crate. Generators
        // are not stable in Rust. The crate uses a macro to simulate generators
        // on top of async/await. There are limitations, so read the
        // documentation there.
        try_stream! {
            while let Some(message) = self.next_message().await? {
                yield message;
            }
        }
    }

    /// Subscribe to a list of new channels
    #[instrument(skip(self))]
    pub async fn subscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        // Issue the subscribe command
        self.client.subscribe_cmd(channels).await?;

        // Update the set of subscribed channels.
        self.subscribed_channels
            .extend(channels.iter().map(Clone::clone));

        Ok(())
    }

    /// Unsubscribe to a list of new channels
    #[instrument(skip(self))]
    pub async fn unsubscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        let frame = Unsubscribe::new(channels).into_frame();

        debug!(request = ?frame);

        // Write the frame to the socket
        let resp_frame = frame.encode_resp()?;
        self.client.connection.write_frame(resp_frame).await?;

        // if the input channel list is empty, server acknowledges as unsubscribing
        // from all subscribed channels, so we assert that the unsubscribe list received
        // matches the client subscribed one
        let num = if channels.is_empty() {
            self.subscribed_channels.len()
        } else {
            channels.len()
        };

        // Read the response
        for _ in 0..num {
            let response = self.client.read_response().await?;

            match response {
                Frame::Array(ref frame) => match frame.as_slice() {
                    [unsubscribe, channel, ..] if *unsubscribe == "unsubscribe" => {
                        let len = self.subscribed_channels.len();

                        if len == 0 {
                            // There must be at least one channel
                            return Err(response.to_error());
                        }

                        // unsubscribed channel should exist in the subscribed list at this point
                        self.subscribed_channels.retain(|c| *channel != &c[..]);

                        // Only a single channel should be removed from the
                        // list of subscribed channels.
                        if self.subscribed_channels.len() != len - 1 {
                            return Err(response.to_error());
                        }
                    }
                    _ => return Err(response.to_error()),
                },
                frame => return Err(frame.to_error()),
            };
        }

        Ok(())
    }
}
