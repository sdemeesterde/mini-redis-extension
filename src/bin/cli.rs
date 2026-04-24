use miniredis::{DEFAULT_PORT, clients::Client};

use bytes::Bytes;
use clap::{Parser, Subcommand};
use std::num::ParseIntError;
use std::str;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(
    name = "miniredis-cli",
    version,
    author,
    about = "Issue Redis commands"
)]
struct Cli {
    #[clap(subcommand)]
    command: Command,

    #[arg(id = "hostname", long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

#[derive(Subcommand, Debug)]
enum Command {
    #[command(alias = "Ping", alias = "PING")]
    Ping {
        /// Message to ping
        msg: Option<Bytes>,
    },
    /// Get the value of key.
    #[command(alias = "Get", alias = "GET")]
    Get {
        /// Name of key to get
        key: String,
    },
    /// Set key to hold the string value.
    #[command(alias = "Set", alias = "SET")]
    Set {
        /// Name of key to set
        key: String,

        /// Value to set.
        value: Bytes,

        /// Expire the value after specified amount of time
        #[arg(value_parser = duration_from_ms_str)]
        expires: Option<Duration>,
    },
    /// Del the key(s)
    #[command(alias = "Del", alias = "DEL")]
    Del {
        /// Name of the keys to remove
        keys: Vec<String>,
    },
    /// Len
    #[command(alias = "Len", alias = "LEN")]
    Len,
    /// Add the members to set associated key
    #[command(alias = "Sadd", alias = "SADD")]
    Sadd {
        /// Name of the key
        key: String,

        /// Members to add the the key set
        members: Vec<String>,
    },
    /// Check if member is included is set associated key
    #[command(alias = "Sismember", alias = "SISMEMBER")]
    Sismember {
        /// Name of the key
        key: String,

        /// Member to check whether is contained
        member: String,
    },
    /// Returns the number of member(s) associated with given key
    #[command(alias = "Slength", alias = "SLENGTH")]
    Slength {
        /// Name of the key
        key: String,
    },
    /// Remove member(s) from the set associated with given key
    #[command(alias = "Srem", alias = "SREM")]
    Srem {
        /// Name of the key
        key: String,

        /// Member(s) to remove
        members: Vec<String>,
    },
    /// Adds all the specified members with the specified scores
    /// to the sorted set stored at key.
    #[command(alias = "Zadd", alias = "ZADD")]
    Zadd {
        /// Name of the key
        key: String,

        /// Entries (pair of score-member)
        /// Clap only supports flat list, it sees: ["10", "foo", "20", "bar"]
        entries: Vec<String>,
    },
    /// Returns the length of (member-score) set associated with given key
    #[command(alias = "Zlength", alias = "ZLENGTH")]
    Zlength {
        /// Name of the key
        key: String,
    },
    /// Get the score of the key associated member
    #[command(alias = "Zscore", alias = "ZSCORE")]
    Zscore {
        /// Name of the key
        key: String,

        /// Member to check its score
        member: String,
    },
    /// Returns the specified range of elements in the sorted set stored at key.
    #[command(alias = "Zrange", alias = "ZRANGE")]
    Zrange {
        /// Name of the key
        key: String,
        /// Start of the range (included)
        start: u64,
        /// End of the range (included)
        stop: u64,

        /// Optional REV
        /// Reverse the order of the output (decreasing)
        #[arg(value_parser = ["REV"], ignore_case = true)]
        rev: Option<String>,

        /// Optional [LIMIT offset count]
        limit_args: Vec<String>,
    },
    /// Returns the 0-indexed rank of the mamber in the sorted key.
    #[command(alias = "Zrank", alias = "ZRANK")]
    Zrank {
        /// Name of the key
        key: String,

        /// Member
        member: String,

        /// Optional DESC
        #[arg(value_parser = ["DESC"], ignore_case = true)]
        desc: Option<String>,
    },
    /// Remove member(s) from the sorted set associated key
    #[command(alias = "Zrem", alias = "ZREM")]
    Zrem {
        /// Name of the key
        key: String,

        /// Member(s) to remove
        members: Vec<String>,
    },
    ///  Publisher to send a message to a specific channel.
    #[command(alias = "Publish", alias = "PUBLISH")]
    Publish {
        /// Name of channel
        channel: String,

        /// Message to publish
        message: Bytes,
    },
    /// Subscribe a client to a specific channel or channels.
    #[command(alias = "Subscribe", alias = "SUBSCRIBE")]
    Subscribe {
        /// Specific channel or channels
        channels: Vec<String>,
    },
}

/// Entry point for CLI tool.
///
/// The `[tokio::main]` annotation signals that the Tokio runtime should be
/// started when the function is called. The body of the function is executed
/// within the newly spawned runtime.
///
/// `flavor = "current_thread"` is used here to avoid spawning background
/// threads. The CLI tool use case benefits more by being lighter instead of
/// multi-threaded.
#[tokio::main(flavor = "current_thread")]
async fn main() -> miniredis::Result<()> {
    // Enable logging
    tracing_subscriber::fmt::try_init()?;

    // Parse command line arguments
    let cli = Cli::parse();

    // Get the remote address to connect to
    let addr = format!("{}:{}", cli.host, cli.port);

    // Establish a connection
    let mut client = Client::connect(&addr).await?;

    // Process the requested command
    match cli.command {
        Command::Ping { msg } => {
            let value = client.ping(msg).await?;
            if let Ok(string) = str::from_utf8(&value) {
                println!("\"{}\"", string);
            } else {
                println!("{:?}", value);
            }
        }
        Command::Get { key } => {
            if let Some(value) = client.get(&key).await? {
                if let Ok(string) = str::from_utf8(&value) {
                    println!("\"{string}\"");
                } else {
                    println!("{value:?}");
                }
            } else {
                println!("(nil)");
            }
        }
        Command::Set {
            key,
            value,
            expires: None,
        } => {
            client.set(&key, value).await?;
            println!("OK");
        }
        Command::Set {
            key,
            value,
            expires: Some(expires),
        } => {
            client.set_expires(&key, value, expires).await?;
            println!("OK");
        }
        Command::Del { keys } => {
            let removed = client.del(keys).await?;
            println!("(integer) {removed:?}");
        }
        Command::Len => {
            let len = client.len().await?;
            println!("(integer) {len:?}");
        }
        Command::Sadd { key, members } => {
            let added = client.sadd(&key, members).await?;
            println!("(integer) {added:?}");
        }
        Command::Sismember { key, member } => {
            let is_member = client.sismember(&key, &member).await?;
            println!("(integer) {is_member:?}");
        }
        Command::Slength { key } => {
            let length = client.slength(&key).await?;
            println!("(integer) {length:?}");
        }
        Command::Srem { key, members } => {
            let removed = client.srem(&key, members).await?;
            println!("(integer) {removed:?}");
        }
        Command::Zadd { key, entries } => {
            let mut iter = entries.into_iter();
            let mut pairs = Vec::new();
            while let (Some(score), Some(member)) = (iter.next(), iter.next()) {
                let score = score.parse::<u64>()?;
                pairs.push((score, member));
            }
            let added = client.zadd(&key, pairs).await?;
            println!("(integer) {added:?}");
        }
        Command::Zlength { key } => {
            let length = client.zlength(&key).await?;
            println!("(integer) {length:?}");
        }
        Command::Zscore { key, member } => {
            let score = client.zscore(&key, &member).await?;
            match score {
                Some(s) => println!("(integer) {s:?}"),
                None => println!("(nil)"),
            }
        }
        Command::Zrange {
            key,
            start,
            stop,
            rev,
            limit_args,
        } => {
            let mut iter = limit_args.iter();
            let (offset, count) = match iter.next() {
                None => (None, None),
                Some(arg) if arg.eq_ignore_ascii_case("LIMIT") => {
                    let off = iter.next().ok_or("LIMIT requires offset")?;
                    let cnt = iter.next().ok_or("LIMIT requires count")?;

                    (Some(off.parse()?), Some(cnt.parse()?))
                }
                Some(arg) => {
                    return Err(format!("Unexpected argument: {}", arg).into());
                }
            };
            let score_member = client
                .zrange(&key, start, stop, rev.is_some(), offset, count)
                .await?;
            if score_member.is_empty() {
                println!("(nil)");
            } else {
                for (score, member) in score_member.into_iter() {
                    println!("Score: {score:?} \t by: {member}");
                }
            }
        }
        Command::Zrank { key, member, desc } => {
            let rank = client.zrank(&key, &member, desc.is_some()).await?;
            match rank {
                Some(r) => println!("(integer) {r}"),
                None => println!("(nil)"),
            }
        }
        Command::Zrem { key, members } => {
            let removed = client.zrem(&key, members).await?;
            println!("(integer) {removed:?}");
        }
        Command::Publish { channel, message } => {
            client.publish(&channel, message).await?;
            println!("Publish OK");
        }
        Command::Subscribe { channels } => {
            if channels.is_empty() {
                return Err("channel(s) must be provided".into());
            }
            let mut subscriber = client.subscribe(channels).await?;

            // await messages on channels
            while let Some(msg) = subscriber.next_message().await? {
                println!(
                    "got message from the channel: {}; message = {:?}",
                    msg.channel, msg.content
                );
            }
        }
    }

    Ok(())
}

fn duration_from_ms_str(src: &str) -> Result<Duration, ParseIntError> {
    let ms = src.parse::<u64>()?;
    Ok(Duration::from_millis(ms))
}
