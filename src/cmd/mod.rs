mod del;
pub use del::Del;

mod get;
pub use get::Get;

mod publish;
pub use publish::Publish;

mod set;
pub use set::Set;

mod subscribe;
pub use subscribe::{Subscribe, Unsubscribe};

mod ping;
pub use ping::Ping;

mod sadd;
pub use sadd::Sadd;

mod sismember;
pub use sismember::Sismember;

mod slength;
pub use slength::Slength;

mod srem;
pub use srem::Srem;

mod zadd;
pub use zadd::Zadd;

mod zscore;
pub use zscore::Zscore;

mod zrange;
pub use zrange::Zrange;

mod zrank;
pub use zrank::Zrank;

mod zrem;
pub use zrem::Zrem;

mod unknown;
pub use unknown::Unknown;

use crate::{Connection, Db, Frame, Parse, ParseError, Shutdown};

/// Enumeration of supported Redis commands.
///
/// Methods called on `Command` are delegated to the command implementation.
#[derive(Debug)]
pub enum Command {
    Del(Del),
    Get(Get),
    Publish(Publish),
    Set(Set),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Ping(Ping),
    Sadd(Sadd),
    Sismember(Sismember),
    Slength(Slength),
    Srem(Srem),
    Zadd(Zadd),
    Zscore(Zscore),
    Zrange(Zrange),
    Zrank(Zrank),
    Zrem(Zrem),
    Unknown(Unknown),
}

impl Command {
    /// Parse a command from a received frame.
    ///
    /// The `Frame` must represent a Redis command supported by `miniredis` and
    /// be the array variant.
    ///
    /// # Returns
    ///
    /// On success, the command value is returned, otherwise, `Err` is returned.
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        // The frame value is decorated with `Parse`. `Parse` provides a
        // "cursor" like API which makes parsing the command easier.
        //
        // The frame value must be an array variant. Any other frame variants
        // result in an error being returned.
        let mut parse = Parse::new(frame)?;

        // All redis commands begin with the command name as a string. The name
        // is read and converted to lower cases in order to do case sensitive
        // matching.
        let command_name = parse.next_string()?.to_lowercase();

        // Match the command name, delegating the rest of the parsing to the
        // specific command.
        let command = match &command_name[..] {
            "del" => Command::Del(Del::parse_frames(&mut parse)?),
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "publish" => Command::Publish(Publish::parse_frames(&mut parse)?),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::parse_frames(&mut parse)?),
            "unsubscribe" => Command::Unsubscribe(Unsubscribe::parse_frames(&mut parse)?),
            "ping" => Command::Ping(Ping::parse_frames(&mut parse)?),
            "sadd" => Command::Sadd(Sadd::parse_frames(&mut parse)?),
            "sismember" => Command::Sismember(Sismember::parse_frames(&mut parse)?),
            "slength" => Command::Slength(Slength::parse_frames(&mut parse)?),
            "srem" => Command::Srem(Srem::parse_frames(&mut parse)?),
            "zadd" => Command::Zadd(Zadd::parse_frames(&mut parse)?),
            "zscore" => Command::Zscore(Zscore::parse_frames(&mut parse)?),
            "zrange" => Command::Zrange(Zrange::parse_frames(&mut parse)?),
            "zrank" => Command::Zrank(Zrank::parse_frames(&mut parse)?),
            "zrem" => Command::Zrem(Zrem::parse_frames(&mut parse)?),
            _ => {
                // The command is not recognized and an Unknown command is
                // returned.
                //
                // `return` is called here to skip the `finish()` call below. As
                // the command is not recognized, there is most likely
                // unconsumed fields remaining in the `Parse` instance.
                return Ok(Command::Unknown(Unknown::new(command_name)));
            }
        };

        // Check if there is any remaining unconsumed fields in the `Parse`
        // value. If fields remain, this indicates an unexpected frame format
        // and an error is returned.
        parse.finish()?;

        // The command has been successfully parsed
        Ok(command)
    }

    /// Apply the command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    pub(crate) async fn apply(
        self,
        db: &Db,
        dst: Option<&mut Connection>,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;

        match self {
            Del(cmd) => cmd.apply(db, dst).await,
            Get(cmd) => cmd.apply(db, dst).await,
            Publish(cmd) => cmd.apply(db, dst).await,
            Set(cmd) => cmd.apply(db, dst).await,
            Subscribe(cmd) => {
                if let Some(dst) = dst {
                    cmd.apply(db, dst, shutdown).await
                } else {
                    Err("A connection is required to subscribe".into())
                }
            }
            Ping(cmd) => {
                if let Some(dst) = dst {
                    cmd.apply(dst).await
                } else {
                    Err("A client must be provided for Ping cmd to respond to".into())
                }
            }
            Unknown(cmd) => {
                if let Some(dst) = dst {
                    cmd.apply(dst).await
                } else {
                    Err("Unknown command received with no connection provided".into())
                }
            }
            Sadd(cmd) => cmd.apply(db, dst).await,
            Sismember(cmd) => cmd.apply(db, dst).await,
            Slength(cmd) => cmd.apply(db, dst).await,
            Srem(cmd) => cmd.apply(db, dst).await,
            Zadd(cmd) => cmd.apply(db, dst).await,
            Zscore(cmd) => cmd.apply(db, dst).await,
            Zrange(cmd) => cmd.apply(db, dst).await,
            Zrank(cmd) => cmd.apply(db, dst).await,
            Zrem(cmd) => cmd.apply(db, dst).await,
            // `Unsubscribe` cannot be applied. It may only be received from the
            // context of a `Subscribe` command.
            Unsubscribe(_) => Err("`Unsubscribe` is unsupported in this context".into()),
        }
    }

    /// Returns true if the commmand modifies the db.
    /// Used to filter commands relevant for AOF.
    pub(crate) fn is_write_command(&self) -> bool {
        matches!(
            self,
            Command::Set(_)
                | Command::Del(_)
                | Command::Sadd(_)
                | Command::Srem(_)
                | Command::Zadd(_)
                | Command::Zrem(_)
        )
    }

    /// Returns the command name
    pub(crate) fn get_name(&self) -> &str {
        match self {
            Command::Del(_) => "del",
            Command::Get(_) => "get",
            Command::Publish(_) => "pub",
            Command::Set(_) => "set",
            Command::Subscribe(_) => "subscribe",
            Command::Unsubscribe(_) => "unsubscribe",
            Command::Ping(_) => "ping",
            Command::Sadd(_) => "sadd",
            Command::Sismember(_) => "sismember",
            Command::Slength(_) => "slength",
            Command::Srem(_) => "srem",
            Command::Zadd(_) => "zadd",
            Command::Zscore(_) => "zscore",
            Command::Zrange(_) => "zrange",
            Command::Zrank(_) => "zrank",
            Command::Zrem(_) => "zrem",
            Command::Unknown(cmd) => cmd.get_name(),
        }
    }
}
