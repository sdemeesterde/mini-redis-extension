use crate::{Connection, Db, Frame, Parse};
use bytes::Bytes;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Zadd {
    key: String,
    entries: Vec<(u64, String)>,
}

impl Zadd {
    pub(crate) fn new(key: String, entries: Vec<(u64, String)>) -> Zadd {
        Zadd { key, entries }
    }

    /// Parse a `Zadd` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `ZADD` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Zadd` value on success. If the frame is malformed,
    /// `Err` is returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing at least three entries.
    ///
    /// ```text
    /// ZADD key score member [score member]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zadd> {
        let key = parse.next_string()?;
        let mut entries = Vec::new();

        while !parse.empty() {
            let score = parse.next_int()?;
            let member = parse.next_string()?;
            entries.push((score, member));
        }

        Ok(Zadd { key, entries })
    }

    /// Apply the `Zadd` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: Option<&mut Connection>) -> crate::Result<()> {
        let added = db.zadd(self.key, self.entries);

        if let Some(dst) = dst {
            let response = Frame::Integer(added);
            debug!(?response);

            let resp_frame = response.encode_resp()?;
            dst.write_frame(resp_frame).await?;
        }

        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Zadd` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("zadd".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        for (score, member) in self.entries.into_iter() {
            frame.push_int(score);
            frame.push_bulk(Bytes::from(member.into_bytes()));
        }
        frame
    }
}
