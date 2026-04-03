use crate::{Connection, Db, Frame, Parse};
use bytes::Bytes;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Zrem {
    key: String,
    members: Vec<String>,
}

impl Zrem {
    pub(crate) fn new(key: String, members: Vec<String>) -> Zrem {
        Zrem { key, members }
    }

    /// Parse a `Zrem` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `ZREM` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Zrem` value on success. If the frame is malformed,
    /// `Err` is returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing at least two frames.
    ///
    /// ```text
    /// ZREM key member [member]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zrem> {
        let key = parse.next_string()?;
        let mut members = Vec::new();

        while !parse.empty() {
            let member = parse.next_string()?;
            members.push(member);
        }

        Ok(Zrem { key, members })
    }

    /// Apply the `Zrem` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: Option<&mut Connection>) -> crate::Result<()> {
        let removed = db.srem(self.key, self.members);

        if let Some(dst) = dst {
            let response = Frame::Integer(removed);
            debug!(?response);

            let resp_frame = response.encode_resp()?;
            dst.write_frame(resp_frame).await?;
        }

        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Zrem` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("zrem".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        for member in self.members.into_iter() {
            frame.push_bulk(Bytes::from(member.into_bytes()));
        }
        frame
    }
}
