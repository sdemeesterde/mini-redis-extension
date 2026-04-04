use crate::{Connection, Db, Frame, Parse};
use bytes::Bytes;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Sismember {
    key: String,
    member: String,
}

impl Sismember {
    pub(crate) fn new(key: String, member: String) -> Sismember {
        Sismember { key, member }
    }

    /// Parse a `Sismember` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `SISMEMBER` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `SISMEMBER` value on success. If the frame is malformed,
    /// `Err` is returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing two frames.
    ///
    /// ```text
    /// SISMEMBER key member
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Sismember> {
        let key = parse.next_string()?;
        let member = parse.next_string()?;

        Ok(Sismember { key, member })
    }

    /// Apply the `Sismember` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: Option<&mut Connection>) -> crate::Result<()> {
        let ismember = db.sismember(&self.key, &self.member);

        if let Some(dst) = dst {
            let response = Frame::Integer(ismember);
            debug!(?response);

            let resp_frame = response.encode_resp()?;
            dst.write_frame(resp_frame).await?;
        }

        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Sismember` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("sismember".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(Bytes::from(self.member.into_bytes()));
        frame
    }
}
