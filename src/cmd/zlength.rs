use crate::{Connection, Db, Frame, Parse};
use bytes::Bytes;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Zlength {
    key: String,
}

impl Zlength {
    pub(crate) fn new(key: String) -> Zlength {
        Zlength { key }
    }

    /// Parse a `Zlength` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `ZLENGTH` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `ZLENGTH` value on success. If the frame is malformed,
    /// `Err` is returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing one frame.
    ///
    /// ```text
    /// ZLENGTH key
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zlength> {
        let key = parse.next_string()?;

        Ok(Zlength { key })
    }

    /// Apply the `Zlength` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: Option<&mut Connection>) -> crate::Result<()> {
        let length = db.zlength(&self.key);

        if let Some(dst) = dst {
            let response = Frame::Integer(length);
            debug!(?response);

            let resp_frame = response.encode_resp()?;
            dst.write_frame(resp_frame).await?;
        }

        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Zlength` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("zlength".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame
    }
}
