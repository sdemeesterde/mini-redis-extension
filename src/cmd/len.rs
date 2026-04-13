use crate::{Connection, Db, Frame};

use bytes::Bytes;
use tracing::{debug, instrument};

/// Returns the len of entries.
#[derive(Debug)]
pub struct Len;

impl Len {
    /// Parse a `Len` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `LEN` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Len` value on success. If the frame is malformed, `Err` is
    /// returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing two entries.
    ///
    /// ```text
    /// LEN
    /// ```
    pub(crate) fn parse_frames() -> crate::Result<Len> {
        Ok(Len)
    }

    /// Apply the `Len` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: Option<&mut Connection>) -> crate::Result<()> {
        let len = db.len();
        if let Some(dst) = dst {
            let response = Frame::Integer(len);

            debug!(?response);

            // Write the response back to the client
            let resp_frame = response.encode_resp()?;
            dst.write_frame(resp_frame).await?;
        }

        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Len` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("len".as_bytes()));
        frame
    }
}
