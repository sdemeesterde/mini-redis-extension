use std::vec;

use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Del {
    /// List of the keys to delete
    keys: Vec<String>,
}

impl Del {
    pub fn new(keys: &[String]) -> Del {
        Del {
            keys: keys.to_vec(),
        }
    }

    /// Parse a `Del` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `DEL` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the number of deleted value(s) on success. If the frame is malformed,
    /// `Err` is returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing at least two entries.
    ///
    /// ```text
    /// DEL key1 [key2, key3, ..]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Del> {
        let mut keys = vec![];

        while let Ok(s) = parse.next_string() {
            keys.push(s);
        }

        if keys.is_empty() {
            return Err("DEL expects at least one key".into());
        }

        Ok(Del { keys })
    }

    /// Apply the `Del` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: Option<&mut Connection>) -> crate::Result<()> {
        let mut removed = 0;
        for key in self.keys {
            if db.remove(key.clone()).is_some() {
                removed += 1;
            }
        }

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
    /// This is called by the client when encoding a `Get` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("del".as_bytes()));
        for key in self.keys {
            frame.push_bulk(Bytes::from(key.into_bytes()));
        }
        frame
    }
}
