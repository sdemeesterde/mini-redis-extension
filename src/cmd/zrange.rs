use bytes::Bytes;
use tracing::{debug, instrument};

use crate::{db::EntryScore, Connection, Db, Frame, Parse};

#[derive(Debug)]
pub struct Zrange {
    key: String,
    start: u64,
    stop: u64,
}

impl Zrange {
    pub(crate) fn new(key: String, start: u64, stop: u64) -> Zrange {
        Zrange { key, start, stop }
    }
    /// Parse a `Zrange` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `ZRANGE` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Zrange` value on success. If the frame is malformed,
    /// `Err` is returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing three entries.
    /// Start and Stop are expected to be unsigned integers.
    ///
    /// ```text
    /// ZRANGE key start stop
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zrange> {
        let key = parse.next_string()?;
        let start = parse.next_int()?;
        let stop = parse.next_int()?;

        Ok(Zrange { key, start, stop })
    }

    /// Apply the `Zrange` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: Option<&mut Connection>) -> crate::Result<()> {
        if let Some(dst) = dst {
            let mut response = Frame::array();
            let entry_scores: Vec<EntryScore> = db.zrange(&self.key, self.start, self.stop);

            for entry_score in entry_scores.into_iter() {
                response.push_int(entry_score.get_score());
                response.push_bulk(Bytes::from(entry_score.get_member()));
            }

            debug!(?response);

            let resp_frame = response.encode_resp()?;
            dst.write_frame(resp_frame).await?;
        }

        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Zrange` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("zrange".as_bytes()));
        frame.push_bulk(Bytes::from(self.key));
        frame.push_int(self.start);
        frame.push_int(self.stop);
        frame
    }
}
