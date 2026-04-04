use bytes::Bytes;
use tracing::{debug, instrument};

use crate::{db::ScoreEntry, Connection, Db, Frame, Parse};

#[derive(Debug)]
pub struct Zrange {
    key: String,
    start: u64,
    stop: u64,
    rev: bool,
    offset: Option<u64>,
    count: Option<u64>,
}

impl Zrange {
    pub(crate) fn new(
        key: String,
        start: u64,
        stop: u64,
        rev: bool,
        offset: Option<u64>,
        count: Option<u64>,
    ) -> Zrange {
        assert!(
            offset.is_some() == count.is_some(),
            "Both optional arguments must either both be Some() or None."
        );
        Zrange {
            key,
            start,
            stop,
            rev,
            offset,
            count,
        }
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
    /// Expects an array frame containing at least three entries.
    /// Start and Stop are expected to be unsigned integers.
    ///
    /// ```text
    /// ZRANGE key start stop [REV] [LIMIT offset count]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zrange> {
        let key = parse.next_string()?;
        let start = parse.next_int()?;
        let stop = parse.next_int()?;

        let mut rev = false;
        let mut offset = None;
        let mut count = None;

        if parse.empty() {
            return Ok(Zrange {
                key,
                start,
                stop,
                rev,
                offset,
                count,
            });
        }
        let arg1 = parse.next_string()?;

        if arg1.eq_ignore_ascii_case("REV") {
            rev = true;

            if parse.empty() {
                return Ok(Zrange {
                    key,
                    start,
                    stop,
                    rev,
                    offset,
                    count,
                });
            }

            let arg2 = parse.next_string()?;
            if !arg2.eq_ignore_ascii_case("LIMIT") {
                return Err("Currently `ZRANGE` only supports REV and LIMIT options".into());
            }

            offset = Some(parse.next_int()?);
            count = Some(parse.next_int()?);
        } else if arg1.eq_ignore_ascii_case("LIMIT") {
            offset = Some(parse.next_int()?);
            count = Some(parse.next_int()?);
        } else {
            return Err("Currently `ZRANGE` only supports REV and LIMIT options".into());
        }

        Ok(Zrange {
            key,
            start,
            stop,
            rev,
            offset,
            count,
        })
    }

    /// Apply the `Zrange` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: Option<&mut Connection>) -> crate::Result<()> {
        if let Some(dst) = dst {
            let mut response = Frame::array();
            let entry_scores: Vec<ScoreEntry> = db.zrange(
                &self.key,
                self.start,
                self.stop,
                self.rev,
                self.offset,
                self.count,
            );

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
        if self.rev {
            frame.push_bulk(Bytes::from("rev"));
        }
        if let (Some(offset), Some(count)) = (self.offset, self.count) {
            frame.push_bulk(Bytes::from("limit"));
            frame.push_int(offset);
            frame.push_int(count);
        }
        frame
    }
}
