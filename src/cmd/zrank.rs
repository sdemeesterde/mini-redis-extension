use crate::{Connection, Db, Frame, Parse};
use bytes::Bytes;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Zrank {
    key: String,
    member: String,
    desc: bool,
}

impl Zrank {
    pub(crate) fn new(key: String, member: String, desc: bool) -> Zrank {
        Zrank { key, member, desc }
    }

    /// Parse a `Zrank` instance from a received frame.
    ///
    /// The `Parse` argument provides a cursor-like API to read fields from the
    /// `Frame`. At this point, the entire frame has already been received from
    /// the socket.
    ///
    /// The `ZRANK` string has already been consumed.
    ///
    /// # Returns
    ///
    /// Returns the `Zrank` value on success. If the frame is malformed,
    /// `Err` is returned.
    ///
    /// # Format
    ///
    /// Expects an array frame containing at least two entries.
    ///
    /// ```text
    /// ZRANK key member [DESC]
    /// ```
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Zrank> {
        let key = parse.next_string()?;
        let member = parse.next_string()?;
        let mut desc = false;

        if !parse.empty() {
            let arg = parse.next_string()?;
            if arg.eq_ignore_ascii_case("DESC") {
                desc = true;
            }
        }

        Ok(Zrank { key, member, desc })
    }

    /// Apply the `Zrank` command to the specified `Db` instance.
    ///
    /// The response is written to `dst`. This is called by the server in order
    /// to execute a received command.
    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: Option<&mut Connection>) -> crate::Result<()> {
        let rank = db.zrank(self.key, self.member, self.desc);

        if let Some(dst) = dst {
            let response = match rank {
                Some(r) => Frame::Integer(r),
                None => Frame::Null,
            };
            debug!(?response);

            let resp_frame = response.encode_resp()?;
            dst.write_frame(resp_frame).await?;
        }

        Ok(())
    }

    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Zrank` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("zrank".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(Bytes::from(self.member.into_bytes()));
        if self.desc {
            frame.push_bulk(Bytes::from("desc".as_bytes()));
        }
        frame
    }
}
