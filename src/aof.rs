use std::io::Cursor;
use std::time::Duration;
use tokio::fs::OpenOptions;

use bytes::{Buf, Bytes, BytesMut};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::mpsc;
use tokio::time;

use crate::{Command, Db, Frame, Shutdown};

/// Represents the Append-Only-File (AOF) file.
///
/// It is made up of `Sender`'s and a `Receiver` that acts as
/// the buffer.
///
/// The actual AOF file is wrapped in BufWriter to further save
/// I/O call when writing frames from the buffer to the file.
#[derive(Debug)]
pub struct Aof {
    /// The buffer sender part
    tx: mpsc::Sender<Bytes>,
    /// The buffer receiver part
    rx: mpsc::Receiver<Bytes>,

    /// The AOF file
    buf_writer: BufWriter<File>,
}

/// Factory struct responsible for producing AofWriter for each
/// connection, using the fork method.
#[derive(Debug)]
pub struct AofWriterFactory {
    tx: mpsc::Sender<Bytes>,
}

/// Every connection must be able to write to the AOF file.
///
/// `AofWriter` serves as a handler for that giving a sender access
/// to the AOF buffer.
#[derive(Debug)]
pub struct AofWriter {
    tx: mpsc::Sender<Bytes>,
}

impl Aof {
    pub(crate) async fn new(filename: String) -> Result<Aof, tokio::io::Error> {
        let (tx, rx) = mpsc::channel(4096);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&filename)
            .await?;
        let buf_writer = BufWriter::new(file);
        Ok(Aof { tx, rx, buf_writer })
    }
    /// Write the frame waiting in the aof buffer to disk
    pub(crate) async fn write_to_file(&mut self) -> Result<(), tokio::io::Error> {
        // Pull all available frames
        loop {
            match self.rx.try_recv() {
                Ok(frame) => {
                    self.buf_writer.write_all(&frame).await?;
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        self.buf_writer.flush().await?;
        Ok(())
    }

    /// Future corresponding to the AOF background task
    ///
    /// It sleeps for 1 second, then writes and flushes all available frames
    /// from the AOF buffer.
    pub(crate) async fn run(&mut self) -> Result<(), tokio::io::Error> {
        loop {
            time::sleep(Duration::from_secs(1)).await;
            self.write_to_file().await?;
        }
    }

    /// Fill in the database with redis RESP commands from `filename`
    pub(crate) async fn warmup_db(
        db: &Db,
        filename: String,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use crate::frame::Error::Incomplete;

        let file = File::open(filename).await?;
        let mut reader = BufReader::new(file);

        let mut buf = BytesMut::with_capacity(4 * 1024);

        loop {
            // Either reader reached EOF or buf is full
            if 0 == reader.read_buf(&mut buf).await? {
                break;
            }

            loop {
                let len = {
                    let mut buf_cursor = Cursor::new(&buf[..]);

                    match Frame::check(&mut buf_cursor) {
                        Ok(_) => buf_cursor.position() as usize,
                        // If incomplete, break to attempt reading more bytes
                        Err(Incomplete) => break,
                        // Simply ignore corrupted frames
                        Err(_) => break,
                    }
                };

                let mut buf_cursor = Cursor::new(&buf[..len]);
                buf_cursor.set_position(0);

                let frame = Frame::parse(&mut buf_cursor)?;
                buf.advance(len);

                let cmd = Command::from_frame(frame)?;

                // Server fills its database.
                // No need to provide a connection, hence use of None.
                cmd.apply(db, None, shutdown).await?;

                if buf.is_empty() {
                    break;
                }
            }
        }
        Ok(())
    }
}

impl AofWriterFactory {
    pub(crate) fn new(aof: &Aof) -> AofWriterFactory {
        AofWriterFactory { tx: aof.tx.clone() }
    }
    /// Every connection must receive a clone `AofWriter`
    pub(crate) fn fork(&self) -> AofWriter {
        AofWriter {
            tx: self.tx.clone(),
        }
    }
}

impl AofWriter {
    /// Write given `frame` to the buffer.
    pub(crate) async fn write_to_buffer(&self, frame: Bytes) -> crate::Result<()> {
        self.tx.send(frame).await?;
        Ok(())
    }
}
