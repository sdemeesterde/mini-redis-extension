use std::time::Duration;
use tokio::fs::OpenOptions;

use bytes::Bytes;
use tokio::fs::File;
use tokio::io::{self, AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::time;

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
    pub(crate) async fn run(&mut self) -> Result<(), io::Error> {
        loop {
            time::sleep(Duration::from_secs(1)).await;
            self.write_to_file().await?;
        }
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
