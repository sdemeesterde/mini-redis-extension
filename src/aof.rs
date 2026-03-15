use std::time::Duration;
use tokio::fs::OpenOptions;

use bytes::Bytes;
use tokio::fs::File;
use tokio::io::{self, AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::time;

#[derive(Debug)]
pub struct Aof {
    tx: mpsc::Sender<Bytes>,
    rx: mpsc::Receiver<Bytes>,

    buf_writer: BufWriter<File>,
}

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

    pub(crate) async fn run(&mut self) -> Result<(), io::Error> {
        loop {
            time::sleep(Duration::from_secs(1)).await;
            self.write_to_file().await?;
        }
    }
}

impl AofWriter {
    pub(crate) fn new(aof: &Aof) -> AofWriter {
        AofWriter { tx: aof.tx.clone() }
    }
    pub(crate) fn fork(aof_writer: &AofWriter) -> AofWriter {
        AofWriter {
            tx: aof_writer.tx.clone(),
        }
    }
    pub(crate) async fn write_to_buffer(&self, frame: Bytes) -> crate::Result<()> {
        self.tx.send(frame).await?;
        Ok(())
    }
}
