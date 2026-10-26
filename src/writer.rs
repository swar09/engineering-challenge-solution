use std::fs;
use std::path::{Path, PathBuf};
use std::thread::{self, JoinHandle};
use anyhow::{Context, Result};
use chrono::Utc;
use crossbeam::channel::Receiver;
use tracing::{info, warn};

use crate::orderbook::OrderbookMessage;
use crate::util::AlignedSbeWriter;

pub const CHUNK_SIZE_64_MEBIBYTES: usize = 64 << 20;
pub const BOUNDED_CHANNEL_CAPACITY: usize = 250_000;

pub struct SbeWriterHandle {
    join_handle: Option<JoinHandle<Result<()>>>,
    file_path: PathBuf,
}

impl SbeWriterHandle {
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    pub fn join(mut self) -> Result<()> {
        if let Some(handle) = self.join_handle.take() {
            handle
                .join()
                .map_err(|e| anyhow::anyhow!("Writer thread panicked: {e:?}"))?
        } else {
            Ok(())
        }
    }
}

pub struct SbeBoundedWriter;

impl SbeBoundedWriter {
    pub fn spawn(
        data_dir: impl AsRef<Path>,
        rx: Receiver<Vec<OrderbookMessage>>,
    ) -> Result<SbeWriterHandle> {
        let dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)
            .with_context(|| format!("Failed to create data directory: {:?}", dir))?;

        let timestamp = Utc::now().timestamp_micros();
        let file_path = dir.join(format!("market_orderbook_{timestamp}.sbe"));
        let thread_file_path = file_path.clone();

        let join_handle = thread::Builder::new()
            .name("sbe-bounded-writer".to_string())
            .spawn(move || -> Result<()> {
                info!("Starting SBE writer on {:?}", thread_file_path);
                let mut writer =
                    AlignedSbeWriter::create(&thread_file_path, CHUNK_SIZE_64_MEBIBYTES)
                        .with_context(|| {
                            format!(
                                "Failed to create AlignedSbeWriter at {:?}",
                                thread_file_path
                            )
                        })?;

                let mut msg_count = 0u64;
                while let Ok(batch) = rx.recv() {
                    for msg in batch {
                        if let Err(e) = writer.write_msg(&msg) {
                            warn!("Failed to encode SBE message: {e}");
                        } else {
                            msg_count += 1;
                        }
                    }
                }

                info!(
                    "Channel disconnected, flushing final chunk (total messages: {msg_count})..."
                );
                writer
                    .flush()
                    .context("Failed to flush AlignedSbeWriter on shutdown")?;
                info!(
                    "SBE writer finished successfully on {:?}",
                    thread_file_path
                );
                Ok(())
            })
            .context("Failed to spawn writer thread")?;

        Ok(SbeWriterHandle {
            join_handle: Some(join_handle),
            file_path,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::dtos::challenge_orderbook::{
        AnyMessage, PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
        SnapshotDomain,
    };
    use crate::util::{AlignedSbeReader, OrderbookSchema};
    use crate::Side;
    use chrono::DateTime;

    #[test]
    fn test_sbe_bounded_writer_aligned_output() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let (tx, rx) = crossbeam::channel::bounded(BOUNDED_CHANNEL_CAPACITY);

        let handle = SbeBoundedWriter::spawn(dir.path(), rx)?;
        let file_path = handle.file_path().to_path_buf();

        let ts = DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap();
        let snapshot = SnapshotDomain {
            received_at: ts,
            exchange_timestamp: ts,
            bids: vec![SnapshotBidsEntryDomain {
                price_level: 450_000,
                quantity: 1_000_000,
            }],
            asks: vec![SnapshotAsksEntryDomain {
                price_level: 550_000,
                quantity: 2_000_000,
            }],
            symbol: "TEST".to_string(),
        };

        let update = PriceLevelUpdateDomain {
            backtest_timestamp: ts,
            exchange_timestamp: ts,
            price_level: 450_000,
            quantity: 500_000,
            side: Side::Buy,
            symbol: "TEST".to_string(),
        };

        tx.send(vec![
            OrderbookMessage::Snapshot(snapshot.clone()),
            OrderbookMessage::PriceLevelUpdate(update.clone()),
        ])?;

        drop(tx);
        handle.join()?;

        let metadata = fs::metadata(&file_path)?;
        assert_eq!(
            metadata.len() % (CHUNK_SIZE_64_MEBIBYTES as u64),
            0,
            "File size must be a multiple of 64 MiB"
        );
        assert_eq!(metadata.len(), CHUNK_SIZE_64_MEBIBYTES as u64);

        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&file_path)?;

        let Some(AnyMessage::Snapshot(decoded_snap)) = reader.read_msg()? else {
            panic!("Expected SnapshotDomain");
        };
        assert_eq!(SnapshotDomain::try_from_decoder(decoded_snap)?, snapshot);

        let Some(AnyMessage::PriceLevelUpdate(decoded_upd)) = reader.read_msg()? else {
            panic!("Expected PriceLevelUpdateDomain");
        };
        assert_eq!(
            PriceLevelUpdateDomain::try_from_decoder(decoded_upd)?,
            update
        );

        assert!(reader.read_msg()?.is_none());

        Ok(())
    }
}
