use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

#[test]
fn test_verify_data_sbe_files() -> anyhow::Result<()> {
    let data_dir = std::path::Path::new("data");
    if !data_dir.exists() {
        return Ok(());
    }

    let mut found_file = false;
    for entry in std::fs::read_dir(data_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("sbe") {
            let metadata = std::fs::metadata(&path)?;
            if metadata.len() == 0 {
                continue;
            }
            found_file = true;
            assert_eq!(
                metadata.len() % (64 << 20),
                0,
                "File {:?} must be 64 MiB aligned",
                path
            );

            let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path)?;
            let mut snapshot_count = 0;
            let mut update_count = 0;

            while let Some(msg) = reader.read_msg()? {
                match msg {
                    AnyMessage::Snapshot(dec) => {
                        let snap = SnapshotDomain::try_from_decoder(dec)?;
                        assert!(!snap.symbol.is_empty());
                        snapshot_count += 1;
                    }
                    AnyMessage::PriceLevelUpdate(dec) => {
                        let upd = PriceLevelUpdateDomain::try_from_decoder(dec)?;
                        assert!(!upd.symbol.is_empty());
                        update_count += 1;
                    }
                    _ => {}
                }
            }

            println!(
                "Successfully verified file {:?}: {} snapshots, {} price updates",
                path, snapshot_count, update_count
            );
            assert!(snapshot_count + update_count > 0);
        }
    }

    assert!(
        found_file,
        "At least one valid SBE file must exist in data/"
    );
    Ok(())
}
