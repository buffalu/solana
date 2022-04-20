use crossbeam_channel::{bounded, unbounded};
use itertools::Itertools;
use tokio::runtime::Runtime;
use {
    crate::blockstore::Blockstore,
    log::*,
    solana_measure::measure::Measure,
    solana_sdk::clock::Slot,
    std::{
        collections::HashSet,
        result::Result,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    },
};
use solana_storage_bigtable::LedgerStorage;

// Attempt to upload this many blocks in parallel
const NUM_BLOCKS_TO_UPLOAD_IN_PARALLEL: usize = 64;

// Read up to this many blocks from blockstore before blocking on the upload process
const BLOCK_READ_AHEAD_DEPTH: usize = NUM_BLOCKS_TO_UPLOAD_IN_PARALLEL * 3;

pub async fn upload_confirmed_blocks(
    runtime: Arc<Runtime>,
    blockstore: Arc<Blockstore>,
    bigtable: solana_storage_bigtable::LedgerStorage,
    starting_slot: Slot,
    ending_slot: Option<Slot>,
    force_reupload: bool,
    exit: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut measure = Measure::start("entire upload");

    info!("Loading ledger slots starting at {}...", starting_slot);
    let blockstore_slots: Vec<_> = blockstore
        .slot_meta_iterator(starting_slot)
        .map_err(|err| {
            format!(
                "Failed to load entries starting from slot {}: {:?}",
                starting_slot, err
            )
        })?
        .filter_map(|(slot, _slot_meta)| {
            if let Some(ending_slot) = &ending_slot {
                if slot > *ending_slot {
                    return None;
                }
            }
            Some(slot)
        })
        .collect();

    if blockstore_slots.is_empty() {
        return Err(format!(
            "Ledger has no slots from {} to {:?}",
            starting_slot, ending_slot
        )
            .into());
    }

    info!(
        "Found {} slots in the range ({}, {})",
        blockstore_slots.len(),
        blockstore_slots.first().unwrap(),
        blockstore_slots.last().unwrap()
    );

    // Gather the blocks that are already present in bigtable, by slot
    let bigtable_slots = if !force_reupload {
        let mut bigtable_slots = vec![];
        let first_blockstore_slot = *blockstore_slots.first().unwrap();
        let last_blockstore_slot = *blockstore_slots.last().unwrap();
        info!(
            "Loading list of bigtable blocks between slots {} and {}...",
            first_blockstore_slot, last_blockstore_slot
        );

        let mut start_slot = *blockstore_slots.first().unwrap();
        while start_slot <= last_blockstore_slot {
            let mut next_bigtable_slots = loop {
                match bigtable.get_confirmed_blocks(start_slot, 1000).await {
                    Ok(slots) => break slots,
                    Err(err) => {
                        error!("get_confirmed_blocks for {} failed: {:?}", start_slot, err);
                        // Consider exponential backoff...
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            };
            if next_bigtable_slots.is_empty() {
                break;
            }
            bigtable_slots.append(&mut next_bigtable_slots);
            start_slot = bigtable_slots.last().unwrap() + 1;
        }
        bigtable_slots
            .into_iter()
            .filter(|slot| *slot <= last_blockstore_slot)
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    // The blocks that still need to be uploaded is the difference between what's already in the
    // bigtable and what's in blockstore...
    let blocks_to_upload = {
        let blockstore_slots = blockstore_slots.iter().cloned().collect::<HashSet<_>>();
        let bigtable_slots = bigtable_slots.into_iter().collect::<HashSet<_>>();

        let mut blocks_to_upload = blockstore_slots
            .difference(&bigtable_slots)
            .cloned()
            .collect::<Vec<_>>();
        blocks_to_upload.sort_unstable();
        blocks_to_upload
    };

    if blocks_to_upload.is_empty() {
        info!("No blocks need to be uploaded to bigtable");
        return Ok(());
    }
    info!(
        "{} blocks to be uploaded to the bucket in the range ({}, {})",
        blocks_to_upload.len(),
        blocks_to_upload.first().unwrap(),
        blocks_to_upload.last().unwrap()
    );

    // Load the blocks out of blockstore in a separate thread to allow for concurrent block uploading
    let (_loader_threads, receiver): (Vec<_>, _) = {
        let exit = exit.clone();

        let (sender, receiver) = bounded(BLOCK_READ_AHEAD_DEPTH);

        let (slot_tx, slot_rx) = unbounded();

        let _ = blocks_to_upload.into_iter().for_each(|b| slot_tx.send(b).unwrap());
        drop(slot_tx);

        ((0..16).map(|_| {
            let blockstore = blockstore.clone();
            let sender = sender.clone();
            let slot_rx = slot_rx.clone();

            std::thread::spawn(move || {
                loop {
                    let slot = match slot_rx.recv() {
                        Ok(slot) => slot,
                        Err(_) => {
                            break;
                        }
                    };
                    let _ = match blockstore.get_rooted_block(slot, true) {
                        Ok(confirmed_block) => {
                            info!("fetched block {} from blockstore, sending to other thread", slot);
                            sender.send((slot, Some(confirmed_block)))
                            // Ok(())
                        }
                        Err(err) => {
                            warn!(
                                    "Failed to get load confirmed block from slot {}: {:?}",
                                    slot, err
                                );
                            sender.send((slot, None))
                        }
                    };
                }
            })
        }).collect(),
         receiver
        )
    };

    let mut failures = 0;
    use futures::stream::StreamExt;

    let mut stream =
        tokio_stream::iter(receiver.into_iter()).chunks(NUM_BLOCKS_TO_UPLOAD_IN_PARALLEL);

    while let Some(blocks) = stream.next().await {
        if exit.load(Ordering::Relaxed) {
            break;
        }

        let mut measure_upload = Measure::start("Upload");
        let mut num_blocks = blocks.len();
        info!("Preparing the next {} blocks for upload", num_blocks);

        let uploads = blocks.into_iter().filter_map(|(slot, block)| match block {
            None => {
                num_blocks -= 1;
                None
            }
            Some(confirmed_block) => {
                let bt = bigtable.clone();
                Some(runtime.spawn(async move { bt.upload_confirmed_block(slot, confirmed_block).await }))
            }
        });

        for result in futures::future::join_all(uploads).await {
            if result.is_err() {
                error!("upload_confirmed_block() failed: {:?}", result.err());
                failures += 1;
            }
        }

        measure_upload.stop();
        info!("{} for {} blocks", measure_upload, num_blocks);
    }

    measure.stop();
    info!("{}", measure);
    if failures > 0 {
        Err(format!("Incomplete upload, {} operations failed", failures).into())
    } else {
        Ok(())
    }
}
