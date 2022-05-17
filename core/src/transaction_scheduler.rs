//! Implements a transaction scheduler for the three types of transaction receiving pipelines:
//! - Normal transactions
//! - TPU vote transactions
//! - Gossip vote transactions

use crate::qos_service::QosService;
use crate::unprocessed_packet_batches::{DeserializedPacket, ImmutableDeserializedPacket};
use solana_runtime::accounts::AccountLocks;
use solana_runtime::bank::Bank;
use solana_runtime::cost_model::CostModel;
use solana_sdk::feature_set;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::transaction::{AddressLoader, SanitizedTransaction, TransactionError};
use std::sync::{Arc, Mutex, RwLock};
use thiserror::Error;
use {
    crate::unprocessed_packet_batches::{self, UnprocessedPacketBatches},
    crossbeam_channel::{select, unbounded, Receiver, Sender},
    solana_perf::packet::PacketBatch,
    std::sync::atomic::{AtomicBool, Ordering},
    std::time::Duration,
    std::{
        thread,
        thread::{Builder, JoinHandle},
    },
};

pub enum SchedulerMessage {
    RequestBatch { num_txs: usize, bank: Arc<Bank> },
    Ping { id: usize },
}

pub struct SchedulerRequest {
    msg: SchedulerMessage,
    response_sender: Sender<SchedulerResponse>,
}

#[derive(Clone)]
pub struct ScheduledBatch {}

#[derive(Clone)]
pub struct Pong {
    id: usize,
}

pub enum SchedulerResponse {
    ScheduledBatch(ScheduledBatch),
    Pong(Pong),
}

impl SchedulerResponse {
    fn pong(self) -> Pong {
        match self {
            SchedulerResponse::ScheduledBatch { .. } => {
                unreachable!("invalid response expected");
            }
            SchedulerResponse::Pong(pong) => pong,
        }
    }
}

pub enum SchedulerStage {
    // normal transactions
    Transactions,
    // votes coming in on tpu port
    TpuVotes,
    // gossip votes
    GossipVotes,
}

#[derive(Error, Debug, PartialEq, Eq, Clone)]
pub enum SchedulerError {
    #[error("invalid sanitized transaction")]
    InvalidSanitizedTransaction,

    #[error("irrecoverable transaction format error: {0}")]
    InvalidTransactionFormat(TransactionError),

    #[error("account in use")]
    AccountInUse,
}

pub type Result<T> = std::result::Result<T, SchedulerError>;

pub struct TransactionScheduler {
    tx_request_handler_thread: JoinHandle<()>,
    tx_scheduler_request_sender: Sender<SchedulerRequest>,

    tpu_vote_request_handler_thread: JoinHandle<()>,
    tpu_vote_scheduler_request_sender: Sender<SchedulerRequest>,

    gossip_vote_request_handler_thread: JoinHandle<()>,
    gossip_vote_scheduler_request_sender: Sender<SchedulerRequest>,
}

impl TransactionScheduler {
    /// Creates a thread for each type of transaction and a handle to the event loop.
    pub fn new(
        verified_receiver: Receiver<Vec<PacketBatch>>,
        verified_tpu_vote_packets_receiver: Receiver<Vec<PacketBatch>>,
        verified_gossip_vote_packets_receiver: Receiver<Vec<PacketBatch>>,
        exit: Arc<AtomicBool>,
        cost_model: Arc<RwLock<CostModel>>,
    ) -> Self {
        // keep track of account locking here too? TODO (LB): remove
        let scheduled_accounts = Arc::new(Mutex::new(AccountLocks::default()));

        let (tx_scheduler_request_sender, tx_scheduler_request_receiver) = unbounded();
        let tx_request_handler_thread = Self::start_event_loop(
            "tx_scheduler_insertion_thread",
            tx_scheduler_request_receiver,
            verified_receiver,
            scheduled_accounts.clone(),
            QosService::new(cost_model.clone(), 0),
            &exit,
        );

        let (tpu_vote_scheduler_request_sender, tpu_vote_scheduler_request_receiver) = unbounded();
        let tpu_vote_request_handler_thread = Self::start_event_loop(
            "tpu_vote_scheduler_tx_insertion_thread",
            tpu_vote_scheduler_request_receiver,
            verified_tpu_vote_packets_receiver,
            scheduled_accounts.clone(),
            QosService::new(cost_model.clone(), 1),
            &exit,
        );

        let (gossip_vote_scheduler_request_sender, gossip_vote_scheduler_request_receiver) =
            unbounded();
        let gossip_vote_request_handler_thread = Self::start_event_loop(
            "gossip_vote_scheduler_tx_insertion_thread",
            gossip_vote_scheduler_request_receiver,
            verified_gossip_vote_packets_receiver,
            scheduled_accounts.clone(),
            QosService::new(cost_model.clone(), 2),
            &exit,
        );

        TransactionScheduler {
            tx_request_handler_thread,
            tx_scheduler_request_sender,
            tpu_vote_request_handler_thread,
            tpu_vote_scheduler_request_sender,
            gossip_vote_request_handler_thread,
            gossip_vote_scheduler_request_sender,
        }
    }

    // ***************************************************************
    // Client methods
    // ***************************************************************

    /// Requests a batch of num_txs transactions from one of the scheduler stages.
    pub fn request_batch(
        &self,
        stage: SchedulerStage,
        num_txs: usize,
        bank: Arc<Bank>,
    ) -> SchedulerResponse {
        Self::make_scheduler_request(
            self.get_sender_from_stage(stage),
            SchedulerMessage::RequestBatch { num_txs, bank },
        )
    }

    /// Ping-pong a scheduler stage
    pub fn send_ping(&self, stage: SchedulerStage, id: usize) -> SchedulerResponse {
        Self::make_scheduler_request(
            self.get_sender_from_stage(stage),
            SchedulerMessage::Ping { id },
        )
    }

    /// Sends a scheduler request and blocks on waiting for a response
    fn make_scheduler_request(
        request_sender: &Sender<SchedulerRequest>,
        msg: SchedulerMessage,
    ) -> SchedulerResponse {
        let (response_sender, response_receiver) = unbounded();
        let request = SchedulerRequest {
            msg,
            response_sender,
        };
        // TODO (LB): don't unwrap
        let _ = request_sender.send(request).unwrap();
        response_receiver.recv().unwrap()
    }

    /// Clean up the threads
    pub fn join(self) -> thread::Result<()> {
        self.tx_request_handler_thread.join()?;
        self.tpu_vote_request_handler_thread.join()?;
        self.gossip_vote_request_handler_thread.join()?;
        Ok(())
    }

    // ***************************************************************
    // Internal logic
    // ***************************************************************

    /// The event loop has two main responsibilities:
    /// 1. Handle incoming packets and prioritization around them.
    /// 2. Serve scheduler requests and return responses.
    fn start_event_loop(
        t_name: &str,
        scheduler_request_receiver: Receiver<SchedulerRequest>,
        packet_receiver: Receiver<Vec<PacketBatch>>,
        scheduled_accounts: Arc<Mutex<AccountLocks>>,
        _qos_service: QosService,
        exit: &Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let exit = exit.clone();
        Builder::new()
            .name(t_name.to_string())
            .spawn(move || {
                let mut unprocessed_packet_batches = UnprocessedPacketBatches::with_capacity(700_000);

                loop {
                    select! {
                        recv(packet_receiver) -> maybe_packet_batches => {
                            match maybe_packet_batches {
                                Ok(packet_batches) => {
                                    Self::handle_packet_batches(&mut unprocessed_packet_batches, packet_batches);
                                }
                                Err(_) => {
                                    break;
                                }
                            }
                        }
                        recv(scheduler_request_receiver) -> maybe_batch_request => {
                            match maybe_batch_request {
                                Ok(batch_request) => {
                                    Self::handle_scheduler_request(&mut unprocessed_packet_batches, &scheduled_accounts, batch_request);
                                }
                                Err(_) => {
                                    break;
                                }
                            }
                        }
                        default(Duration::from_millis(100)) => {
                            if exit.load(Ordering::Relaxed) {
                                break;
                            }
                        }
                    }
                }
            })
            .unwrap()
    }

    /// Attempts to schedule a transaction to be executed.
    fn try_schedule(
        deserialized_packet: &DeserializedPacket,
        bank: &Arc<Bank>,
        scheduled_accounts: &Arc<Mutex<AccountLocks>>,
    ) -> Result<SanitizedTransaction> {
        let sanitized_tx = Self::transaction_from_deserialized_packet(
            deserialized_packet.immutable_section(),
            &bank.feature_set,
            bank.vote_only_bank(),
            bank.as_ref(),
        )
        .ok_or_else(|| SchedulerError::InvalidSanitizedTransaction)?;

        // should evaluate these in the order of speed and likelihood
        // check to make sure it doesn't have any lock contention
        // check to make sure it can be scheduled according to the priority fee mechanism

        {
            let mut scheduled_accounts_l = scheduled_accounts.lock().unwrap();
            // NOTE: as soon at these accounts are locked, must ensure that they're unlocked or
            // those accounts will never get scheduled
            let account_locks = sanitized_tx
                .get_account_locks(&bank.feature_set)
                .map_err(|e| SchedulerError::InvalidTransactionFormat(e))?;
            Self::lock_accounts(
                &mut scheduled_accounts_l,
                account_locks.writable,
                account_locks.readonly,
            )?;
        }

        Ok(sanitized_tx)
    }

    /// Handles scheduler requests and sends back a response over the channel
    fn handle_scheduler_request(
        unprocessed_packets: &mut UnprocessedPacketBatches,
        scheduled_accounts: &Arc<Mutex<AccountLocks>>,
        scheduler_request: SchedulerRequest,
    ) {
        let response_sender = scheduler_request.response_sender;

        match scheduler_request.msg {
            SchedulerMessage::RequestBatch { num_txs, bank } => {
                let mut rescheduled_txs = vec![];
                while let Some(deserialized_packet) = unprocessed_packets.pop_max() {
                    match Self::try_schedule(&deserialized_packet, &bank, scheduled_accounts) {
                        Ok(_) => {}
                        Err(SchedulerError::InvalidSanitizedTransaction) => {
                            // non-recoverable error, drop the packet
                            continue;
                        }
                        Err(SchedulerError::InvalidTransactionFormat(_)) => {
                            // non-recoverable error, drop the packet
                            continue;
                        }
                        Err(SchedulerError::AccountInUse) => {
                            // need to reschedule
                            rescheduled_txs.push(deserialized_packet);
                        }
                    }
                }

                // TODO send batch

                rescheduled_txs.into_iter().for_each(|tx| {
                    unprocessed_packets.push(tx);
                });
            }
            SchedulerMessage::Ping { id } => {
                let _ = response_sender
                    .send(SchedulerResponse::Pong(Pong { id }))
                    .unwrap();
            }
        }
    }

    /// NOTE: this is copied from accounts.rs
    fn lock_accounts(
        account_locks: &mut AccountLocks,
        writable_keys: Vec<&Pubkey>,
        readonly_keys: Vec<&Pubkey>,
    ) -> Result<()> {
        for k in writable_keys.iter() {
            if account_locks.is_locked_write(k) || account_locks.is_locked_readonly(k) {
                debug!("Writable account in use: {:?}", k);
                return Err(SchedulerError::AccountInUse);
            }
        }
        for k in readonly_keys.iter() {
            if account_locks.is_locked_write(k) {
                debug!("Read-only account in use: {:?}", k);
                return Err(SchedulerError::AccountInUse);
            }
        }

        for k in writable_keys {
            account_locks.write_locks.insert(*k);
        }

        for k in readonly_keys {
            if !account_locks.lock_readonly(k) {
                account_locks.insert_new_readonly(k);
            }
        }

        Ok(())
    }

    fn transaction_from_deserialized_packet(
        deserialized_packet: &ImmutableDeserializedPacket,
        feature_set: &Arc<feature_set::FeatureSet>,
        votes_only: bool,
        address_loader: impl AddressLoader,
    ) -> Option<SanitizedTransaction> {
        if votes_only && !deserialized_packet.is_simple_vote() {
            return None;
        }

        let tx = SanitizedTransaction::try_new(
            deserialized_packet.transaction().clone(),
            *deserialized_packet.message_hash(),
            deserialized_packet.is_simple_vote(),
            address_loader,
        )
        .ok()?;
        tx.verify_precompiles(feature_set).ok()?;
        Some(tx)
    }

    fn handle_packet_batches(
        unprocessed_packets: &mut UnprocessedPacketBatches,
        packet_batches: Vec<PacketBatch>,
    ) {
        let mut number_of_dropped_packets = 0;
        for packet_batch in packet_batches {
            let packet_indexes: Vec<usize> = packet_batch
                .packets
                .iter()
                .enumerate()
                .filter_map(|(idx, p)| if !p.meta.discard() { Some(idx) } else { None })
                .collect();
            number_of_dropped_packets +=
                unprocessed_packets.insert_batch(unprocessed_packet_batches::deserialize_packets(
                    &packet_batch,
                    &packet_indexes,
                    None,
                ));
        }
        info!("dropped {} transactions", number_of_dropped_packets);
    }

    /// Returns sending side of the channel given the scheduler stage
    fn get_sender_from_stage(&self, stage: SchedulerStage) -> &Sender<SchedulerRequest> {
        match stage {
            SchedulerStage::Transactions => &self.tx_scheduler_request_sender,
            SchedulerStage::TpuVotes => &self.tpu_vote_scheduler_request_sender,
            SchedulerStage::GossipVotes => &self.gossip_vote_scheduler_request_sender,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::transaction_scheduler::{SchedulerStage, TransactionScheduler};
    use crossbeam_channel::unbounded;
    use solana_runtime::cost_model::CostModel;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, RwLock};
    use std::time::Instant;

    #[test]
    fn test_start_and_join_channel_dropped() {
        let (tx_sender, tx_receiver) = unbounded();
        let (tpu_vote_sender, tpu_vote_receiver) = unbounded();
        let (gossip_vote_sender, gossip_vote_receiver) = unbounded();
        let exit = Arc::new(AtomicBool::new(false));

        let scheduler =
            TransactionScheduler::new(tx_receiver, tpu_vote_receiver, gossip_vote_receiver, exit);

        // check alive
        assert_eq!(
            scheduler
                .send_ping(SchedulerStage::Transactions, 1)
                .pong()
                .id,
            1
        );
        assert_eq!(
            scheduler
                .send_ping(SchedulerStage::GossipVotes, 2)
                .pong()
                .id,
            2
        );
        assert_eq!(
            scheduler.send_ping(SchedulerStage::TpuVotes, 3).pong().id,
            3
        );

        drop(tx_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);

        assert_matches!(scheduler.join(), Ok(()));
    }

    #[test]
    fn test_start_and_join_channel_exit_signal() {
        let (tx_sender, tx_receiver) = unbounded();
        let (tpu_vote_sender, tpu_vote_receiver) = unbounded();
        let (gossip_vote_sender, gossip_vote_receiver) = unbounded();
        let exit = Arc::new(AtomicBool::new(false));

        let cost_model = Arc::new(RwLock::new(CostModel::default()));

        let scheduler = TransactionScheduler::new(
            tx_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            exit.clone(),
            cost_model,
        );

        // check alive
        assert_eq!(
            scheduler
                .send_ping(SchedulerStage::Transactions, 1)
                .pong()
                .id,
            1
        );
        assert_eq!(
            scheduler
                .send_ping(SchedulerStage::GossipVotes, 2)
                .pong()
                .id,
            2
        );
        assert_eq!(
            scheduler.send_ping(SchedulerStage::TpuVotes, 3).pong().id,
            3
        );

        exit.store(true, Ordering::Relaxed);

        assert_matches!(scheduler.join(), Ok(()));
        drop(tx_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);
    }

    #[test]
    fn test_duration() {
        solana_logger::setup_with_default("info");

        let (tx_sender, tx_receiver) = unbounded();
        let (tpu_vote_sender, tpu_vote_receiver) = unbounded();
        let (gossip_vote_sender, gossip_vote_receiver) = unbounded();
        let exit = Arc::new(AtomicBool::new(false));

        let scheduler = TransactionScheduler::new(
            tx_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            exit.clone(),
            cost_model,
        );

        // make sure thread is awake by pinging and waiting for response
        assert_eq!(
            scheduler
                .send_ping(SchedulerStage::Transactions, 1)
                .pong()
                .id,
            1
        );

        for _ in 0..1000 {
            // now test latency
            let now = Instant::now();
            let _ = scheduler.send_ping(SchedulerStage::Transactions, 1).pong();
            let elapsed = now.elapsed();
            info!("elapsed: {:?}", elapsed);
        }

        drop(tx_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);
        assert_matches!(scheduler.join(), Ok(()));
    }
}
