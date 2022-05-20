//! Implements a transaction scheduler for the three types of transaction receiving pipelines:
//! - Normal transactions
//! - TPU vote transactions
//! - Gossip vote transactions

use {
    crate::{
        qos_service::QosService,
        transaction_scheduler::SchedulerError::TransactionCheckFailed,
        unprocessed_packet_batches::{
            self, DeserializedPacket, ImmutableDeserializedPacket, UnprocessedPacketBatches,
        },
    },
    crossbeam_channel::{select, unbounded, Receiver, Sender},
    solana_perf::packet::PacketBatch,
    solana_runtime::{accounts::AccountLocks, bank::Bank, cost_model::CostModel},
    solana_sdk::{
        feature_set,
        pubkey::Pubkey,
        transaction::{AddressLoader, SanitizedTransaction, TransactionError},
    },
    std::{
        collections::{hash_map::Entry, HashMap},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
    thiserror::Error,
};

#[derive(Debug)]
pub enum SchedulerMessage {
    RequestBatch {
        num_txs: usize,
        bank: Arc<Bank>,
    },
    Ping {
        id: usize,
    },
    ExecutedBatchUpdate {
        executed_transactions: Vec<SanitizedTransaction>,
        rescheduled_transactions: Vec<SanitizedTransaction>,
    },
}

#[derive(Debug)]
pub struct SchedulerRequest {
    msg: SchedulerMessage,
    response_sender: Sender<SchedulerResponse>,
}

#[derive(Clone, Debug)]
pub struct ScheduledBatch {
    sanitized_transactions: Vec<SanitizedTransaction>,
}

#[derive(Clone)]
pub struct Pong {
    id: usize,
}

#[derive(Clone)]
pub struct ExecutedBatchResponse {}

pub enum SchedulerResponse {
    ScheduledBatch(ScheduledBatch),
    Pong(Pong),
    ExecutedBatchResponse(ExecutedBatchResponse),
}

impl SchedulerResponse {
    fn pong(self) -> Pong {
        match self {
            SchedulerResponse::Pong(pong) => pong,
            _ => {
                unreachable!("invalid response expected");
            }
        }
    }

    fn scheduled_batch(self) -> ScheduledBatch {
        match self {
            SchedulerResponse::ScheduledBatch(batch) => batch,
            _ => {
                unreachable!("invalid response expected");
            }
        }
    }

    fn executed_batch_response(self) -> ExecutedBatchResponse {
        match self {
            SchedulerResponse::ExecutedBatchResponse(response) => response,
            _ => {
                unreachable!("invalid response expected");
            }
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

    #[error("account is blocked by higher paying: account {0}")]
    AccountBlocked(Pubkey),

    #[error("transaction check failed {0}")]
    TransactionCheckFailed(TransactionError),
}

pub type Result<T> = std::result::Result<T, SchedulerError>;

#[derive(Clone)]
pub struct TransactionSchedulerHandle {
    sender: Sender<SchedulerRequest>,
}

impl TransactionSchedulerHandle {
    pub fn new(sender: Sender<SchedulerRequest>) -> TransactionSchedulerHandle {
        TransactionSchedulerHandle { sender }
    }

    /// Requests a batch of num_txs transactions from one of the scheduler stages.
    pub fn request_batch(&self, num_txs: usize, bank: &Arc<Bank>) -> ScheduledBatch {
        Self::make_scheduler_request(
            &self.sender,
            SchedulerMessage::RequestBatch {
                num_txs,
                bank: bank.clone(),
            },
        )
        .scheduled_batch()
    }

    /// Ping-pong a scheduler stage
    pub fn send_ping(&self, id: usize) -> Pong {
        Self::make_scheduler_request(&self.sender, SchedulerMessage::Ping { id }).pong()
    }

    /// Send the scheduler an update on what was scheduled
    pub fn send_batch_execution_update(
        &self,
        executed_transactions: Vec<SanitizedTransaction>,
        rescheduled_transactions: Vec<SanitizedTransaction>,
    ) -> ExecutedBatchResponse {
        Self::make_scheduler_request(
            &self.sender,
            SchedulerMessage::ExecutedBatchUpdate {
                executed_transactions,
                rescheduled_transactions,
            },
        )
        .executed_batch_response()
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
        let _ = request_sender.send(request).unwrap();
        response_receiver.recv().unwrap()
    }
}

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

    /// Returns a handle to one of the schedulers
    pub fn get_handle(&self, scheduler_stage: SchedulerStage) -> TransactionSchedulerHandle {
        TransactionSchedulerHandle::new(self.get_sender_from_stage(scheduler_stage).clone())
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
        highest_wl_blocked_account_fees: &mut HashMap<Pubkey, u64>,
        highest_rl_blocked_account_fees: &mut HashMap<Pubkey, u64>,
        scheduled_accounts: &Arc<Mutex<AccountLocks>>,
    ) -> Result<SanitizedTransaction> {
        let sanitized_tx = Self::transaction_from_deserialized_packet(
            deserialized_packet.immutable_section(),
            &bank.feature_set,
            bank.vote_only_bank(),
            bank.as_ref(),
        )
        .ok_or_else(|| SchedulerError::InvalidSanitizedTransaction)?;

        let priority = deserialized_packet.immutable_section().priority();

        {
            let mut scheduled_accounts_l = scheduled_accounts.lock().unwrap();

            let account_locks = sanitized_tx
                .get_account_locks(&bank.feature_set)
                .map_err(|e| SchedulerError::InvalidTransactionFormat(e))?;

            trace!(
                "popped tx w/ priority: {}, readable: {:?}, writeable: {:?}",
                priority,
                account_locks.readonly,
                account_locks.writable
            );

            // Make sure that this transaction isn't blocked on another transaction that has a higher
            // fee for one of its accounts
            if let Err(e) = Self::check_accounts_not_blocked(
                &scheduled_accounts_l,
                highest_wl_blocked_account_fees,
                highest_rl_blocked_account_fees,
                &account_locks.writable,
                &account_locks.readonly,
            ) {
                Self::upsert_higher_fee_account_lock(
                    &account_locks.writable,
                    &account_locks.readonly,
                    highest_wl_blocked_account_fees,
                    highest_rl_blocked_account_fees,
                    priority,
                );
                return Err(e);
            }

            // already checked we can lock accounts in check_accounts_not_blocked
            Self::lock_accounts(
                &mut scheduled_accounts_l,
                &account_locks.writable,
                &account_locks.readonly,
            )
            .unwrap();

            // let mut sanitized_txs = vec![sanitized_tx];
            // let lock_results = vec![Ok(())];
            // let mut error_counters = TransactionErrorMetrics::default();
            // ensure that the tx
            // let tx_results = bank.check_transactions(
            //     &sanitized_txs,
            //     &lock_results,
            //     MAX_PROCESSING_AGE,
            //     &mut error_counters,
            // );
            // if let Err(e) = &tx_results.get(0).unwrap().0 {
            //     return Err(SchedulerError::TransactionCheckFailed(e.clone()));
            // }

            Ok(sanitized_tx)
        }
    }

    fn upsert_higher_fee_account_lock(
        writable_keys: &[&Pubkey],
        readonly_keys: &[&Pubkey],
        highest_wl_blocked_account_fees: &mut HashMap<Pubkey, u64>,
        highest_rl_blocked_account_fees: &mut HashMap<Pubkey, u64>,
        priority: u64,
    ) {
        for acc in readonly_keys {
            match highest_rl_blocked_account_fees.entry(**acc) {
                Entry::Occupied(mut e) => {
                    if priority > *e.get() {
                        // NOTE: this should never be the case!
                        e.insert(priority);
                    }
                }
                Entry::Vacant(e) => {
                    e.insert(priority);
                }
            }
        }

        for acc in writable_keys {
            match highest_wl_blocked_account_fees.entry(**acc) {
                Entry::Occupied(mut e) => {
                    if priority > *e.get() {
                        // NOTE: this should never be the case!
                        e.insert(priority);
                    }
                }
                Entry::Vacant(e) => {
                    e.insert(priority);
                }
            }
        }
    }

    fn check_accounts_not_blocked(
        account_locks: &AccountLocks,
        highest_wl_blocked_account_fees: &HashMap<Pubkey, u64>,
        highest_rl_blocked_account_fees: &HashMap<Pubkey, u64>,
        writable_keys: &[&Pubkey],
        readonly_keys: &[&Pubkey],
    ) -> Result<()> {
        // writes are blocked if there's a blocked read or write account already
        for acc in writable_keys {
            if highest_wl_blocked_account_fees.get(*acc).is_some()
                || highest_rl_blocked_account_fees.get(*acc).is_some()
            {
                trace!("write locked account blocked by another tx: {:?}", acc);
                return Err(SchedulerError::AccountBlocked(**acc));
            }
        }

        // reads are blocked if the account exists in write blocked state
        for acc in readonly_keys {
            if highest_wl_blocked_account_fees.get(*acc).is_some() {
                trace!("read locked account blocked by another tx: {:?}", acc);
                return Err(SchedulerError::AccountBlocked(**acc));
            }
        }

        // double check to make sure we can lock this against currently executed transactions and
        // accounts
        Self::can_lock_accounts(account_locks, writable_keys, readonly_keys)?;

        Ok(())
    }

    fn get_scheduled_batch(
        unprocessed_packets: &mut UnprocessedPacketBatches,
        scheduled_accounts: &Arc<Mutex<AccountLocks>>,
        num_txs: usize,
        bank: &Arc<Bank>,
    ) -> (Vec<SanitizedTransaction>, Vec<DeserializedPacket>) {
        let mut sanitized_transactions = Vec::new();
        let mut rescheduled_packets = Vec::new();

        // hashmap representing the highest fee of currently write-locked and read-locked blocked accounts
        // almost a pseudo AccountLocks but fees instead of hashset/read lock count
        let mut highest_wl_blocked_account_fees = HashMap::with_capacity(20_000);
        let mut highest_rl_blocked_account_fees = HashMap::with_capacity(20_000);

        while let Some(deserialized_packet) = unprocessed_packets.pop_max() {
            match Self::try_schedule(
                &deserialized_packet,
                bank,
                &mut highest_wl_blocked_account_fees,
                &mut highest_rl_blocked_account_fees,
                scheduled_accounts,
            ) {
                Ok(sanitized_tx) => {
                    sanitized_transactions.push(sanitized_tx);
                    if sanitized_transactions.len() >= num_txs {
                        break;
                    }
                }
                Err(e) => {
                    trace!("e: {:?}", e);
                    match e {
                        SchedulerError::InvalidSanitizedTransaction => {
                            // non-recoverable error, drop the packet
                            continue;
                        }
                        SchedulerError::InvalidTransactionFormat(_) => {
                            // non-recoverable error, drop the packet
                            continue;
                        }
                        TransactionCheckFailed(_) => {
                            // non-recoverable error, drop the packet
                            continue;
                        }
                        SchedulerError::AccountInUse => {
                            // need to reschedule
                            rescheduled_packets.push(deserialized_packet);
                        }
                        SchedulerError::AccountBlocked(_) => {
                            // need to reschedule
                            rescheduled_packets.push(deserialized_packet);
                        }
                    }
                }
            }
        }

        (sanitized_transactions, rescheduled_packets)
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
                trace!("SchedulerMessage::RequestBatch num_txs: {}", num_txs);
                let (sanitized_transactions, rescheduled_packets) = Self::get_scheduled_batch(
                    unprocessed_packets,
                    scheduled_accounts,
                    num_txs,
                    &bank,
                );
                trace!(
                    "sanitized_transactions num: {}, rescheduled_packets num: {}, unprocessed_packets num: {}",
                    sanitized_transactions.len(),
                    rescheduled_packets.len(),
                    unprocessed_packets.len()
                );

                let _ = response_sender
                    .send(SchedulerResponse::ScheduledBatch(ScheduledBatch {
                        sanitized_transactions,
                    }))
                    .unwrap();

                // push rescheduled back on
                for tx in rescheduled_packets {
                    unprocessed_packets.push(tx.clone());
                }
            }
            SchedulerMessage::Ping { id } => {
                let _ = response_sender
                    .send(SchedulerResponse::Pong(Pong { id }))
                    .unwrap();
            }
            SchedulerMessage::ExecutedBatchUpdate {
                executed_transactions,
                rescheduled_transactions,
            } => {
                {
                    // drop account locks
                    let mut account_locks = scheduled_accounts.lock().unwrap();
                    for tx in executed_transactions
                        .iter()
                        .chain(rescheduled_transactions.iter())
                    {
                        let tx_locks = tx.get_account_locks_unchecked();
                        trace!("unlocking locks: {:?}", tx_locks);
                        Self::drop_account_locks(
                            &mut account_locks,
                            &tx_locks.writable,
                            &tx_locks.readonly,
                        );
                    }
                    trace!("dropped account locks, account_locks: {:?}", account_locks);
                }
                // TODO (LB): reschedule transactions as packet
                // for tx in rescheduled_transactions {
                //     unprocessed_packets.push(tx.deser)
                // }
                let _ = response_sender
                    .send(SchedulerResponse::ExecutedBatchResponse(
                        ExecutedBatchResponse {},
                    ))
                    .unwrap();
            }
        }
    }

    fn can_lock_accounts(
        account_locks: &AccountLocks,
        writable_keys: &[&Pubkey],
        readonly_keys: &[&Pubkey],
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
        Ok(())
    }

    /// NOTE: this is copied from accounts.rs
    fn lock_accounts(
        account_locks: &mut AccountLocks,
        writable_keys: &[&Pubkey],
        readonly_keys: &[&Pubkey],
    ) -> Result<()> {
        Self::can_lock_accounts(account_locks, &writable_keys, &readonly_keys)?;

        for k in writable_keys {
            account_locks.write_locks.insert(**k);
        }

        for k in readonly_keys {
            if !account_locks.lock_readonly(k) {
                account_locks.insert_new_readonly(k);
            }
        }

        Ok(())
    }

    fn drop_account_locks(
        account_locks: &mut AccountLocks,
        writable_keys: &[&Pubkey],
        readonly_keys: &[&Pubkey],
    ) {
        for k in writable_keys {
            account_locks.unlock_write(k);
        }
        for k in readonly_keys {
            account_locks.unlock_readonly(k);
        }
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
        let mut num_added = 0;

        for packet_batch in packet_batches {
            let packet_indexes: Vec<usize> = packet_batch
                .packets
                .iter()
                .enumerate()
                .filter_map(|(idx, p)| if !p.meta.discard() { Some(idx) } else { None })
                .collect();
            number_of_dropped_packets += unprocessed_packets.insert_batch(
                unprocessed_packet_batches::deserialize_packets(&packet_batch, &packet_indexes),
            );
            num_added += packet_indexes.len();
        }
        trace!(
            "new packets: added {}, dropped {}, total: {}",
            num_added,
            number_of_dropped_packets,
            unprocessed_packets.len()
        );
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
    use {
        crate::transaction_scheduler::{SchedulerStage, TransactionScheduler},
        crossbeam_channel::{unbounded, Sender},
        solana_perf::packet::PacketBatch,
        solana_runtime::{bank::Bank, cost_model::CostModel},
        solana_sdk::{
            compute_budget::ComputeBudgetInstruction,
            hash::Hash,
            instruction::{AccountMeta, Instruction},
            packet::Packet,
            pubkey::Pubkey,
            signature::{Keypair, Signer},
            system_program,
            transaction::Transaction,
        },
        std::{
            collections::HashMap,
            sync::{
                atomic::{AtomicBool, Ordering},
                Arc, RwLock,
            },
        },
    };

    #[test]
    fn test_start_and_join_channel_dropped() {
        let (tx_sender, tx_receiver) = unbounded();
        let (tpu_vote_sender, tpu_vote_receiver) = unbounded();
        let (gossip_vote_sender, gossip_vote_receiver) = unbounded();
        let exit = Arc::new(AtomicBool::new(false));
        let cost_model = Arc::new(RwLock::new(CostModel::default()));

        let scheduler = TransactionScheduler::new(
            tx_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            exit,
            cost_model,
        );

        let tx_handle = scheduler.get_handle(SchedulerStage::Transactions);
        let gossip_vote_handle = scheduler.get_handle(SchedulerStage::GossipVotes);
        let tpu_vote_handle = scheduler.get_handle(SchedulerStage::TpuVotes);

        // check alive
        assert_eq!(tx_handle.send_ping(1).id, 1);
        assert_eq!(gossip_vote_handle.send_ping(2).id, 2);
        assert_eq!(tpu_vote_handle.send_ping(3).id, 3);

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

        let tx_handle = scheduler.get_handle(SchedulerStage::Transactions);
        let gossip_vote_handle = scheduler.get_handle(SchedulerStage::GossipVotes);
        let tpu_vote_handle = scheduler.get_handle(SchedulerStage::TpuVotes);

        // check alive
        assert_eq!(tx_handle.send_ping(1).id, 1);
        assert_eq!(gossip_vote_handle.send_ping(2).id, 2);
        assert_eq!(tpu_vote_handle.send_ping(3).id, 3);

        exit.store(true, Ordering::Relaxed);

        assert_matches!(scheduler.join(), Ok(()));
        drop(tx_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);
    }

    #[test]
    fn test_single_tx() {
        solana_logger::setup_with_default("trace");
        let (tx_sender, tx_receiver) = unbounded();
        let (tpu_vote_sender, tpu_vote_receiver) = unbounded();
        let (gossip_vote_sender, gossip_vote_receiver) = unbounded();
        let exit = Arc::new(AtomicBool::new(false));
        let cost_model = Arc::new(RwLock::new(CostModel::default()));
        let bank = Arc::new(Bank::default_for_tests());

        let accounts = get_random_accounts();

        let scheduler = TransactionScheduler::new(
            tx_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            exit.clone(),
            cost_model,
        );
        let tx_handle = scheduler.get_handle(SchedulerStage::Transactions);

        // main logic
        {
            let tx = get_tx(&[get_account_meta(&accounts, "A", true)], 200);

            send_transactions(&[&tx], &tx_sender);

            // should probably have gotten the packet by now
            let _ = tx_handle.send_ping(1);

            // make sure the requested batch is the single packet
            let mut batch = tx_handle.request_batch(1, &bank);
            assert_eq!(batch.sanitized_transactions.len(), 1);
            assert_eq!(
                batch.sanitized_transactions.pop().unwrap().signature(),
                &tx.signatures[0]
            );

            // make sure the batch is unlocked
            let _ = tx_handle.send_batch_execution_update(batch.sanitized_transactions, vec![]);
        }

        drop(tx_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);
        assert_matches!(scheduler.join(), Ok(()));
    }

    #[test]
    fn test_conflicting_transactions() {
        const BATCH_SIZE: usize = 128;
        solana_logger::setup_with_default("trace");
        let (tx_sender, tx_receiver) = unbounded();
        let (tpu_vote_sender, tpu_vote_receiver) = unbounded();
        let (gossip_vote_sender, gossip_vote_receiver) = unbounded();
        let exit = Arc::new(AtomicBool::new(false));
        let cost_model = Arc::new(RwLock::new(CostModel::default()));
        let bank = Arc::new(Bank::default_for_tests());

        let accounts = get_random_accounts();

        let scheduler = TransactionScheduler::new(
            tx_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            exit.clone(),
            cost_model,
        );
        let tx_handle = scheduler.get_handle(SchedulerStage::Transactions);

        // main logic
        {
            let tx1 = get_tx(&[get_account_meta(&accounts, "A", true)], 200);
            let tx2 = get_tx(&[get_account_meta(&accounts, "A", true)], 250);

            send_transactions(&[&tx1, &tx2], &tx_sender);

            // should probably have gotten the packet by now
            let _ = tx_handle.send_ping(1);

            // request two transactions, tx2 should be scheduled because it has higher fee for account A
            let first_batch = tx_handle.request_batch(BATCH_SIZE, &bank);
            assert_eq!(first_batch.sanitized_transactions.len(), 1);
            assert_eq!(
                first_batch
                    .sanitized_transactions
                    .get(0)
                    .unwrap()
                    .signature(),
                &tx2.signatures[0]
            );

            // attempt to request another transaction for schedule, won't schedule bc tx2 locked account A
            assert_eq!(
                tx_handle
                    .request_batch(BATCH_SIZE, &bank)
                    .sanitized_transactions
                    .len(),
                0
            );

            // make sure the tx2 is unlocked by sending it execution results of that batch
            let _ =
                tx_handle.send_batch_execution_update(first_batch.sanitized_transactions, vec![]);

            // tx1 should schedule now that tx2 is done executing
            let second_batch = tx_handle.request_batch(BATCH_SIZE, &bank);
            assert_eq!(second_batch.sanitized_transactions.len(), 1);
            assert_eq!(
                second_batch
                    .sanitized_transactions
                    .get(0)
                    .unwrap()
                    .signature(),
                &tx1.signatures[0]
            );
        }

        drop(tx_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);
        assert_matches!(scheduler.join(), Ok(()));
    }

    #[test]
    fn test_blocked_transactions() {
        const BATCH_SIZE: usize = 128;
        solana_logger::setup_with_default("trace");
        let (tx_sender, tx_receiver) = unbounded();
        let (tpu_vote_sender, tpu_vote_receiver) = unbounded();
        let (gossip_vote_sender, gossip_vote_receiver) = unbounded();
        let exit = Arc::new(AtomicBool::new(false));
        let cost_model = Arc::new(RwLock::new(CostModel::default()));
        let bank = Arc::new(Bank::default_for_tests());

        let accounts = get_random_accounts();

        let scheduler = TransactionScheduler::new(
            tx_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            exit.clone(),
            cost_model,
        );
        let tx_handle = scheduler.get_handle(SchedulerStage::Transactions);

        // main logic
        {
            // 300: A, B(RO)
            // 200:    B,     C (RO), D (RO)
            // 100:    B(R0), C (RO), D (RO), E
            // under previous logic, the previous batches would be [(300, 100), (200)] bc 300 and 100 can be parallelized
            // under this logic, we expect [(300), (200), (100)]
            // 200 has write priority on B
            let tx1 = get_tx(
                &[
                    get_account_meta(&accounts, "A", true),
                    get_account_meta(&accounts, "B", false),
                ],
                300,
            );
            let tx2 = get_tx(
                &[
                    get_account_meta(&accounts, "B", true),
                    get_account_meta(&accounts, "C", false),
                    get_account_meta(&accounts, "D", false),
                ],
                200,
            );
            let tx3 = get_tx(
                &[
                    get_account_meta(&accounts, "B", false),
                    get_account_meta(&accounts, "C", false),
                    get_account_meta(&accounts, "D", false),
                    get_account_meta(&accounts, "E", true),
                ],
                100,
            );

            send_transactions(&[&tx1, &tx2, &tx3], &tx_sender);

            // should probably have gotten the packet by now
            let _ = tx_handle.send_ping(1);

            let first_batch = tx_handle.request_batch(BATCH_SIZE, &bank);
            assert_eq!(first_batch.sanitized_transactions.len(), 1);
            assert_eq!(
                first_batch
                    .sanitized_transactions
                    .get(0)
                    .unwrap()
                    .signature(),
                &tx1.signatures[0]
            );

            // attempt to request another transaction for schedule, won't schedule bc tx2 locked account A
            assert_eq!(
                tx_handle
                    .request_batch(BATCH_SIZE, &bank)
                    .sanitized_transactions
                    .len(),
                0
            );

            let _ =
                tx_handle.send_batch_execution_update(first_batch.sanitized_transactions, vec![]);

            let second_batch = tx_handle.request_batch(BATCH_SIZE, &bank);
            assert_eq!(second_batch.sanitized_transactions.len(), 1);
            assert_eq!(
                second_batch
                    .sanitized_transactions
                    .get(0)
                    .unwrap()
                    .signature(),
                &tx2.signatures[0]
            );

            let _ =
                tx_handle.send_batch_execution_update(second_batch.sanitized_transactions, vec![]);

            let third_batch = tx_handle.request_batch(BATCH_SIZE, &bank);
            assert_eq!(third_batch.sanitized_transactions.len(), 1);
            assert_eq!(
                third_batch
                    .sanitized_transactions
                    .get(0)
                    .unwrap()
                    .signature(),
                &tx3.signatures[0]
            );
        }

        drop(tx_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);
        assert_matches!(scheduler.join(), Ok(()));
    }

    #[test]
    fn test_blocked_transactions_read_locked() {
        const BATCH_SIZE: usize = 128;
        solana_logger::setup_with_default("trace");
        let (tx_sender, tx_receiver) = unbounded();
        let (tpu_vote_sender, tpu_vote_receiver) = unbounded();
        let (gossip_vote_sender, gossip_vote_receiver) = unbounded();
        let exit = Arc::new(AtomicBool::new(false));
        let cost_model = Arc::new(RwLock::new(CostModel::default()));
        let bank = Arc::new(Bank::default_for_tests());

        let accounts = get_random_accounts();

        let scheduler = TransactionScheduler::new(
            tx_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            exit.clone(),
            cost_model,
        );
        let tx_handle = scheduler.get_handle(SchedulerStage::Transactions);

        // main logic
        {
            // 300: A, B(RO)
            // 200: A, B(R0), C (RO), D (RO)
            // 100:    B(R0), C (RO), D (RO), E
            // should schedule as [(300, 100), (200)] because while 200 is blocked on 300 bc account A, it read-locks B so the ordering
            // doesn't matter on 200, 100 or 100, 200
            let tx1 = get_tx(
                &[
                    get_account_meta(&accounts, "A", true),
                    get_account_meta(&accounts, "B", false),
                ],
                300,
            );
            let tx2 = get_tx(
                &[
                    get_account_meta(&accounts, "A", true),
                    get_account_meta(&accounts, "B", false),
                    get_account_meta(&accounts, "C", false),
                    get_account_meta(&accounts, "D", false),
                ],
                200,
            );
            let tx3 = get_tx(
                &[
                    get_account_meta(&accounts, "B", false),
                    get_account_meta(&accounts, "C", false),
                    get_account_meta(&accounts, "D", false),
                    get_account_meta(&accounts, "E", true),
                ],
                100,
            );

            send_transactions(&[&tx1, &tx2, &tx3], &tx_sender);

            // should probably have gotten the packet by now
            let _ = tx_handle.send_ping(1);

            let first_batch = tx_handle.request_batch(BATCH_SIZE, &bank);
            assert_eq!(first_batch.sanitized_transactions.len(), 2);
            assert_eq!(
                first_batch
                    .sanitized_transactions
                    .get(0)
                    .unwrap()
                    .signature(),
                &tx1.signatures[0]
            );
            assert_eq!(
                first_batch
                    .sanitized_transactions
                    .get(1)
                    .unwrap()
                    .signature(),
                &tx3.signatures[0]
            );

            // attempt to request another transaction for schedule, won't schedule bc tx2 locked account A
            assert_eq!(
                tx_handle
                    .request_batch(BATCH_SIZE, &bank)
                    .sanitized_transactions
                    .len(),
                0
            );

            let _ =
                tx_handle.send_batch_execution_update(first_batch.sanitized_transactions, vec![]);

            let second_batch = tx_handle.request_batch(BATCH_SIZE, &bank);
            assert_eq!(second_batch.sanitized_transactions.len(), 1);
            assert_eq!(
                second_batch
                    .sanitized_transactions
                    .get(0)
                    .unwrap()
                    .signature(),
                &tx2.signatures[0]
            );

            let _ =
                tx_handle.send_batch_execution_update(second_batch.sanitized_transactions, vec![]);
        }

        drop(tx_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);
        assert_matches!(scheduler.join(), Ok(()));
    }

    #[test]
    fn test_blocked_transactions_write_lock_released() {
        const BATCH_SIZE: usize = 128;
        solana_logger::setup_with_default("trace");
        let (tx_sender, tx_receiver) = unbounded();
        let (tpu_vote_sender, tpu_vote_receiver) = unbounded();
        let (gossip_vote_sender, gossip_vote_receiver) = unbounded();
        let exit = Arc::new(AtomicBool::new(false));
        let cost_model = Arc::new(RwLock::new(CostModel::default()));
        let bank = Arc::new(Bank::default_for_tests());

        let accounts = get_random_accounts();

        let scheduler = TransactionScheduler::new(
            tx_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            exit.clone(),
            cost_model,
        );
        let tx_handle = scheduler.get_handle(SchedulerStage::Transactions);

        // main logic
        {
            // 300: A, B(RO)
            // 200: A, B(R0), C (RO), D (RO)
            // 100:    B(R0), C (RO), D (RO), E
            //  50: A, B(R0), C (RO), D (RO), E
            // should schedule as [(300, 100), (200), (50)]
            let tx1 = get_tx(
                &[
                    get_account_meta(&accounts, "A", true),
                    get_account_meta(&accounts, "B", false),
                ],
                300,
            );
            let tx2 = get_tx(
                &[
                    get_account_meta(&accounts, "A", true),
                    get_account_meta(&accounts, "B", false),
                    get_account_meta(&accounts, "C", false),
                    get_account_meta(&accounts, "D", false),
                ],
                200,
            );
            let tx3 = get_tx(
                &[
                    get_account_meta(&accounts, "B", false),
                    get_account_meta(&accounts, "C", false),
                    get_account_meta(&accounts, "D", false),
                    get_account_meta(&accounts, "E", true),
                ],
                100,
            );
            let tx4 = get_tx(
                &[
                    get_account_meta(&accounts, "A", true),
                    get_account_meta(&accounts, "B", false),
                    get_account_meta(&accounts, "C", false),
                    get_account_meta(&accounts, "D", false),
                    get_account_meta(&accounts, "E", true),
                ],
                50,
            );

            send_transactions(&[&tx1, &tx2, &tx3, &tx4], &tx_sender);

            // should probably have gotten the packet by now
            let _ = tx_handle.send_ping(1);

            let first_batch = tx_handle.request_batch(BATCH_SIZE, &bank);
            assert_eq!(first_batch.sanitized_transactions.len(), 2);
            assert_eq!(
                first_batch
                    .sanitized_transactions
                    .get(0)
                    .unwrap()
                    .signature(),
                &tx1.signatures[0]
            );
            assert_eq!(
                first_batch
                    .sanitized_transactions
                    .get(1)
                    .unwrap()
                    .signature(),
                &tx3.signatures[0]
            );

            // attempt to request another transaction for schedule, won't schedule bc tx2 locked account A
            assert_eq!(
                tx_handle
                    .request_batch(BATCH_SIZE, &bank)
                    .sanitized_transactions
                    .len(),
                0
            );

            let _ =
                tx_handle.send_batch_execution_update(first_batch.sanitized_transactions, vec![]);

            let second_batch = tx_handle.request_batch(BATCH_SIZE, &bank);
            assert_eq!(second_batch.sanitized_transactions.len(), 1);
            assert_eq!(
                second_batch
                    .sanitized_transactions
                    .get(0)
                    .unwrap()
                    .signature(),
                &tx2.signatures[0]
            );

            let _ =
                tx_handle.send_batch_execution_update(second_batch.sanitized_transactions, vec![]);

            let third_batch = tx_handle.request_batch(BATCH_SIZE, &bank);
            assert_eq!(third_batch.sanitized_transactions.len(), 1);
            assert_eq!(
                third_batch
                    .sanitized_transactions
                    .get(0)
                    .unwrap()
                    .signature(),
                &tx4.signatures[0]
            );

            let _ =
                tx_handle.send_batch_execution_update(third_batch.sanitized_transactions, vec![]);
        }

        drop(tx_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);
        assert_matches!(scheduler.join(), Ok(()));
    }

    #[test]
    fn test_read_locked_blocks_write() {
        const BATCH_SIZE: usize = 128;
        solana_logger::setup_with_default("trace");
        let (tx_sender, tx_receiver) = unbounded();
        let (tpu_vote_sender, tpu_vote_receiver) = unbounded();
        let (gossip_vote_sender, gossip_vote_receiver) = unbounded();
        let exit = Arc::new(AtomicBool::new(false));
        let cost_model = Arc::new(RwLock::new(CostModel::default()));
        let bank = Arc::new(Bank::default_for_tests());

        let accounts = get_random_accounts();

        let scheduler = TransactionScheduler::new(
            tx_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            exit.clone(),
            cost_model,
        );
        let tx_handle = scheduler.get_handle(SchedulerStage::Transactions);

        {
            // 300: A, B, C
            // 200:    B, C, D(RO)
            // 100:        , D,     E
            //  50:                 E(RO), F
            // request schedule: 300
            // return 300
            // request schedule: 200
            // return 200
            // request schedule: 100
            // return schedule
            // request schedule: 50
            // return schedule
            let tx1 = get_tx(
                &[
                    get_account_meta(&accounts, "A", true),
                    get_account_meta(&accounts, "B", true),
                    get_account_meta(&accounts, "C", true),
                ],
                300,
            );
            let tx2 = get_tx(
                &[
                    get_account_meta(&accounts, "B", true),
                    get_account_meta(&accounts, "C", true),
                    get_account_meta(&accounts, "D", false),
                ],
                200,
            );
            let tx3 = get_tx(
                &[
                    get_account_meta(&accounts, "D", true),
                    get_account_meta(&accounts, "E", true),
                ],
                100,
            );
            let tx4 = get_tx(
                &[
                    get_account_meta(&accounts, "E", false),
                    get_account_meta(&accounts, "F", true),
                ],
                50,
            );

            send_transactions(&[&tx1, &tx2, &tx3, &tx4], &tx_sender);

            let first_batch = tx_handle.request_batch(BATCH_SIZE, &bank);
            assert_eq!(first_batch.sanitized_transactions.len(), 1);
            assert_eq!(
                first_batch
                    .sanitized_transactions
                    .get(0)
                    .unwrap()
                    .signature(),
                &tx1.signatures[0]
            );
            let _ =
                tx_handle.send_batch_execution_update(first_batch.sanitized_transactions, vec![]);

            let second_batch = tx_handle.request_batch(BATCH_SIZE, &bank);
            assert_eq!(second_batch.sanitized_transactions.len(), 1);
            assert_eq!(
                second_batch
                    .sanitized_transactions
                    .get(0)
                    .unwrap()
                    .signature(),
                &tx2.signatures[0]
            );
            let _ =
                tx_handle.send_batch_execution_update(second_batch.sanitized_transactions, vec![]);

            let third_batch = tx_handle.request_batch(BATCH_SIZE, &bank);
            assert_eq!(third_batch.sanitized_transactions.len(), 1);
            assert_eq!(
                third_batch
                    .sanitized_transactions
                    .get(0)
                    .unwrap()
                    .signature(),
                &tx3.signatures[0]
            );
            let _ =
                tx_handle.send_batch_execution_update(third_batch.sanitized_transactions, vec![]);

            let fourth_batch = tx_handle.request_batch(BATCH_SIZE, &bank);
            assert_eq!(fourth_batch.sanitized_transactions.len(), 1);
            assert_eq!(
                fourth_batch
                    .sanitized_transactions
                    .get(0)
                    .unwrap()
                    .signature(),
                &tx4.signatures[0]
            );
            let _ =
                tx_handle.send_batch_execution_update(fourth_batch.sanitized_transactions, vec![]);
        }

        drop(tx_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);
        assert_matches!(scheduler.join(), Ok(()));
    }

    #[test]
    fn test_read_locked_does_not_block_red() {
        const BATCH_SIZE: usize = 128;
        solana_logger::setup_with_default("trace");
        let (tx_sender, tx_receiver) = unbounded();
        let (tpu_vote_sender, tpu_vote_receiver) = unbounded();
        let (gossip_vote_sender, gossip_vote_receiver) = unbounded();
        let exit = Arc::new(AtomicBool::new(false));
        let cost_model = Arc::new(RwLock::new(CostModel::default()));
        let bank = Arc::new(Bank::default_for_tests());

        let accounts = get_random_accounts();

        let scheduler = TransactionScheduler::new(
            tx_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            exit.clone(),
            cost_model,
        );
        let tx_handle = scheduler.get_handle(SchedulerStage::Transactions);

        {
            // 300: A, B, C
            // 200:    B, C, D(RO)
            // 100:          D(RO), E
            //  50:                 E(RO), F
            // request schedule: 300, 100
            // return 300, 100
            // request schedule: 200, 50
            // return 200, 50
            let tx1 = get_tx(
                &[
                    get_account_meta(&accounts, "A", true),
                    get_account_meta(&accounts, "B", true),
                    get_account_meta(&accounts, "C", true),
                ],
                300,
            );
            let tx2 = get_tx(
                &[
                    get_account_meta(&accounts, "B", true),
                    get_account_meta(&accounts, "C", true),
                    get_account_meta(&accounts, "D", false),
                ],
                200,
            );
            let tx3 = get_tx(
                &[
                    get_account_meta(&accounts, "D", false),
                    get_account_meta(&accounts, "E", true),
                ],
                100,
            );
            let tx4 = get_tx(
                &[
                    get_account_meta(&accounts, "E", false),
                    get_account_meta(&accounts, "F", true),
                ],
                50,
            );

            send_transactions(&[&tx1, &tx2, &tx3, &tx4], &tx_sender);

            let first_batch = tx_handle.request_batch(BATCH_SIZE, &bank);
            assert_eq!(first_batch.sanitized_transactions.len(), 2);
            assert_eq!(
                first_batch
                    .sanitized_transactions
                    .get(0)
                    .unwrap()
                    .signature(),
                &tx1.signatures[0]
            );
            assert_eq!(
                first_batch
                    .sanitized_transactions
                    .get(1)
                    .unwrap()
                    .signature(),
                &tx3.signatures[0]
            );
            let _ =
                tx_handle.send_batch_execution_update(first_batch.sanitized_transactions, vec![]);

            let second_batch = tx_handle.request_batch(BATCH_SIZE, &bank);
            assert_eq!(second_batch.sanitized_transactions.len(), 2);
            assert_eq!(
                second_batch
                    .sanitized_transactions
                    .get(0)
                    .unwrap()
                    .signature(),
                &tx2.signatures[0]
            );
            assert_eq!(
                second_batch
                    .sanitized_transactions
                    .get(1)
                    .unwrap()
                    .signature(),
                &tx4.signatures[0]
            );
            let _ =
                tx_handle.send_batch_execution_update(second_batch.sanitized_transactions, vec![]);
        }

        drop(tx_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);
        assert_matches!(scheduler.join(), Ok(()));
    }

    // TODO: need to think of a clear and concise way to test this scheduler!

    // TODO some other tests:
    // 300: A(R), B, C
    // 250: A(R), B, C
    // 200: A(R), D, E
    // request schedule: (300, 200)
    // return (300, 200)
    // request schedule: (250)
    // return 250
    // -----------------------
    // 300: A(R), B, C,
    // 250: A,    B, C,
    // 200: A(R),       D, E
    // request schedule: 300
    // return 300
    // request schedule: 250
    // return 250
    // request schedule: 200
    // return 200
    // -----------------------
    // 300: A(R), B, C,
    // 250: A,    B, C,
    // 200: A(R),       D, E
    // request schedule: 300
    // insert:
    // 275: A(R),       D(R)
    // request schedule: 275
    // request schedule: []
    // return 300
    // request schedule: 250
    // return 275
    // return 250
    // request schedule: 200
    // return 200

    /// Converts transactions to packets and sends them to scheduler over channel
    fn send_transactions(txs: &[&Transaction], tx_sender: &Sender<Vec<PacketBatch>>) {
        let packets = txs
            .into_iter()
            .map(|tx| Packet::from_data(None, *tx).unwrap());
        tx_sender
            .send(vec![PacketBatch::new(packets.collect())])
            .unwrap();
    }

    /// Builds some arbitrary transaction with given AccountMetas and prioritization fee
    fn get_tx(account_metas: &[AccountMeta], micro_lamports_fee_per_cu: u64) -> Transaction {
        let kp = Keypair::new();
        Transaction::new_signed_with_payer(
            &[
                ComputeBudgetInstruction::set_compute_unit_price(micro_lamports_fee_per_cu),
                Instruction::new_with_bytes(system_program::id(), &[0], account_metas.to_vec()),
            ],
            Some(&kp.pubkey()),
            &[&kp],
            Hash::default(),
        )
    }

    /// Gets random accounts w/ alphabetical access for easy testing.
    fn get_random_accounts() -> HashMap<&'static str, Pubkey> {
        HashMap::from([
            ("A", Pubkey::new_unique()),
            ("B", Pubkey::new_unique()),
            ("C", Pubkey::new_unique()),
            ("D", Pubkey::new_unique()),
            ("E", Pubkey::new_unique()),
            ("F", Pubkey::new_unique()),
            ("G", Pubkey::new_unique()),
            ("H", Pubkey::new_unique()),
            ("I", Pubkey::new_unique()),
            ("J", Pubkey::new_unique()),
            ("K", Pubkey::new_unique()),
            ("L", Pubkey::new_unique()),
            ("M", Pubkey::new_unique()),
            ("N", Pubkey::new_unique()),
            ("O", Pubkey::new_unique()),
            ("P", Pubkey::new_unique()),
            ("Q", Pubkey::new_unique()),
            ("R", Pubkey::new_unique()),
            ("S", Pubkey::new_unique()),
            ("T", Pubkey::new_unique()),
            ("U", Pubkey::new_unique()),
            ("V", Pubkey::new_unique()),
            ("W", Pubkey::new_unique()),
            ("X", Pubkey::new_unique()),
            ("Y", Pubkey::new_unique()),
            ("Z", Pubkey::new_unique()),
        ])
    }

    /// Returns pubkey from map created above
    fn get_pubkey(map: &HashMap<&str, Pubkey>, char: &str) -> Pubkey {
        return map.get(char).unwrap().clone();
    }

    /// Returns AccountMeta with pubkey from account above and writeable flag set
    fn get_account_meta(map: &HashMap<&str, Pubkey>, char: &str, is_writable: bool) -> AccountMeta {
        if is_writable {
            AccountMeta::new(get_pubkey(map, char), false)
        } else {
            AccountMeta::new_readonly(get_pubkey(map, char), false)
        }
    }
}
