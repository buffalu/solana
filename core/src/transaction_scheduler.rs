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
        transaction::{
            AddressLoader, SanitizedTransaction, TransactionAccountLocks, TransactionError,
        },
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
    ) -> ScheduledBatch {
        Self::make_scheduler_request(
            self.get_sender_from_stage(stage),
            SchedulerMessage::RequestBatch { num_txs, bank },
        )
        .scheduled_batch()
    }

    /// Ping-pong a scheduler stage
    pub fn send_ping(&self, stage: SchedulerStage, id: usize) -> Pong {
        Self::make_scheduler_request(
            self.get_sender_from_stage(stage),
            SchedulerMessage::Ping { id },
        )
        .pong()
    }

    /// Send the scheduler an update on what was scheduled
    pub fn send_batch_execution_update(
        &self,
        stage: SchedulerStage,
        executed_transactions: Vec<SanitizedTransaction>,
        rescheduled_transactions: Vec<SanitizedTransaction>,
    ) -> ExecutedBatchResponse {
        Self::make_scheduler_request(
            self.get_sender_from_stage(stage),
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

                // hashmap representing the highest fee of currently blocked accounts
                let mut highest_blocked_account_fees = HashMap::with_capacity(20_000);

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
                                    // rescheduled txs might get big, so allocated outside of this fn
                                    Self::handle_scheduler_request(&mut unprocessed_packet_batches, &scheduled_accounts, batch_request, &mut highest_blocked_account_fees);
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
        blocked_account_fees: &mut HashMap<Pubkey, u64>,
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
        // check QoS too!

        let fee_per_cu = deserialized_packet.immutable_section().fee_per_cu();

        {
            let mut scheduled_accounts_l = scheduled_accounts.lock().unwrap();
            // NOTE: as soon at these accounts are locked, must ensure that they're unlocked or
            // those accounts will never get scheduled
            // TODO: clone hacky AF
            // TODO: might wanna rearrange some of this to avoid long lock time.
            let account_locks = sanitized_tx
                .get_account_locks(&bank.feature_set)
                .map_err(|e| SchedulerError::InvalidTransactionFormat(e))?;

            trace!("blocked_account_fees: {:?}", blocked_account_fees);

            trace!(
                "tx fee: {}, readable: {:?}, writeable: {:?}",
                fee_per_cu,
                account_locks.readonly,
                account_locks.writable
            );

            // Make sure that this transaction isn't blocked on another transaction that has a higher
            // fee for one of its accounts
            if let Err(e) = Self::check_higher_payer_than_blocked_accounts(
                blocked_account_fees,
                &account_locks.writable,
                &account_locks.readonly,
                fee_per_cu,
            ) {
                trace!(
                    "account is blocked by another blocked account, upsert account fee block: {}",
                    fee_per_cu
                );
                Self::upsert_higher_fee_account_lock(
                    &account_locks,
                    blocked_account_fees,
                    fee_per_cu,
                );
                return Err(e);
            }

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

            // Make sure this transaction can be scheduled in a parallel manner.
            if let Err(e) = Self::lock_accounts(
                &mut scheduled_accounts_l,
                &account_locks.writable,
                &account_locks.readonly,
            ) {
                trace!(
                    "account is blocked by an executing tx, upsert account fee block: {}",
                    fee_per_cu
                );
                Self::upsert_higher_fee_account_lock(
                    &account_locks,
                    blocked_account_fees,
                    fee_per_cu,
                );
                return Err(e);
            }

            // Assuming it can be scheduled in a parallel manner and everything else ok, remove any fees
            for acc in account_locks
                .writable
                .iter()
                .chain(account_locks.readonly.iter())
            {
                match blocked_account_fees.entry(**acc) {
                    Entry::Occupied(blocked_fee) => {
                        if fee_per_cu >= *blocked_fee.get() {
                            blocked_fee.remove();
                        }
                    }
                    Entry::Vacant(_) => {}
                }
            }
            trace!("updated blocked_account_fees: {:?}", blocked_account_fees);

            Ok(sanitized_tx)
        }
    }

    /// upserts higher blocked fees
    fn upsert_higher_fee_account_lock(
        account_locks: &TransactionAccountLocks,
        blocked_account_fees: &mut HashMap<Pubkey, u64>,
        fee_per_cu: u64,
    ) {
        for acc in account_locks
            .writable
            .iter()
            .chain(account_locks.readonly.iter())
        {
            match blocked_account_fees.entry(**acc) {
                Entry::Occupied(mut e) => {
                    if fee_per_cu > *e.get() {
                        e.insert(fee_per_cu);
                    }
                }
                Entry::Vacant(e) => {
                    e.insert(fee_per_cu);
                }
            }
        }
    }

    fn check_higher_payer_than_blocked_accounts(
        blocked_account_fees: &mut HashMap<Pubkey, u64>,
        writable_keys: &[&Pubkey],
        readonly_keys: &[&Pubkey],
        fee_per_cu: u64,
    ) -> Result<()> {
        for acc in writable_keys.iter().chain(readonly_keys.iter()) {
            if let Some(blocked_fee) = blocked_account_fees.get(acc) {
                if blocked_fee > &fee_per_cu {
                    return Err(SchedulerError::AccountBlocked(**acc));
                }
            }
        }
        Ok(())
    }

    fn get_scheduled_batch(
        unprocessed_packets: &mut UnprocessedPacketBatches,
        scheduled_accounts: &Arc<Mutex<AccountLocks>>,
        highest_blocked_account_fees: &mut HashMap<Pubkey, u64>,
        num_txs: usize,
        bank: &Arc<Bank>,
    ) -> (Vec<SanitizedTransaction>, Vec<DeserializedPacket>) {
        let mut sanitized_transactions = Vec::new();
        let mut rescheduled_packets = Vec::new();

        while let Some(deserialized_packet) = unprocessed_packets.pop_max() {
            match Self::try_schedule(
                &deserialized_packet,
                bank,
                highest_blocked_account_fees,
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
        highest_blocked_account_fees: &mut HashMap<Pubkey, u64>,
    ) {
        let response_sender = scheduler_request.response_sender;
        match scheduler_request.msg {
            SchedulerMessage::RequestBatch { num_txs, bank } => {
                trace!("SchedulerMessage::RequestBatch num_txs: {}", num_txs);
                let (sanitized_transactions, rescheduled_packets) = Self::get_scheduled_batch(
                    unprocessed_packets,
                    scheduled_accounts,
                    highest_blocked_account_fees,
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
                        Self::drop_account_locks(
                            &mut account_locks,
                            &tx_locks.writable,
                            &tx_locks.readonly,
                        );
                    }
                }
                // TODO (LB): reschedule transactions
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

    /// NOTE: this is copied from accounts.rs
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
            number_of_dropped_packets +=
                unprocessed_packets.insert_batch(unprocessed_packet_batches::deserialize_packets(
                    &packet_batch,
                    &packet_indexes,
                    None,
                ));
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
    use solana_runtime::bank::Bank;
    use solana_sdk::instruction::AccountMeta;
    use {
        crate::transaction_scheduler::{SchedulerStage, TransactionScheduler},
        crossbeam_channel::unbounded,
        solana_perf::packet::PacketBatch,
        solana_runtime::cost_model::CostModel,
        solana_sdk::{
            hash::Hash,
            instruction::Instruction,
            packet::Packet,
            signature::{Keypair, Signer},
            system_program,
            transaction::Transaction,
        },
        std::sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
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

        // check alive
        assert_eq!(scheduler.send_ping(SchedulerStage::Transactions, 1).id, 1);
        assert_eq!(scheduler.send_ping(SchedulerStage::GossipVotes, 2).id, 2);
        assert_eq!(scheduler.send_ping(SchedulerStage::TpuVotes, 3).id, 3);

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
        assert_eq!(scheduler.send_ping(SchedulerStage::Transactions, 1).id, 1);
        assert_eq!(scheduler.send_ping(SchedulerStage::GossipVotes, 2).id, 2);
        assert_eq!(scheduler.send_ping(SchedulerStage::TpuVotes, 3).id, 3);

        exit.store(true, Ordering::Relaxed);

        assert_matches!(scheduler.join(), Ok(()));
        drop(tx_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);
    }

    #[test]
    fn test_simple_no_fee() {
        solana_logger::setup_with_default("trace");
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

        let a = Keypair::new().pubkey();
        let b = Keypair::new().pubkey();
        let c = Keypair::new().pubkey();

        let tx0_kp = Keypair::new();
        let tx0 = Transaction::new_signed_with_payer(
            &[Instruction::new_with_bytes(
                system_program::id(),
                &[0],
                vec![
                    AccountMeta::new(a.clone(), false),
                    AccountMeta::new(b.clone(), false),
                    AccountMeta::new(c.clone(), false),
                ],
            )],
            Some(&tx0_kp.pubkey()),
            &[&tx0_kp],
            Hash::default(),
        );
        let serialized_packet = Packet::from_data(None, &tx0).unwrap();
        tx_sender
            .send(vec![PacketBatch::new(vec![serialized_packet])])
            .unwrap();

        let _ = scheduler.send_ping(SchedulerStage::Transactions, 1);

        let bank = Arc::new(Bank::default_for_tests());

        let mut batch = scheduler.request_batch(SchedulerStage::Transactions, 1, bank);
        assert_eq!(batch.sanitized_transactions.len(), 1);
        assert_eq!(
            batch.sanitized_transactions.pop().unwrap().signature(),
            &tx0.signatures[0]
        );

        let _ = scheduler.send_batch_execution_update(
            SchedulerStage::Transactions,
            batch.sanitized_transactions,
            vec![],
        );

        drop(tx_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);
        assert_matches!(scheduler.join(), Ok(()));
    }
}
