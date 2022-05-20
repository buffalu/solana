//! The `banking_stage` processes Transaction messages. It is intended to be used
//! to construct a software pipeline. The stage uses all available CPU cores and
//! can do its processing in parallel with signature verification on the GPU.
use crate::transaction_scheduler::{
    SchedulerStage, TransactionScheduler, TransactionSchedulerHandle,
};
use {
    crate::{
        leader_slot_banking_stage_metrics::LeaderSlotMetricsTracker,
        leader_slot_banking_stage_timing_metrics::RecordTransactionsTimings,
    },
    itertools::Itertools,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_perf::data_budget::DataBudget,
    solana_poh::poh_recorder::{PohRecorder, PohRecorderError},
    solana_runtime::{bank::Bank, cost_model::CostModel, vote_sender_types::ReplayVoteSender},
    solana_sdk::pubkey::Pubkey,
    std::{
        cmp, env,
        sync::{Arc, Mutex, RwLock},
        thread::{self, Builder, JoinHandle},
    },
};

// Fixed thread size seems to be fastest on GCP setup
pub const NUM_THREADS: u32 = 6;

const NUM_VOTE_PROCESSING_THREADS: u32 = 2;
const MIN_THREADS_BANKING: u32 = 1;
const MIN_TOTAL_THREADS: u32 = NUM_VOTE_PROCESSING_THREADS + MIN_THREADS_BANKING;
const UNPROCESSED_BUFFER_STEP_SIZE: usize = 128;

pub struct ProcessTransactionBatchOutput {
    // The number of transactions filtered out by the cost model
    cost_model_throttled_transactions_count: usize,
    // Amount of time spent running the cost model
    cost_model_us: u64,
    execute_and_commit_transactions_output: ExecuteAndCommitTransactionsOutput,
}

struct RecordTransactionsSummary {
    // Metrics describing how time was spent recording transactions
    record_transactions_timings: RecordTransactionsTimings,
    // Result of trying to record the transactions into the PoH stream
    result: Result<(), PohRecorderError>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CommitTransactionDetails {
    Committed { compute_units: u64 },
    NotCommitted,
}

#[derive(Debug, Default)]
pub struct BatchedTransactionDetails {
    pub costs: BatchedTransactionCostDetails,
    pub errors: BatchedTransactionErrorDetails,
}

#[derive(Debug, Default)]
pub struct BatchedTransactionCostDetails {
    pub batched_signature_cost: u64,
    pub batched_write_lock_cost: u64,
    pub batched_data_bytes_cost: u64,
    pub batched_builtins_execute_cost: u64,
    pub batched_bpf_execute_cost: u64,
}

#[derive(Debug, Default)]
pub struct BatchedTransactionErrorDetails {
    pub batched_retried_txs_per_block_limit_count: u64,
    pub batched_retried_txs_per_vote_limit_count: u64,
    pub batched_retried_txs_per_account_limit_count: u64,
    pub batched_retried_txs_per_account_data_block_limit_count: u64,
    pub batched_dropped_txs_per_account_data_total_limit_count: u64,
}

#[derive(Debug, Default)]
struct EndOfSlot {
    next_slot_leader: Option<Pubkey>,
    working_bank: Option<Arc<Bank>>,
}

/// Stores the stage's thread handle and output receiver.
pub struct BankingStage {
    bank_thread_hdls: Vec<JoinHandle<()>>,
}

#[derive(Debug, Clone)]
pub enum BufferedPacketsDecision {
    Consume(u128),
    Forward,
    ForwardAndHold,
    Hold,
}

#[derive(Debug, Clone)]
pub enum ForwardOption {
    NotForward,
    ForwardTpuVote,
    ForwardTransaction,
}

impl BankingStage {
    /// Create the stage using `bank`. Exit when `verified_receiver` is dropped.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        transaction_scheduler: &TransactionScheduler,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
        cost_model: Arc<RwLock<CostModel>>,
    ) -> Self {
        Self::new_num_threads(
            cluster_info,
            poh_recorder,
            transaction_scheduler,
            Self::num_threads(),
            transaction_status_sender,
            gossip_vote_sender,
            cost_model,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_num_threads(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        transaction_scheduler: &TransactionScheduler,
        num_threads: u32,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
        cost_model: Arc<RwLock<CostModel>>,
    ) -> Self {
        assert!(num_threads >= MIN_TOTAL_THREADS);
        // Single thread to generate entries from many banks.
        // This thread talks to poh_service and broadcasts the entries once they have been recorded.
        // Once an entry has been recorded, its blockhash is registered with the bank.
        let data_budget = Arc::new(DataBudget::default());

        // Many banks that process transactions in parallel.
        let bank_thread_hdls: Vec<JoinHandle<()>> = (0..num_threads)
            .map(|i| {
                let (scheduler_handle, forward_option) = match i {
                    0 => {
                        // Disable forwarding of vote transactions
                        // from gossip. Note - votes can also arrive from tpu
                        (
                            transaction_scheduler.get_handle(SchedulerStage::GossipVotes),
                            ForwardOption::NotForward,
                        )
                    }
                    1 => (
                        transaction_scheduler.get_handle(SchedulerStage::TpuVotes),
                        ForwardOption::ForwardTpuVote,
                    ),
                    _ => (
                        transaction_scheduler.get_handle(SchedulerStage::Transactions),
                        ForwardOption::ForwardTransaction,
                    ),
                };

                let poh_recorder = poh_recorder.clone();
                let cluster_info = cluster_info.clone();
                let transaction_status_sender = transaction_status_sender.clone();
                let gossip_vote_sender = gossip_vote_sender.clone();
                let data_budget = data_budget.clone();
                Builder::new()
                    .name(format!("solana-banking-stage-tx-{}", i))
                    .spawn(move || {
                        Self::process_loop(
                            scheduler_handle,
                            &poh_recorder,
                            &cluster_info,
                            forward_option,
                            i,
                            transaction_status_sender,
                            gossip_vote_sender,
                            &data_budget,
                        );
                    })
                    .unwrap()
            })
            .collect();
        Self { bank_thread_hdls }
    }

    #[allow(clippy::too_many_arguments)]
    fn process_loop(
        scheduler_handle: TransactionSchedulerHandle,
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        cluster_info: &ClusterInfo,
        forward_option: ForwardOption,
        id: u32,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
        data_budget: &DataBudget,
    ) {
        let recorder = poh_recorder.lock().unwrap().recorder();
        let mut banking_stage_stats = BankingStageStats::new(id);
        let mut slot_metrics_tracker = LeaderSlotMetricsTracker::new(id);

        loop {}
    }

    pub fn num_threads() -> u32 {
        cmp::max(
            env::var("SOLANA_BANKING_THREADS")
                .map(|x| x.parse().unwrap_or(NUM_THREADS))
                .unwrap_or(NUM_THREADS),
            MIN_TOTAL_THREADS,
        )
    }

    pub fn join(self) -> thread::Result<()> {
        for bank_thread_hdl in self.bank_thread_hdls {
            bank_thread_hdl.join()?;
        }
        Ok(())
    }
}
