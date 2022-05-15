//! Implements a transaction scheduler for the three types of transaction receiving pipelines:
//! - Normal transactions
//! - TPU vote transactions
//! - Gossip vote transactions

use crate::unprocessed_packet_batches::{self, UnprocessedPacketBatches};
use {
    crossbeam_channel::{select, unbounded, Receiver, Sender},
    solana_perf::packet::PacketBatch,
    std::{
        thread,
        thread::{Builder, JoinHandle},
    },
};

pub enum SchedulerMessage {
    RequestBatch { num_txs: usize },
}

pub struct SchedulerRequest {
    msg: SchedulerMessage,
    response_sender: Sender<SchedulerResponse>,
}

pub struct SchedulerResponse {}

pub struct TransactionScheduler {
    tx_request_handler_thread: JoinHandle<()>,
    tx_scheduler_request_sender: Sender<SchedulerRequest>,

    tpu_vote_request_handler_thread: JoinHandle<()>,
    tpu_vote_scheduler_request_sender: Sender<SchedulerRequest>,

    gossip_vote_request_handler_thread: JoinHandle<()>,
    gossip_vote_scheduler_request_sender: Sender<SchedulerRequest>,
}

impl TransactionScheduler {
    pub fn new(
        verified_receiver: Receiver<Vec<PacketBatch>>,
        verified_tpu_vote_packets_receiver: Receiver<Vec<PacketBatch>>,
        verified_gossip_vote_packets_receiver: Receiver<Vec<PacketBatch>>,
    ) -> Self {
        let (tx_scheduler_request_sender, tx_scheduler_request_receiver) = unbounded();
        let tx_request_handler_thread = Self::start_event_loop(
            "tx_scheduler_insertion_thread",
            tx_scheduler_request_receiver,
            verified_receiver,
        );

        let (tpu_vote_scheduler_request_sender, tpu_vote_scheduler_request_receiver) = unbounded();
        let tpu_vote_request_handler_thread = Self::start_event_loop(
            "tpu_vote_scheduler_tx_insertion_thread",
            tpu_vote_scheduler_request_receiver,
            verified_tpu_vote_packets_receiver,
        );

        let (gossip_vote_scheduler_request_sender, gossip_vote_scheduler_request_receiver) =
            unbounded();
        let gossip_vote_request_handler_thread = Self::start_event_loop(
            "gossip_vote_scheduler_tx_insertion_thread",
            gossip_vote_scheduler_request_receiver,
            verified_gossip_vote_packets_receiver,
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

    fn start_event_loop(
        t_name: &str,
        scheduler_request_receiver: Receiver<SchedulerRequest>,
        packet_receiver: Receiver<Vec<PacketBatch>>,
    ) -> JoinHandle<()> {
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
                                    Self::handle_scheduler_request(&mut unprocessed_packet_batches, batch_request)
                                }
                                Err(_) => {
                                    break;
                                }
                            }
                        }
                    }
                }
            })
            .unwrap()
    }

    /// Handles scheduler requests and sends back a response over the channel
    fn handle_scheduler_request(
        _unprocessed_packets: &mut UnprocessedPacketBatches,
        scheduler_request: SchedulerRequest,
    ) {
        let _response_sender = scheduler_request.response_sender;

        match scheduler_request.msg {
            SchedulerMessage::RequestBatch { .. } => {}
        }
    }

    /// TODO(LB): probably want to have the same forwarding, hold and forward, etc. logic here
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

    /// Requests a batch containing up to num_txs transactions
    pub fn request_tx_batch(self, num_txs: usize) -> SchedulerResponse {
        Self::request_batch_internal(&self.tx_scheduler_request_sender, num_txs)
    }

    /// Requests a batch containing up to num_txs vote transactions from the tpu_vote port
    pub fn request_tpu_vote_batch(self, num_txs: usize) -> SchedulerResponse {
        Self::request_batch_internal(&self.tpu_vote_scheduler_request_sender, num_txs)
    }

    /// Requests a batch containing up to num_txs vote transactions from gossip
    pub fn request_gossip_vote_batch(self, num_txs: usize) -> SchedulerResponse {
        Self::request_batch_internal(&self.gossip_vote_scheduler_request_sender, num_txs)
    }

    fn request_batch_internal(
        request_sender: &Sender<SchedulerRequest>,
        num_txs: usize,
    ) -> SchedulerResponse {
        let (response_sender, response_receiver) = unbounded();
        let request = SchedulerRequest {
            msg: SchedulerMessage::RequestBatch { num_txs },
            response_sender,
        };
        let _ = request_sender.send(request).unwrap();
        response_receiver.recv().unwrap()
    }

    pub fn join(self) -> thread::Result<()> {
        self.tx_request_handler_thread.join()?;
        self.tpu_vote_request_handler_thread.join()?;
        self.gossip_vote_request_handler_thread.join()?;
        Ok(())
    }
}
