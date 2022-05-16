//! Implements a transaction scheduler for the three types of transaction receiving pipelines:
//! - Normal transactions
//! - TPU vote transactions
//! - Gossip vote transactions

use std::sync::Arc;
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
    RequestBatch { num_txs: usize },
    Ping { id: usize },
}

pub struct SchedulerRequest {
    msg: SchedulerMessage,
    response_sender: Sender<SchedulerResponse>,
}

pub enum SchedulerResponse {
    RequestBatch { batch: Vec<usize> },
    Ping { id: usize },
}

impl SchedulerResponse {
    fn ping(&self) -> usize {
        match self {
            SchedulerResponse::RequestBatch { .. } => {
                unreachable!("invalid response expected");
            }
            SchedulerResponse::Ping { id } => *id,
        }
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
    /// Creates an event loop and channel so external threads can communicate with each scheduler.
    pub fn new(
        verified_receiver: Receiver<Vec<PacketBatch>>,
        verified_tpu_vote_packets_receiver: Receiver<Vec<PacketBatch>>,
        verified_gossip_vote_packets_receiver: Receiver<Vec<PacketBatch>>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let (tx_scheduler_request_sender, tx_scheduler_request_receiver) = unbounded();
        let tx_request_handler_thread = Self::start_event_loop(
            "tx_scheduler_insertion_thread",
            tx_scheduler_request_receiver,
            verified_receiver,
            &exit,
        );

        let (tpu_vote_scheduler_request_sender, tpu_vote_scheduler_request_receiver) = unbounded();
        let tpu_vote_request_handler_thread = Self::start_event_loop(
            "tpu_vote_scheduler_tx_insertion_thread",
            tpu_vote_scheduler_request_receiver,
            verified_tpu_vote_packets_receiver,
            &exit,
        );

        let (gossip_vote_scheduler_request_sender, gossip_vote_scheduler_request_receiver) =
            unbounded();
        let gossip_vote_request_handler_thread = Self::start_event_loop(
            "gossip_vote_scheduler_tx_insertion_thread",
            gossip_vote_scheduler_request_receiver,
            verified_gossip_vote_packets_receiver,
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

    /// The event loop has two main responsibilities:
    /// 1. Handle incoming packets and prioritization around them.
    /// 2. Serve scheduler requests and return responses.
    fn start_event_loop(
        t_name: &str,
        scheduler_request_receiver: Receiver<SchedulerRequest>,
        packet_receiver: Receiver<Vec<PacketBatch>>,
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
                                    Self::handle_scheduler_request(&mut unprocessed_packet_batches, batch_request)
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

    /// Handles scheduler requests and sends back a response over the channel
    fn handle_scheduler_request(
        _unprocessed_packets: &mut UnprocessedPacketBatches,
        scheduler_request: SchedulerRequest,
    ) {
        let response_sender = scheduler_request.response_sender;

        match scheduler_request.msg {
            SchedulerMessage::RequestBatch { .. } => {}
            SchedulerMessage::Ping { id } => {
                let _ = response_sender
                    .send(SchedulerResponse::Ping { id })
                    .unwrap();
            }
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
        Self::make_scheduler_request(
            &self.tx_scheduler_request_sender,
            SchedulerMessage::RequestBatch { num_txs },
        )
    }

    /// Requests a batch containing up to num_txs vote transactions from the tpu_vote port
    pub fn request_tpu_vote_batch(self, num_txs: usize) -> SchedulerResponse {
        Self::make_scheduler_request(
            &self.tpu_vote_scheduler_request_sender,
            SchedulerMessage::RequestBatch { num_txs },
        )
    }

    /// Requests a batch containing up to num_txs vote transactions from gossip
    pub fn request_gossip_vote_batch(self, num_txs: usize) -> SchedulerResponse {
        Self::make_scheduler_request(
            &self.gossip_vote_scheduler_request_sender,
            SchedulerMessage::RequestBatch { num_txs },
        )
    }

    #[cfg(test)]
    pub fn ping_tx(&self, id: usize) -> SchedulerResponse {
        Self::make_scheduler_request(
            &self.tx_scheduler_request_sender,
            SchedulerMessage::Ping { id },
        )
    }

    #[cfg(test)]
    pub fn ping_tpu_vote(&self, id: usize) -> SchedulerResponse {
        Self::make_scheduler_request(
            &self.tpu_vote_scheduler_request_sender,
            SchedulerMessage::Ping { id },
        )
    }

    #[cfg(test)]
    pub fn ping_gossip_vote(&self, id: usize) -> SchedulerResponse {
        Self::make_scheduler_request(
            &self.gossip_vote_scheduler_request_sender,
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

    pub fn join(self) -> thread::Result<()> {
        self.tx_request_handler_thread.join()?;
        self.tpu_vote_request_handler_thread.join()?;
        self.gossip_vote_request_handler_thread.join()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::transaction_scheduler::TransactionScheduler;
    use crossbeam_channel::unbounded;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_start_and_join_channel_dropped() {
        let (tx_sender, tx_receiver) = unbounded();
        let (tpu_vote_sender, tpu_vote_receiver) = unbounded();
        let (gossip_vote_sender, gossip_vote_receiver) = unbounded();
        let exit = Arc::new(AtomicBool::new(false));

        let scheduler =
            TransactionScheduler::new(tx_receiver, tpu_vote_receiver, gossip_vote_receiver, exit);

        // check alive
        assert_eq!(scheduler.ping_tx(1).ping(), 1);
        assert_eq!(scheduler.ping_gossip_vote(2).ping(), 2);
        assert_eq!(scheduler.ping_tpu_vote(3).ping(), 3);

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

        let scheduler = TransactionScheduler::new(
            tx_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            exit.clone(),
        );

        // check alive
        assert_eq!(scheduler.ping_tx(1).ping(), 1);
        assert_eq!(scheduler.ping_gossip_vote(2).ping(), 2);
        assert_eq!(scheduler.ping_tpu_vote(3).ping(), 3);

        exit.store(true, Ordering::Relaxed);

        assert_matches!(scheduler.join(), Ok(()));
        drop(tx_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);
    }
}
