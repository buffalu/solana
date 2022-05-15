use std::thread::{Builder, JoinHandle};

pub struct TransactionScheduler {
    insertion_thread: JoinHandle<()>,
}

impl TransactionScheduler {
    pub fn new() -> Self {
        let insertion_thread = Self::start_insertion_thread();
        TransactionScheduler { insertion_thread }
    }

    fn start_insertion_thread() -> JoinHandle<()> {
        Builder::new()
            .name("scheulder_insertion_thread")
            .spawn(move || {})
            .unwrap()
    }
}
