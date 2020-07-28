pub mod overseer;

use std::any::Any;

use crossbeam_channel::SendError;

pub enum Message {
    Stop,
}

#[derive(Debug)]
pub enum WorkerError {
    Channel(SendError<Message>),
    Thread(Box<dyn Any + Send + 'static>),
    ThreadNotStarted,
}

pub trait Worker {
    type Error;
    fn run(&mut self);
    fn stop(&mut self) -> Result<(), Self::Error>;
}
