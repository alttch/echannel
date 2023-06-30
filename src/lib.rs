use async_channel::{Receiver, Sender};
pub use async_channel::{RecvError, TryRecvError, TrySendError};
use std::collections::BTreeSet;
use std::ops::Deref;

pub trait EIdHash {
    fn eid_hash(&self) -> Option<u64>;
}

pub struct EFrame<T>
where
    T: EIdHash,
{
    pub data: T,
    initial: bool,
}

impl<T> EFrame<T>
where
    T: EIdHash,
{
    #[inline]
    pub fn new(data: T) -> Self {
        Self {
            data,
            initial: false,
        }
    }
    #[inline]
    pub fn new_initial(data: T) -> Self {
        Self {
            data,
            initial: true,
        }
    }
}

pub struct EFrameSender<T>
where
    T: EIdHash,
{
    tx: Sender<EFrame<T>>,
}

impl<T> Clone for EFrameSender<T>
where
    T: EIdHash,
{
    #[inline]
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

impl<T> EFrameSender<T>
where
    T: EIdHash,
{
    #[inline]
    pub fn send(&self, data: T) -> async_channel::Send<'_, EFrame<T>> {
        self.tx.send(EFrame::new(data))
    }
    #[inline]
    pub fn send_initial(&self, data: T) -> async_channel::Send<'_, EFrame<T>> {
        self.tx.send(EFrame::new_initial(data))
    }
    #[inline]
    pub fn try_send(&self, data: T) -> Result<(), TrySendError<EFrame<T>>> {
        self.tx.try_send(EFrame::new(data))
    }
    #[inline]
    pub fn try_send_initial(&self, data: T) -> Result<(), TrySendError<EFrame<T>>> {
        self.tx.try_send(EFrame::new_initial(data))
    }
}

impl<T> Deref for EFrameSender<T>
where
    T: EIdHash,
{
    type Target = Sender<EFrame<T>>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.tx
    }
}

pub struct EFrameReceiver<T>
where
    T: EIdHash,
{
    rx: Receiver<EFrame<T>>,
    processed: BTreeSet<u64>,
}

macro_rules! process_recv {
    ($frame: expr, $processed: expr) => {
        if let Some(eid_hash) = $frame.data.eid_hash() {
            if !$frame.initial || !$processed.contains(&eid_hash) {
                $processed.insert(eid_hash);
                return Ok($frame.data);
            }
        } else {
            return Ok($frame.data);
        }
    };
}

impl<T> EFrameReceiver<T>
where
    T: EIdHash,
{
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        loop {
            process_recv!(self.rx.recv().await?, self.processed);
        }
    }
    pub fn recv_blocking(&mut self) -> Result<T, RecvError> {
        loop {
            process_recv!(self.rx.recv_blocking()?, self.processed);
        }
    }
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        loop {
            process_recv!(self.rx.try_recv()?, self.processed);
        }
    }
    #[inline]
    pub fn reset_processed(&mut self) {
        self.processed.clear();
    }
}

impl<T> Deref for EFrameReceiver<T>
where
    T: EIdHash,
{
    type Target = Receiver<EFrame<T>>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

pub fn bounded<T>(cap: usize) -> (EFrameSender<T>, EFrameReceiver<T>)
where
    T: EIdHash,
{
    let (tx, rx) = async_channel::bounded(cap);
    (
        EFrameSender { tx },
        EFrameReceiver {
            rx,
            processed: <_>::default(),
        },
    )
}
