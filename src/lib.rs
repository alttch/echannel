use async_channel::{Receiver, RecvError, Sender};
use std::collections::BTreeSet;
use std::ops::Deref;

pub trait EIdHash {
    fn eid_hash(&self) -> u64;
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
    pub fn new(data: T) -> Self {
        Self {
            data,
            initial: false,
        }
    }
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
}

impl<T> Deref for EFrameSender<T>
where
    T: EIdHash,
{
    type Target = Sender<EFrame<T>>;

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

impl<T> EFrameReceiver<T>
where
    T: EIdHash,
{
    pub async fn recv(&mut self) -> Result<T, RecvError> {
        loop {
            let frame = self.rx.recv().await?;
            let eid_hash = frame.data.eid_hash();
            if !frame.initial || !self.processed.contains(&eid_hash) {
                self.processed.insert(eid_hash);
                return Ok(frame.data);
            }
        }
    }
}

impl<T> Deref for EFrameReceiver<T>
where
    T: EIdHash,
{
    type Target = Receiver<EFrame<T>>;

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
