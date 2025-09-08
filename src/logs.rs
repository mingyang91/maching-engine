use rocksdb::DB;
use rocksdb::Options;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::thread;
use std::thread::yield_now;
use tokio::sync::RwLock;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{Sender, channel};
use tokio::sync::oneshot;

use crate::persister::PersisterError;
use crate::{
    persister::{MetadataStore, Persister},
    protos::Log,
};

struct LogEnvelope {
    logs: Vec<Log>,
    commit: oneshot::Sender<()>,
}

pub struct LogsPersister<P, E>
where
    P: Persister<u64, Log, Error = E>,
    P: MetadataStore<Error = E>,
    P: 'static + Send + Sync,
    E: Error,
{
    persister: P,
    last_sequence: AtomicU64,
    persist_tx: Sender<LogEnvelope>,
}

#[derive(thiserror::Error, Debug)]
pub enum LogsPersisterError<T: Error> {
    #[error("channel closed")]
    ChannelClosed,
    #[error("persister error")]
    PersisterError(#[from] T),
}

impl<P, E> LogsPersister<P, E>
where
    P: Persister<u64, Log, Error = E>,
    P: MetadataStore<Error = E>,
    P: 'static + Send + Sync,
    E: Error,
{
    fn create(persister: P, persist_tx: Sender<LogEnvelope>) -> Result<Self, E> {
        let last_sequence = persister.last_sequence()?;
        Ok(Self {
            persister,
            last_sequence: AtomicU64::new(last_sequence),
            persist_tx,
        })
    }

    async fn insert(&self, log: Log) -> Result<(), LogsPersisterError<E>> {
        let (commit, receiver) = oneshot::channel();
        self.persist_tx
            .send(LogEnvelope {
                logs: vec![log],
                commit,
            })
            .await
            .map_err(|_| LogsPersisterError::ChannelClosed)?;
        receiver
            .await
            .map_err(|_| LogsPersisterError::ChannelClosed)
    }

    async fn insert_batch(&self, logs: Vec<Log>) -> Result<(), LogsPersisterError<E>> {
        let (commit, receiver) = oneshot::channel();
        self.persist_tx
            .send(LogEnvelope { logs, commit })
            .await
            .map_err(|_| LogsPersisterError::ChannelClosed)?;
        receiver
            .await
            .map_err(|_| LogsPersisterError::ChannelClosed)
    }

    fn fetch_logs_by_sequence(&self, sequence: u64, limit: u64) -> Result<Vec<Log>, E> {
        self.persister
            .load_range_iter(sequence, sequence + limit)?
            .map(|result| result.map(|(_, log)| log))
            .collect::<Result<Vec<Log>, E>>()
    }
}

pub struct WAL<P, E>
where
    P: Persister<u64, Log, Error = E>,
    P: MetadataStore<Error = E>,
    P: 'static + Send + Sync,
    E: Error,
{
    inner: Arc<LogsPersister<P, E>>,
}

impl<P, E> Clone for WAL<P, E>
where
    P: Persister<u64, Log, Error = E>,
    P: MetadataStore<Error = E>,
    P: 'static + Send + Sync,
    E: Error,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<P, E> WAL<P, E>
where
    P: Persister<u64, Log, Error = E>,
    P: MetadataStore<Error = E>,
    P: 'static + Send + Sync,
    E: Error + 'static,
{
    pub fn load(persister: P) -> Self {
        let (persist_tx, mut persist_rx) = channel(u16::MAX as usize);
        let inner = Arc::new(
            LogsPersister::create(persister, persist_tx).expect("failed to create logs persister"),
        );
        let inner_clone = inner.clone();
        thread::spawn(move || {
            tracing::info!("wal writer thread started");
            'main_loop: loop {
                let logs = {
                    let mut logs = vec![];
                    loop {
                        match persist_rx.try_recv() {
                            Ok(log) => {
                                logs.push(log);
                            }
                            Err(TryRecvError::Empty) => break,
                            Err(TryRecvError::Disconnected) => break 'main_loop,
                        }
                    }
                    logs
                };

                if logs.is_empty() {
                    yield_now();
                    continue 'main_loop;
                }
                println!("wal writer thread received logs batch: {:?}", logs.len());
                tracing::debug!("wal writer thread received logs: {:?}", logs.len());

                let mut insert = vec![];
                let mut commits = vec![];
                for LogEnvelope { logs, commit } in logs {
                    for log in logs {
                        let seq = inner_clone.last_sequence.fetch_add(1, Ordering::Relaxed);
                        insert.push((seq, log));
                    }
                    commits.push(commit);
                }

                println!("wal write begin");
                {
                    inner_clone
                        .persister
                        .save_batch(insert, vec![])
                        .expect("failed to save logs");
                }
                println!("wal write end");

                for commit in commits {
                    if let Err(_) = commit.send(()) {
                        tracing::warn!("failed to send commit");
                    }
                }
            }

            tracing::info!("wal writer thread stopped");
        });
        Self { inner }
    }

    pub async fn insert(&self, log: Log) -> Result<(), LogsPersisterError<E>> {
        self.inner.insert(log).await
    }
    pub async fn insert_batch(&self, logs: Vec<Log>) -> Result<(), LogsPersisterError<E>> {
        self.inner.insert_batch(logs).await
    }

    pub fn fetch_logs_by_sequence(&self, sequence: u64, limit: u64) -> Result<Vec<Log>, E> {
        self.inner.fetch_logs_by_sequence(sequence, limit)
    }

    pub fn last_sequence(&self) -> u64 {
        self.inner.last_sequence.load(Ordering::Relaxed)
    }
}

impl WAL<DB, PersisterError<rocksdb::Error>> {
    pub fn init(path: &str) -> Result<Self, PersisterError<rocksdb::Error>> {
        let mut options = Options::default();
        options.create_if_missing(true);
        let persister = DB::open(&options, path).expect("failed to open db");
        Ok(Self::load(persister))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create() {
        let wal = WAL::init("data/wal").unwrap();
        let logs_persister = wal.inner;
        assert_eq!(logs_persister.last_sequence.load(Ordering::Relaxed), 0);
    }
}
