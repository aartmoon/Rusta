//! Storage-adapter spike. The fixture supplies hard state and membership separately from the log.
use raft::eraftpb::{Entry, HardState, Snapshot};
use raft::{Config, Error, GetEntriesContext, RaftState, RawNode, Storage, StorageError};
use rusta_log::{Options, PartitionLog};
use tempfile::TempDir;

struct DiskStorage(PartitionLog);

fn other(error: std::io::Error) -> Error {
    StorageError::Other(Box::new(error)).into()
}

impl Storage for DiskStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let mut state = HardState::default();
        state.set_commit(self.0.committed().next_index - 1);
        state.set_term(self.term(self.last_index()?)?);
        Ok(RaftState::new(state, (vec![1], vec![]).into()))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        if low < self.first_index()? {
            return Err(StorageError::Compacted.into());
        }
        if high > self.last_index()? + 1 {
            return Err(StorageError::Unavailable.into());
        }
        let mut result = Vec::new();
        self.0
            .visit_entries(
                low,
                high,
                max_size.into().unwrap_or(u64::MAX),
                |stored, bytes| {
                    let mut entry = Entry::default();
                    entry.set_index(stored.position.next_index);
                    entry.set_term(stored.term);
                    entry.set_data(bytes.to_vec().into());
                    result.push(entry);
                    Ok(())
                },
            )
            .map_err(other)?;
        Ok(result)
    }

    fn term(&self, index: u64) -> raft::Result<u64> {
        if index < self.first_index()? - 1 {
            return Err(StorageError::Compacted.into());
        }
        if index > self.last_index()? {
            return Err(StorageError::Unavailable.into());
        }
        self.0.term(index).map_err(other)
    }

    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.0.start().next_index)
    }

    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.0.end().next_index - 1)
    }

    fn snapshot(&self, _request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        Err(StorageError::SnapshotTemporarilyUnavailable.into())
    }
}

#[test]
fn raw_node_applies_the_committed_disk_prefix_on_restart() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = TempDir::new()?;
    let mut log = PartitionLog::open(dir.path(), Options::default())?;
    log.append_internal(2, 1, b"committed")?;
    log.commit()?;
    log.append_internal(2, 2, b"durable only")?;
    log.sync()?;
    drop(log);

    let disk = DiskStorage(PartitionLog::open(dir.path(), Options::default())?);
    let config = Config {
        id: 1,
        ..Config::default()
    };
    let mut node = RawNode::new(
        &config,
        disk,
        &slog::Logger::root(slog::Discard, slog::o!()),
    )?;
    let ready = node.ready();
    assert_eq!(ready.committed_entries().len(), 1);
    assert_eq!(ready.committed_entries()[0].get_data(), b"committed");
    assert_eq!(node.store().last_index()?, 2);
    assert_eq!(node.store().term(2)?, 2);
    Ok(())
}
