use std::fs::{self, OpenOptions};
use std::io::{self, ErrorKind, Seek, SeekFrom, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use rusta_log::{Options, PartitionLog, Position, Retention};

#[test]
fn durable_raft_manifest_upgrades_and_recovers_from_a_torn_slot() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let opts = Options {
        segment_bytes: 120,
        ..Options::default()
    };
    let mut old = PartitionLog::open(dir.path(), opts)?;
    old.append_batch(1, 100, &batch(&[(100, 1)]))?;
    let initial = old.commit()?;
    drop(old);

    let mut log = PartitionLog::open_raft_durable(dir.path(), opts)?;
    assert_eq!(log.committed(), initial);
    log.append_batch(1, 101, &batch(&[(101, 2)]))?;
    let confirmed = log.commit()?;
    log.append_batch(1, 102, &batch(&[(102, 3)]))?;
    drop(log);

    let mut file = OpenOptions::new()
        .write(true)
        .open(dir.path().join("manifest"))?;
    // The next update would overwrite slot 1. Leave a generation without its checksum.
    file.seek(SeekFrom::Start(64 * 1024))?;
    file.write_all(&3_u64.to_be_bytes())?;
    file.sync_data()?;
    drop(file);
    let log = PartitionLog::open_raft_durable(dir.path(), opts)?;
    assert_eq!(log.committed(), confirmed);
    assert_eq!(log.end(), confirmed);
    assert!(log.batch_at(0, &mut Vec::new())?.is_some());
    assert!(log.batch_at(1, &mut Vec::new())?.is_some());
    Ok(())
}

#[test]
fn cached_progress_survives_process_reopen_and_can_be_flushed() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut log = PartitionLog::open(dir.path(), Options::default())?;
    log.enable_page_cache();
    log.append_batch(1, 100, &batch(&[(100, 1)]))?;
    let acknowledged = log.commit()?;
    assert!(log.needs_page_cache_flush());
    drop(log);

    let mut log = PartitionLog::open(dir.path(), Options::default())?;
    assert_eq!(log.committed(), acknowledged);
    log.enable_page_cache();
    log.flush_page_cache()?;
    assert!(!log.needs_page_cache_flush());
    drop(log);
    let log = PartitionLog::open(dir.path(), Options::default())?;
    assert_eq!(log.committed(), acknowledged);
    assert_eq!(log.batch_at(0, &mut Vec::new())?.unwrap().record_count, 1);
    Ok(())
}

#[test]
fn kernel_loss_rolls_back_only_the_buffered_suffix() -> io::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut log = PartitionLog::open_page_cached(dir.path(), Options::default(), false)?;
    log.enable_page_cache();
    log.append_batch(1, 100, &batch(&[(100, 1)]))?;
    let stable = log.commit()?;
    log.flush_page_cache()?;
    log.append_batch(1, 200, &batch(&[(200, 2)]))?;
    assert_eq!(log.commit()?.next_offset, 2);
    drop(log);

    fs::write(dir.path().join("manifest"), b"lost cached manifest")?;
    let mut log = PartitionLog::open_page_cached(dir.path(), Options::default(), true)?;
    assert_eq!(log.committed(), stable);
    assert_eq!(log.end(), stable);
    log.append_batch(2, 300, &batch(&[(300, 3)]))?;
    assert_eq!(log.commit()?.next_offset, 2);
    drop(log);

    let log = PartitionLog::open_page_cached(dir.path(), Options::default(), true)?;
    assert_eq!(log.committed().next_offset, 2);
    assert_eq!(log.term(2)?, 2);
    Ok(())
}
use tempfile::TempDir;

fn options(segment_bytes: u64) -> Options {
    Options {
        segment_bytes,
        ..Options::default()
    }
}

fn position(index: u64, offset: i64) -> Position {
    Position {
        next_index: index,
        next_offset: offset,
    }
}

#[test]
fn legacy_append_time_can_use_the_retention_activation_timestamp() -> io::Result<()> {
    let dir = TempDir::new()?;
    let mut log = PartitionLog::open(dir.path(), Options::default())?;
    log.append_batch(1, 0, &batch(&[(100, 1)]))?;
    let committed = log.commit()?;
    drop(log);
    let log = PartitionLog::open(dir.path(), Options::default())?;
    let policy = |cutoff| Retention {
        keep_since_ms: Some(cutoff),
        max_bytes: None,
    };
    assert_eq!(
        log.retention_boundary_with_legacy_time(log.start(), policy(120), Some(120))?,
        log.start()
    );
    assert_eq!(
        log.retention_boundary_with_legacy_time(log.start(), policy(121), Some(120))?,
        committed
    );
    Ok(())
}

#[test]
fn restart_at_snapshot_boundary_ignores_obsolete_segments() -> io::Result<()> {
    let dir = TempDir::new()?;
    let mut log = PartitionLog::open(dir.path(), options(256))?;
    log.append_batch(1, 100, &batch(&[(100, 1)]))?;
    log.commit()?;
    log.append_internal(1, 0, b"uncommitted")?;
    log.sync()?;
    let obsolete = fs::read(file(dir.path(), 1))?;
    assert!(log.install_snapshot_base(position(2, 0), 1).is_err());
    let base = position(20, 10);
    log.install_snapshot_base(base, 2)?;
    assert_eq!(log.committed(), base);
    drop(log);

    // Simulate a crash after manifest publication and before segment cleanup.
    fs::write(file(dir.path(), 1), obsolete)?;
    let mut log = PartitionLog::open(dir.path(), options(256))?;
    assert!(!file(dir.path(), 1).exists());
    assert_eq!(log.start(), base);
    assert_eq!(log.term(19)?, 2);
    assert_eq!(
        log.append_batch(3, 200, &batch(&[(200, 2)]))?.position,
        base
    );
    assert_eq!(log.commit()?, position(21, 11));
    Ok(())
}

#[test]
#[ignore = "set RUSTA_TEST_FULL_DISK_DIR to a dedicated bounded filesystem"]
fn full_disk_preserves_the_committed_prefix() -> io::Result<()> {
    let filesystem = std::env::var_os("RUSTA_TEST_FULL_DISK_DIR")
        .expect("set RUSTA_TEST_FULL_DISK_DIR to a dedicated tmpfs");
    test_full_disk(&filesystem, |root| {
        PartitionLog::open(root, Options::default())
    })?;
    test_full_disk(&filesystem, |root| {
        PartitionLog::open_raft_durable(root, Options::default())
    })?;

    let dir = tempfile::tempdir_in(filesystem)?;
    let root = dir.path().join("partition");
    let mut log = PartitionLog::open_raft_durable(&root, Options::default())?;
    let confirmed = log.committed();
    log.append_batch(1, 100, &batch(&[(100, 1)]))?;
    let fill_path = dir.path().join("fill");
    fill_filesystem(&fill_path)?;
    // The next checkpoint has to allocate space for the other slot.
    assert_eq!(log.commit().unwrap_err().kind(), ErrorKind::StorageFull);
    drop(log);
    fs::remove_file(fill_path)?;
    let log = PartitionLog::open_raft_durable(&root, Options::default())?;
    assert_eq!(log.committed(), confirmed);
    assert_eq!(log.end(), confirmed);
    Ok(())
}

fn test_full_disk(
    filesystem: impl AsRef<Path>,
    open: impl Fn(&Path) -> io::Result<PartitionLog>,
) -> io::Result<()> {
    let dir = tempfile::tempdir_in(filesystem)?;
    let root = dir.path().join("partition");
    let mut log = open(&root)?;
    log.append_batch(1, 100, &batch(&[(100, 1)]))?;
    let committed = log.commit()?;
    let fill_path = dir.path().join("fill");
    fill_filesystem(&fill_path)?;
    let error = log.append_internal(1, 0, &[0; 8192]).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::StorageFull);
    drop(log);
    fs::remove_file(fill_path)?;

    let mut log = open(&root)?;
    assert_eq!(log.committed(), committed);
    assert_eq!(log.durable(), committed);
    assert_eq!(log.end(), committed);
    assert_eq!(log.batch_at(0, &mut Vec::new())?.unwrap().record_count, 1);
    assert_eq!(
        log.append_batch(2, 200, &batch(&[(200, 2)]))?.position,
        committed
    );
    assert_eq!(log.commit()?, position(3, 2));
    Ok(())
}

fn fill_filesystem(path: &Path) -> io::Result<()> {
    let mut filler = fs::File::create(path)?;
    for _ in 0..16_384 {
        if let Err(error) = filler.write_all(&[0; 4096]) {
            assert_eq!(error.kind(), ErrorKind::StorageFull);
            return Ok(());
        }
    }
    panic!("test filesystem exceeded its 64 MiB budget");
}

fn file(root: &Path, first_index: u64) -> std::path::PathBuf {
    root.join(format!("{first_index:020}.log"))
}

fn varint(bytes: &mut Vec<u8>, value: i64) {
    let mut encoded = ((value << 1) ^ (value >> 63)) as u64;
    while encoded >= 128 {
        bytes.push((encoded as u8 & 127) | 128);
        encoded >>= 7;
    }
    bytes.push(encoded as u8);
}

fn batch(records: &[(i64, u8)]) -> Vec<u8> {
    let base_timestamp = records[0].0;
    let mut data = vec![0; 61];
    data[16] = 2;
    data[23..27].copy_from_slice(&(records.len() as i32 - 1).to_be_bytes());
    data[27..35].copy_from_slice(&base_timestamp.to_be_bytes());
    data[35..43].copy_from_slice(&records.iter().map(|r| r.0).max().unwrap().to_be_bytes());
    data[43..57].fill(255);
    data[57..61].copy_from_slice(&(records.len() as i32).to_be_bytes());
    for (offset, &(timestamp, value)) in records.iter().enumerate() {
        let mut body = vec![0]; // Attributes.
        varint(&mut body, timestamp - base_timestamp);
        varint(&mut body, offset as i64);
        varint(&mut body, -1); // Null key.
        varint(&mut body, 1);
        body.push(value);
        varint(&mut body, 0); // No headers.
        varint(&mut data, body.len() as i64);
        data.extend(body);
    }
    let length = data.len() as i32 - 12;
    data[8..12].copy_from_slice(&length.to_be_bytes());
    let crc = crc32c::crc32c(&data[21..]);
    data[17..21].copy_from_slice(&crc.to_be_bytes());
    data
}

#[test]
fn commits_ordered_offsets_over_segment_rolls() -> io::Result<()> {
    let dir = TempDir::new()?;
    let root = dir.path().join("partition");
    let mut log = PartitionLog::open(&root, options(115))?;
    let mut payload = Vec::new();
    let first = batch(&[(10, 1), (11, 2)]);
    let second = batch(&[(12, 3)]);
    log.append_batch(1, 100, &first)?;
    log.append_batch(1, 101, &second)?;
    assert_eq!(log.end(), position(3, 3));
    assert_eq!(log.committed(), position(1, 0));
    assert!(log.batch_at(0, &mut payload)?.is_none());
    assert_eq!(log.commit()?, position(3, 3));

    drop(log);
    let log = PartitionLog::open(&root, options(115))?;
    assert_eq!(log.committed(), position(3, 3));
    assert_eq!(
        log.batch_at(1, &mut payload)?.unwrap().position,
        position(1, 0)
    );
    assert_eq!(payload, first);
    assert_eq!(
        log.batch_at(2, &mut payload)?.unwrap().position,
        position(2, 2)
    );
    assert_eq!(payload[..8], 2_i64.to_be_bytes());
    assert_eq!(payload[8..], second[8..]);
    assert!(log.batch_at(3, &mut payload)?.is_none());
    assert!(payload.is_empty());
    assert!(file(&root, 2).is_file());
    Ok(())
}

#[test]
fn timestamp_lookup_respects_record_order_and_visibility() -> io::Result<()> {
    let dir = TempDir::new()?;
    let mut log = PartitionLog::open(dir.path(), options(1024))?;
    log.append_batch(1, 10, &batch(&[(2, 1), (100, 2)]))?;
    log.append_internal(1, 11, b"configuration")?;
    log.append_batch(1, 12, &batch(&[(40, 3)]))?;
    log.commit()?;
    log.append_batch(1, 13, &batch(&[(200, 4)]))?;

    let found = log.offset_for_timestamp(35, &mut Vec::new(), |entry, _, _| {
        assert_eq!(entry.position, position(1, 0));
        Ok(Some(entry.position.next_offset + 1))
    })?;
    assert_eq!(found, Some(1));
    let invisible = log.offset_for_timestamp(150, &mut Vec::new(), |_, _, _| {
        panic!("uncommitted batch must not reach the decoder");
    })?;
    assert_eq!(invisible, None);
    assert_eq!(
        log.batch_at(2, &mut Vec::new())?.unwrap().position,
        position(3, 2)
    );
    Ok(())
}

#[test]
fn retention_checkpoints_before_unlink_and_preserves_offset_base() -> io::Result<()> {
    let dir = TempDir::new()?;
    let mut log = PartitionLog::open(dir.path(), options(110))?;
    log.append_batch(1, 10, &batch(&[(100, 1)]))?;
    log.append_batch(2, 20, &batch(&[(1, 2)]))?;
    log.append_batch(2, 30, &batch(&[(2, 3)]))?;
    log.commit()?;
    let first_file = file(dir.path(), 1);
    let orphan = fs::read(&first_file)?;
    assert_eq!(
        log.apply_retention(Retention {
            keep_since_ms: Some(20),
            max_bytes: None,
        })?,
        1
    );
    assert_eq!(log.start(), position(2, 1));
    assert_eq!(log.term(1)?, 1);
    assert!(!first_file.exists());
    drop(log);

    // The new checkpoint is durable, but the original directory entry survived.
    fs::write(&first_file, orphan)?;
    let mut log = PartitionLog::open(dir.path(), options(110))?;
    assert!(!first_file.exists());
    assert_eq!(
        log.batch_at(0, &mut Vec::new()).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        log.apply_retention(Retention {
            max_bytes: Some(0),
            keep_since_ms: None,
        })?,
        1
    );
    assert_eq!(log.start(), position(3, 2));
    assert_eq!(log.batch_at(2, &mut Vec::new())?.unwrap().term, 2);
    assert_eq!(
        log.append_batch(2, 40, &batch(&[(5, 4)]))?.position,
        position(4, 3)
    );
    Ok(())
}

#[test]
fn disk_adapter_reads_terms_and_replaces_a_conflicting_durable_suffix() -> io::Result<()> {
    let dir = TempDir::new()?;
    let mut log = PartitionLog::open(dir.path(), options(1024))?;
    log.append_internal(2, 10, b"leader election")?;
    log.append_batch(2, 11, &batch(&[(2, 7)]))?;
    log.append_batch(2, 12, &batch(&[(3, 8)]))?;
    log.sync()?;
    assert_eq!(log.commit_through(3)?, position(3, 1));
    assert_eq!(log.durable(), position(4, 2));
    drop(log);

    let mut log = PartitionLog::open(dir.path(), options(1024))?;
    assert_eq!(log.term(0)?, 0);
    assert_eq!(log.term(3)?, 2);
    assert!(log.batch_at(1, &mut Vec::new())?.is_none());
    let mut visited = Vec::new();
    let next = log.visit_entries(1, 4, 1, |entry, bytes| {
        assert_eq!(bytes, b"leader election");
        visited.push(entry.position.next_index);
        Ok(())
    })?;
    assert_eq!((next, visited), (2, vec![1]));
    assert!(log.truncate_suffix(2).is_err());
    log.truncate_suffix(3)?;
    assert_eq!(log.durable(), position(3, 1));
    assert_eq!(
        log.append_batch(3, 13, &batch(&[(4, 9)]))?.position,
        position(3, 1)
    );
    log.sync()?;
    log.commit_through(4)?;
    drop(log);

    let log = PartitionLog::open(dir.path(), options(1024))?;
    assert_eq!(log.term(3)?, 3);
    assert_eq!(log.committed(), position(4, 2));
    Ok(())
}

#[test]
fn truncating_staged_suffix_preserves_prefix_without_committing_it() -> io::Result<()> {
    let dir = TempDir::new()?;
    let mut log = PartitionLog::open(dir.path(), options(1024))?;
    log.append_batch(1, 10, &batch(&[(1, 1)]))?;
    log.commit()?;
    log.append_batch(1, 11, &batch(&[(2, 2)]))?;
    log.append_batch(1, 12, &batch(&[(3, 3)]))?;
    log.truncate_suffix(3)?;
    assert_eq!(log.end(), position(3, 2));
    assert_eq!(log.durable(), position(2, 1));
    assert_eq!(log.commit()?, position(3, 2));
    drop(log);
    assert_eq!(
        PartitionLog::open(dir.path(), options(1024))?.committed(),
        position(3, 2)
    );
    Ok(())
}

#[test]
fn recovery_discards_torn_uncommitted_bytes_but_rejects_damage_in_durable_data() -> io::Result<()> {
    let dir = TempDir::new()?;
    let mut log = PartitionLog::open(dir.path(), options(1024))?;
    log.append_batch(1, 10, &batch(&[(1, 1)]))?;
    log.commit()?;
    drop(log);
    let segment = file(dir.path(), 1);
    let durable_len = fs::metadata(&segment)?.len();

    let mut writer = OpenOptions::new().append(true).open(&segment)?;
    writer.write_all(&[0xff; 9])?;
    writer.sync_all()?;
    drop(writer);
    drop(PartitionLog::open(dir.path(), options(1024))?);
    assert_eq!(fs::metadata(&segment)?.len(), durable_len);
    let mut writer = OpenOptions::new().write(true).open(&segment)?;
    writer.seek(SeekFrom::Start(durable_len - 1))?;
    writer.write_all(&[0xff])?;
    writer.sync_all()?;
    drop(writer);

    let error = PartitionLog::open(dir.path(), options(1024)).err().unwrap();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(fs::metadata(&segment)?.len(), durable_len);
    Ok(())
}

#[test]
fn recovery_rejects_missing_durable_segment_and_concurrent_owner() -> io::Result<()> {
    let dir = TempDir::new()?;
    let mut log = PartitionLog::open(dir.path(), options(110))?;
    assert_eq!(
        PartitionLog::open(dir.path(), options(110))
            .err()
            .unwrap()
            .kind(),
        ErrorKind::WouldBlock,
    );
    log.append_batch(1, 10, &batch(&[(1, 1)]))?;
    log.append_batch(1, 11, &batch(&[(2, 2)]))?;
    log.commit()?;
    drop(log);
    fs::remove_file(file(dir.path(), 1))?;
    assert_eq!(
        PartitionLog::open(dir.path(), options(110))
            .err()
            .unwrap()
            .kind(),
        ErrorKind::InvalidData,
    );
    Ok(())
}

#[test]
fn recovery_reads_batches_written_before_lowering_the_admission_limit() -> io::Result<()> {
    let dir = TempDir::new()?;
    let initial = options(1024);
    let mut log = PartitionLog::open(dir.path(), initial)?;
    let bytes = batch(&[(1, 1), (2, 2)]);
    log.append_batch(1, 10, &bytes)?;
    log.commit()?;
    drop(log);
    let mut log = PartitionLog::open(
        dir.path(),
        Options {
            max_entry_bytes: 61,
            ..initial
        },
    )?;
    assert!(log.append_batch(1, 11, &bytes).is_err());
    assert!(log.batch_at(1, &mut Vec::new())?.is_some());
    Ok(())
}

#[test]
fn sparse_index_remains_correct_after_coarsening_and_recovery() -> io::Result<()> {
    let dir = TempDir::new()?;
    let mut log = PartitionLog::open(dir.path(), Options::default())?;
    let bytes = vec![7; 17 * 1024];
    for index in 0..400 {
        log.append_internal(index + 1, 10, &bytes)?;
    }
    log.append_batch(400, 10, &batch(&[(1, 1)]))?;
    log.commit()?;
    for reopen in [false, true] {
        if reopen {
            drop(log);
            log = PartitionLog::open(dir.path(), Options::default())?;
        }
        for index in [1, 125, 129, 199, 255, 391, 400] {
            assert_eq!(log.term(index)?, index);
        }
        assert_eq!(
            log.batch_at(0, &mut Vec::new())?.unwrap().position,
            position(401, 0)
        );
    }
    Ok(())
}

#[test]
fn abrupt_process_exit_preserves_only_the_checkpointed_boundary() -> io::Result<()> {
    let dir = TempDir::new()?;
    let mut child = Command::new(std::env::current_exe()?)
        .args(["--exact", "crash_worker"])
        .env("RUSTA_LOG_CRASH_DIR", dir.path())
        .stdout(Stdio::null())
        .spawn()?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !dir.path().join("ready").exists() {
        if let Some(status) = child.try_wait()? {
            return Err(io::Error::other(format!(
                "worker exited before fault: {status}"
            )));
        }
        if Instant::now() >= deadline {
            child.kill()?;
            child.wait()?;
            return Err(io::Error::other("timed out waiting for staged write"));
        }
        thread::sleep(Duration::from_millis(10));
    }
    child.kill()?;
    assert!(!child.wait()?.success());

    let log = PartitionLog::open(dir.path(), options(110))?;
    assert_eq!(log.end(), position(2, 1));
    assert_eq!(log.committed(), position(2, 1));
    assert!(!file(dir.path(), 2).exists());
    Ok(())
}

#[test]
fn crash_worker() -> io::Result<()> {
    let Some(dir) = std::env::var_os("RUSTA_LOG_CRASH_DIR") else {
        return Ok(());
    };
    let dir = Path::new(&dir);
    let mut log = PartitionLog::open(dir, options(110))?;
    log.append_batch(1, 10, &batch(&[(1, 1)]))?;
    log.commit()?;
    log.append_batch(1, 11, &batch(&[(2, 2)]))?;
    fs::write(dir.join("ready"), b"ready")?;
    loop {
        thread::park();
    }
}

#[test]
fn an_expired_idle_partition_reclaims_its_final_segment() -> io::Result<()> {
    let dir = TempDir::new()?;
    let mut log = PartitionLog::open(dir.path(), options(1024))?;
    log.append_batch(1, 10, &batch(&[(1, 1)]))?;
    log.commit()?;
    assert_eq!(
        log.apply_retention(Retention {
            keep_since_ms: Some(11),
            max_bytes: None
        })?,
        1
    );
    assert_eq!(log.start(), log.committed());
    drop(log);
    let mut log = PartitionLog::open(dir.path(), options(1024))?;
    assert_eq!(log.term(1)?, 1);
    assert_eq!(
        log.append_batch(2, 15, &batch(&[(2, 2)]))?.position,
        position(2, 1)
    );
    log.commit()?;
    Ok(())
}

#[test]
fn invalid_batch_crc_does_not_advance_the_log() -> io::Result<()> {
    let dir = TempDir::new()?;
    let mut log = PartitionLog::open(dir.path(), Options::default())?;
    let mut bytes = batch(&[(1, 1)]);
    bytes[17] ^= 1;
    assert_eq!(
        log.append_batch(1, 10, &bytes).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(log.end(), position(1, 0));
    Ok(())
}

#[test]
fn failed_checkpoint_never_publishes_synced_data() -> io::Result<()> {
    let dir = TempDir::new()?;
    let mut log = PartitionLog::open(dir.path(), options(1024))?;
    log.append_batch(1, 10, &batch(&[(1, 1)]))?;
    log.commit()?;
    log.append_batch(1, 11, &batch(&[(2, 2)]))?;

    // Make the checkpoint write fail after sync_data on the active segment succeeds.
    let temporary = dir.path().join("manifest.tmp");
    fs::create_dir(&temporary)?;
    assert!(log.commit().is_err());
    assert_eq!(log.committed(), position(2, 1));
    assert!(log.batch_at(0, &mut Vec::new()).is_err());
    drop(log);
    fs::remove_dir(temporary)?;

    let log = PartitionLog::open(dir.path(), options(1024))?;
    assert_eq!(log.committed(), position(2, 1));
    assert_eq!(log.end(), position(2, 1));
    assert!(log.batch_at(0, &mut Vec::new())?.is_some());
    assert!(log.batch_at(1, &mut Vec::new())?.is_none());
    Ok(())
}

#[test]
fn logical_retention_keeps_raft_entries_and_respects_segment_boundaries() -> io::Result<()> {
    let dir = TempDir::new()?;
    let root = dir.path().join("partition");
    let mut log = PartitionLog::open(&root, options(115))?;
    for index in 0..3 {
        log.append_batch(1, 100 + index, &batch(&[(index, index as u8)]))?;
    }
    log.commit()?;
    let policy = Retention {
        keep_since_ms: None,
        max_bytes: Some(115),
    };
    let boundary = log.retention_boundary(log.start(), policy)?;
    assert_eq!(boundary, position(3, 2));
    assert_eq!(log.start(), position(1, 0));
    assert_eq!(log.position_at(1)?, position(1, 0));

    let mut payload = Vec::new();
    let mut visited = Vec::new();
    assert_eq!(
        log.offset_for_timestamp_from(2, 0, &mut payload, |entry, _, _| {
            visited.push(entry.position.next_offset);
            Ok(Some(entry.position.next_offset))
        })?,
        Some(2),
    );
    assert_eq!(visited, vec![2]);

    let expired = log.retention_boundary(
        boundary,
        Retention {
            keep_since_ms: Some(200),
            max_bytes: None,
        },
    )?;
    assert_eq!(expired, log.committed());
    log.append_internal(1, 0, b"retention record")?;
    log.commit()?;
    assert_eq!(log.retention_boundary(expired, policy)?.next_offset, 3);
    Ok(())
}
