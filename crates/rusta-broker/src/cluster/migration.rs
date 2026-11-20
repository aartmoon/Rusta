use std::fs::{self, File};
use std::io;
use std::path::Path;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    catalog::{Catalog, DEFAULT_RETENTION_MS},
    cluster::{
        Settings,
        disk::{Disk, replace},
        producers::Producers,
    },
    groups, lock_data,
    partition::MAX_BATCH,
};

use super::{Event, GROUP_SHARDS, MAX_CLUSTER_PARTITIONS, ShardRecord, coordinator, group_shard};

const FENCE: &str = "migration-source-fence";
const TARGET: &str = "migration-target";
const COMPLETE: &str = "migration-complete";
const BUILDING: &str = "migration-raft.tmp";

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct Transfer {
    source: Uuid,
    destination: Uuid,
    voters: Vec<u64>,
}

pub fn check_single_node(root: &Path) -> Result<()> {
    ensure!(
        !root.join(FENCE).exists(),
        "source volume is fenced by a cluster migration"
    );
    ensure!(
        !root.join(TARGET).exists(),
        "migration target must run in cluster mode"
    );
    Ok(())
}

pub fn check_target(root: &Path) -> Result<()> {
    if root.join(TARGET).exists() {
        ensure!(
            root.join(COMPLETE).is_file(),
            "migration target is incomplete"
        );
    }
    Ok(())
}

pub fn run(source: &Path, target: &Path, settings: &Settings) -> Result<()> {
    ensure!(
        source.join("catalog/manifest").is_file() && !source.join("cluster-identity").exists(),
        "source must be a single-node Rusta volume"
    );
    ensure!(
        settings.bootstrap_voters.contains(&settings.id),
        "migration target must be a bootstrap voter"
    );
    fs::create_dir_all(target)?;
    ensure!(
        source.canonicalize()? != target.canonicalize()?,
        "use a separate target volume"
    );
    let _source_lock = lock_data(source).context("stop the source broker before migration")?;
    let _target_lock = lock_data(target).context("stop the target broker before migration")?;
    ensure!(
        !target.join("catalog").exists(),
        "target contains a single-node catalog"
    );

    let catalog = Catalog::open(source).context("recover the source volume")?;
    validate(&catalog)?;
    let transfer = Transfer {
        source: catalog.cluster_id,
        destination: settings.cluster,
        voters: settings.bootstrap_voters.clone(),
    };
    let fence = serde_json::to_vec(&transfer)?;
    let fenced = match fs::read(source.join(FENCE)) {
        Ok(previous) => {
            ensure!(
                previous == fence,
                "source is already fenced for another cluster"
            );
            true
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    let identity = serde_json::to_vec(&(&transfer, settings.id))?;
    match fs::read(target.join(TARGET)) {
        Ok(previous) => ensure!(previous == identity, "target belongs to another migration"),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            ensure!(
                !target.join("cluster-identity").exists()
                    && !target.join("raft").exists()
                    && !target.join(BUILDING).exists()
                    && !target.join(COMPLETE).exists(),
                "migration requires a fresh target volume"
            );
            replace(target, TARGET, &identity)?;
        }
        Err(error) => return Err(error.into()),
    }

    if !fenced {
        replace(source, FENCE, &fence)?;
    }
    if target.join(COMPLETE).is_file() {
        settings.ensure_identity(target)?;
        return Ok(());
    }

    let building = target.join(BUILDING);
    if !target.join("raft").is_dir() {
        if building.exists() {
            fs::remove_dir_all(&building)?;
        }
        fs::create_dir(&building)?;
        seed(&building, &catalog, &transfer.voters)?;
        File::open(&building)?.sync_all()?;
        settings.ensure_identity(target)?;
        fs::rename(&building, target.join("raft"))?;
        File::open(target)?.sync_all()?;
    }
    settings.ensure_identity(target)?;
    replace(target, COMPLETE, b"ready\n")?;
    Ok(())
}

fn validate(catalog: &Catalog) -> Result<()> {
    ensure!(
        catalog.partition_count <= MAX_CLUSTER_PARTITIONS,
        "source exceeds cluster partition limit"
    );
    ensure!(
        catalog.last_reserved_producer() <= i64::MAX - (4096 << 8),
        "source producer IDs leave no allocation range"
    );
    ensure!(
        catalog
            .topics
            .values()
            .all(|topic| !topic.config.allow_weak_acks),
        "disable weak acknowledgements on source topics before migration"
    );
    let mut groups_per_shard = [0; GROUP_SHARDS];
    for group in catalog.groups.entries.keys() {
        let shard = coordinator(group).partition as usize - 1;
        groups_per_shard[shard] += 1;
        ensure!(
            groups_per_shard[shard] <= groups::MAX_GROUPS / GROUP_SHARDS,
            "source exceeds group capacity on shard {shard}"
        );
    }
    Ok(())
}

fn seed(root: &Path, catalog: &Catalog, voters: &[u64]) -> Result<()> {
    let mut control = Disk::seed_log(&root.join("control"), 0)?;
    let append = |log: &mut rusta_log::PartitionLog, event: &Event| -> Result<()> {
        log.append_internal(1, 0, &serde_json::to_vec(event)?)?;
        Ok(())
    };
    if catalog.last_reserved_producer() > 0 {
        append(
            &mut control,
            &Event::ImportedProducers(catalog.last_reserved_producer()),
        )?;
    }
    append(
        &mut control,
        &Event::GroupShards(std::array::from_fn(|_| voters.to_vec())),
    )?;
    for topic in catalog.topics.values() {
        let mut config = topic.config.clone();
        config.retention_ms.get_or_insert(DEFAULT_RETENTION_MS);
        let starts = topic
            .partitions
            .iter()
            .map(|partition| partition.log.start().next_offset)
            .collect::<Vec<_>>();
        for (index, partition) in topic.partitions.iter().enumerate() {
            let mut log =
                Disk::seed_log(&root.join(format!("{}-{index}", config.id)), starts[index])?;
            let end = partition.log.committed();
            let mut next = partition.log.start().next_index;
            while next < end.next_index {
                next = partition.log.visit_entries(
                    next,
                    end.next_index,
                    2 * MAX_BATCH as u64,
                    |entry, batch| {
                        if entry.record_count != 0 {
                            if log.end().next_offset != entry.position.next_offset {
                                return Err(io::Error::other(
                                    "source Kafka offsets are not contiguous",
                                ));
                            }
                            log.append_batch(1, entry.append_timestamp_ms, batch)?;
                        }
                        Ok(())
                    },
                )?;
            }
            ensure!(
                log.end().next_offset == end.next_offset,
                "copied log end offset disagrees with source"
            );
            if !partition.producer_histories().is_empty() {
                let record = Producers::imported_record(partition.producer_histories())?;
                log.append_internal(1, 0, &record)?;
            }
            log.commit()?;
        }
        append(
            &mut control,
            &Event::ImportedTopic {
                replicas: vec![voters.to_vec(); config.partitions],
                config,
                starts,
            },
        )?;
    }
    seed_groups(root, catalog)?;
    control.commit()?;
    Ok(())
}

fn seed_groups(root: &Path, catalog: &Catalog) -> Result<()> {
    let mut shards = Vec::with_capacity(GROUP_SHARDS);
    for index in 0..GROUP_SHARDS {
        let key = group_shard(index);
        shards.push(Disk::seed_log(
            &root.join(format!("{}-{}", key.topic, key.partition)),
            0,
        )?);
    }
    let groups: std::collections::BTreeMap<_, _> = catalog.groups.entries.iter().collect();
    for (name, group) in groups {
        let log = &mut shards[coordinator(name).partition as usize - 1];
        let mut append = |event| -> Result<()> {
            log.append_internal(1, 0, &serde_json::to_vec(&ShardRecord::V1(vec![event]))?)?;
            Ok(())
        };
        if group.generation > 0 {
            append(groups::Event::Generation {
                group: name.clone(),
                id: group.generation,
            })?;
        }
        let mut offsets = group.offsets.iter().collect::<Vec<_>>();
        offsets.sort_unstable_by_key(|(key, _)| (key.topic, key.partition));
        for (key, value) in offsets {
            append(groups::Event::Commit {
                group: name.clone(),
                offsets: vec![(*key, value.clone())],
                empty_since: group.empty_since,
            })?;
        }
        if let Some(since) = group.empty_since {
            append(groups::Event::Empty {
                group: name.clone(),
                since,
            })?;
        }
    }
    for shard in &mut shards {
        shard.commit()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::Peer;

    #[test]
    fn migration_retries_unpublished_and_published_target_phases() -> Result<()> {
        let scratch = tempfile::tempdir()?;
        let source = scratch.path().join("source");
        let target = scratch.path().join("target");
        let catalog = Catalog::open(&source)?;
        let source_id = catalog.cluster_id;
        drop(catalog);
        let settings = Settings {
            id: 1,
            cluster: Uuid::new_v4(),
            peers: [Peer::parse("1,raft1:7000,localhost:9092,broker1:29092")?].into(),
            bootstrap_voters: vec![1],
            staged_peers: Default::default(),
            bind: "127.0.0.1:0".parse()?,
            certificates: scratch.path().join("pki"),
        };
        run(&source, &target, &settings)?;
        assert!(check_single_node(&source).is_err());
        check_target(&target)?;

        // Failure after the Raft directory was published but before readiness.
        fs::remove_file(target.join(COMPLETE))?;
        assert!(check_target(&target).is_err());
        run(&source, &target, &settings)?;
        check_target(&target)?;

        // Failure before publishing the staged Raft directory.
        fs::remove_file(target.join(COMPLETE))?;
        fs::rename(target.join("raft"), target.join(BUILDING))?;
        fs::write(target.join(BUILDING).join("interrupted"), b"partial")?;
        assert!(check_target(&target).is_err());
        run(&source, &target, &settings)?;
        check_target(&target)?;
        assert!(!target.join("raft/interrupted").exists());
        let fence: Transfer = serde_json::from_slice(&fs::read(source.join(FENCE))?)?;
        assert_eq!(fence.source, source_id);
        assert_eq!(fence.destination, settings.cluster);
        Ok(())
    }
}
