# Operate Rusta

Rusta runs as UID and GID 65532. Give each broker its own named volume at `/var/lib/rusta`. Preserve that volume across container replacement. In a cluster, each volume belongs to its original broker ID, cluster ID and bootstrap voter set. Store the Compose project name, resolved listener addresses, broker inventory, image tag and digest, and certificate inventory with the deployment configuration.

## Deploy a versioned image

Use Docker Engine with Compose v2 on Linux, or Docker Desktop with Compose v2 on macOS. The supplied Compose files require support for `gw_priority` when using a cluster. The release workflow prepares a single version tag for Linux `amd64` and `arm64`. Once a tag is available in GHCR, select it with `RUSTA_IMAGE` and use the same tag or image digest on every broker.

```sh
export RUSTA_IMAGE=ghcr.io/aartmoon/rusta:0.1.0
docker compose pull
docker compose up -d --no-build --wait
```

For a cluster, generate and mount [peer certificates](../README.md#run-a-local-cluster) and use `-f compose.cluster.yaml` on both commands. Broker startup needs persistent local storage and, in cluster mode, the other Rusta peers. Copy the image and store the configuration and certificates locally for disconnected deployments.

## Configure listeners and storage

| Variable | Default and scope |
| --- | --- |
| `RUSTA_DATA` | `/var/lib/rusta` in the image and `data` for the standalone binary. |
| `RUSTA_BIND`, `RUSTA_ADVERTISED` | Container bind `0.0.0.0:9092` and advertised endpoint `127.0.0.1:9092`. Advertised endpoints must be reachable by the clients using that listener. |
| `RUSTA_INTERNAL_BIND`, `RUSTA_INTERNAL_ADVERTISED` | Optional paired Kafka listener. The Compose files set port 29092 and advertise each service name. |
| `RUSTA_IMAGE`, `RUSTA_PORT`, `RUSTA_PORT_1` through `RUSTA_PORT_3` | Compose image selector and loopback host ports. Default ports are 9092 for a singleton and 19092 through 19094 for the three-broker file. |
| `RUSTA_NODE_ID`, `RUSTA_CLUSTER_ID` | Setting a node ID enables cluster mode. Assign each node a unique numeric ID and all peers the same non-nil cluster UUID. Set a unique cluster UUID for each deployment. |
| `RUSTA_CLUSTER_PEERS`, `RUSTA_BOOTSTRAP_VOTERS` | Semicolon-separated `id,raft,host,internal` peer definitions and a comma-separated original voter list. The voter list defaults to all configured peers and stays fixed after bootstrap, including when the live membership changes. |
| `RUSTA_RAFT_BIND`, `RUSTA_CLUSTER_CERTS`, `RUSTA_CLUSTER_PKI` | Raft bind defaults to `0.0.0.0:7000`. The Compose host path `RUSTA_CLUSTER_PKI` supplies peer pins and keys to `/etc/rusta/pki`. |
| `RUSTA_GROUP_OFFSET_RETENTION_MS` | 604800000 ms after a group empties. The singleton profile passes it through. Add it to the broker service environment to tune it in a cluster. |
| `RUSTA_CLUSTER_SNAPSHOT_INTERVAL` | 4096 Raft entries between snapshots. Use a positive integer. |
| `RUSTA_CLUSTER_PRODUCER_RETENTION_MS` | 86400000 ms for inactive cluster producer histories. Configure a positive duration in the broker environment, consistently across peers. |
| `RUSTA_DEBUG_CONNECTIONS` | Set to `1` temporarily to log Kafka connection errors. |

## Plan upgrades and scaling

Schedule a coordinated stop when upgrading from the initial cluster revisions, changing the storage or Raft format, or rotating peer pins. Plan a full stop for new versions until their release notes explicitly qualify a rolling upgrade. Pause writers and consumers, record sample topic IDs, partition offsets and group commits, stop every broker, and [back up](#back-up-and-restore) the deployment. Pull the candidate and recreate containers with the original volumes, configuration and certificates. Verify topic IDs, reads, writes and consumer resume before reopening traffic. Roll back by restoring the full stopped backup on the original image. Restoring that backup will drop all writes made after it was taken.

The repository began with a storage crate at `2fd1a65`. The first installable singleton is `abb95e7`, and the cluster baselines are `7281ca3` and `639b77d`. The [upgrade smoke](../tests/release/upgrade.sh) builds these revisions and exercises a coordinated switch to the current image on their named volumes. The [offline singleton migration](../README.md#migrate-single-node-data-to-a-cluster) is a separate workflow for changing deployment mode.

For day-to-day capacity changes, follow the [add broker](../README.md#add-a-broker-to-a-running-cluster), [rebalance](../README.md#rebalance-replicas-and-transfer-leadership) and [remove broker](../README.md#remove-a-broker-from-a-running-cluster) procedures. Check `peer-inventory` and `relocation-status` from a live broker between steps. Maintain a voting majority and wait for relocation to finish before removing a broker. Retain retired broker IDs and public pins for replay.

## Back up and restore

Pause clients and stop all brokers together. Archive every data volume from that stop, plus the image digest, cluster configuration, and the corresponding certificate inventory and keys. Protect private keys and backups with restricted access. Back up the whole volume, including metadata, logs, group state and identity. Use the same procedure for a singleton with one volume.

The following example runs from the Compose directory with the original image already available locally. Repeat the archive command for each volume. Supply the actual volume names from the deployment.

```sh
docker compose stop
umask 077
archive=broker-data.tar
volume=rusta_broker-data
docker run --rm --user 0:0 --entrypoint tar \
  -v "$volume:/data:ro" "$RUSTA_IMAGE" --numeric-owner -cpf - -C /data . > "$archive"
tar -tf "$archive" >/dev/null
shasum -a 256 "$archive" > "$archive.sha256"
```

Check that an expected volume exists with `docker volume inspect` before archiving it. For a cluster, use the cluster Compose file for the stop. Retain the archives as one backup set. Verify checksums before restoring and exercise restores in an isolated Compose project before relying on the backups.

Restore each archive to a fresh named volume. This command requires an empty volume and preserves the archived numeric ownership. Bind the restored volumes to the matching services in Compose before startup, then restore the original image, roster and pins.

```sh
restore_volume=rusta_restore_broker-data
shasum -a 256 -c "$archive.sha256"
docker volume create "$restore_volume"
docker run --rm -i --user 0:0 --entrypoint sh -v "$restore_volume:/data" \
  "$RUSTA_IMAGE" -ec 'test -z "$(ls -A /data)"; tar --numeric-owner -xpf - -C /data; sync' < "$archive"
```

For the singleton, save this override as `restore.yaml` and start with `docker compose -f compose.yaml -f restore.yaml -p rusta-restore up -d --no-build --wait`. In a cluster override each `dataN` volume with its corresponding restored volume and use the cluster Compose file.

```yaml
volumes:
  broker-data:
    external: true
    name: rusta_restore_broker-data
```

Keep client traffic paused during validation. On a cluster wait for controller election and check each broker ID, then compare the recorded topic IDs, retained records, log-start and end offsets, and committed group offsets. Verify writes and a group resume with a test topic before reopening traffic.

## Monitor and respond to incidents

- Check `docker compose ps`, `docker compose logs --tail 200` and `docker stats`. The healthcheck uses Kafka ApiVersions on the local listener. Check Kafka metadata, production to a dedicated test topic and consumption from it to verify end-to-end availability and quorum.
- Track filesystem space and inodes for the Docker volume filesystem, cgroup memory and CPU, Kafka request latency and errors, consumer lag, and unavailable partition leaders. Use client-side metrics and Docker or host monitoring to collect these measurements. The broker has no Prometheus endpoint.
- For a failed peer, check process logs, container resources, peer reachability, and certificate expiry and pins. Keep the surviving majority online, and restart the failed broker with its volume and identity. Verify its return with metadata and a read/write probe.
- On disk exhaustion, pause producers, record available capacity, and recover space outside the broker's stored data or expand the volume filesystem. Preserve the data files. Check logs and run a Kafka read and write before reopening traffic.
- If clients time out during Produce, the write outcome may be unknown. Use the same idempotent producer for retries. Compare topic offsets and application record identifiers when investigating duplicates or gaps.

## Client security

Both Kafka listeners use PLAINTEXT. Kafka clients on an accessible listener can read, write, commit offsets and call supported Admin APIs without broker-side authentication or ACLs. Keep loopback publishing and isolate the Compose network from untrusted workloads. Keep Raft ports private. Peer mTLS authenticates broker nodes and uses per-node certificate pins. Public or multi-tenant deployment requires a separately agreed Kafka client security profile.
