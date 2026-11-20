#!/usr/bin/env bash
set -euo pipefail

nodes="${1:?pass the number of brokers}"
base_port="${2:-19092}"
bootstrap="${3:-}"
if [[ ! "$nodes" =~ ^[1-9][0-9]{0,2}$ ]] || (( nodes > 255 )); then
    echo "node count must be between 1 and 255" >&2
    exit 2
fi
if [[ ! "$base_port" =~ ^[1-9][0-9]{0,4}$ ]] || (( base_port + nodes - 1 > 65535 )); then
    echo "the requested host port range is unavailable" >&2
    exit 2
fi

if [[ -n "$bootstrap" && ! "$bootstrap" =~ ^[1-9][0-9]{0,2}(,[1-9][0-9]{0,2})*$ ]]; then
    echo "bootstrap voters must be a comma-separated list of broker IDs" >&2
    exit 2
fi

peers=""
for ((id = 1; id <= nodes; id++)); do
    port=$((base_port + id - 1))
    peers="${peers}${peers:+;}${id},raft${id}:7000,127.0.0.1:${port},broker${id}:29092"
done

cat <<YAML
x-broker: &broker
  build: .
  image: \${RUSTA_IMAGE:-rusta:local}
  cpus: 1.0
  mem_limit: 512m
  healthcheck:
    test: [CMD, /usr/local/bin/rusta, healthcheck]
    interval: 2s
    timeout: 4s
    retries: 10

x-env: &cluster-env
  RUSTA_CLUSTER_ID: \${RUSTA_CLUSTER_ID:-4e90dcf1-72c1-491e-99a1-72d58be78c35}
  RUSTA_CLUSTER_PEERS: "$peers"
  RUSTA_CLUSTER_CERTS: /etc/rusta/pki
  RUSTA_INTERNAL_BIND: 0.0.0.0:29092
  RUSTA_CLUSTER_SNAPSHOT_INTERVAL: "\${RUSTA_CLUSTER_SNAPSHOT_INTERVAL:-4096}"
YAML
if [[ -n "$bootstrap" ]]; then
    printf '  RUSTA_BOOTSTRAP_VOTERS: "%s"\n' "$bootstrap"
fi
printf '\nservices:\n'
for ((id = 1; id <= nodes; id++)); do
    port=$((base_port + id - 1))
    cat <<YAML
  broker$id:
    <<: *broker
    environment:
      <<: *cluster-env
      RUSTA_NODE_ID: "$id"
      RUSTA_ADVERTISED: 127.0.0.1:$port
      RUSTA_INTERNAL_ADVERTISED: broker$id:29092
    ports: ["127.0.0.1:$port:9092"]
    networks:
      default:
        gw_priority: 1
      raft:
        aliases: [raft$id]
    volumes:
      - data$id:/var/lib/rusta
      - \${RUSTA_CLUSTER_PKI:?generate cluster certificates}/public:/etc/rusta/pki:ro
      - \${RUSTA_CLUSTER_PKI}/node-$id.key:/etc/rusta/pki/key.pem:ro
YAML
done

printf 'volumes:\n'
for ((id = 1; id <= nodes; id++)); do printf '  data%s:\n' "$id"; done
printf 'networks:\n  raft:\n    internal: true\n'
