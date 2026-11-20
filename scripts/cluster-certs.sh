#!/usr/bin/env bash
set -euo pipefail
umask 077

validate_id() {
    if [[ ! "$1" =~ ^[1-9][0-9]{0,2}$ ]] || (( $1 > 255 )); then
        echo "$2 must be between 1 and 255" >&2
        exit 2
    fi
}

issue_node() (
    local id="$1" stage key pin
    key="$target/node-$id.key"
    pin="$target/public/node-$id.pem"
    stage="$(mktemp -d "$target/.node-$id.XXXXXXXX")"
    trap 'rm -r -- "$stage"' EXIT

    if [[ ! -f "$pin" ]]; then
        if [[ ! -f "$key" ]]; then
            openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$stage/key.pem"
            chmod 0444 "$stage/key.pem"
            # Publish the key first so a retry can finish issuing its certificate.
            ln "$stage/key.pem" "$key" || test -f "$key"
        fi
        openssl req -new -key "$key" -subj "/CN=broker$id" -out "$stage/node.csr"
        cat > "$stage/node.conf" <<CONFIG
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = serverAuth,clientAuth
subjectAltName = DNS:raft$id,DNS:broker$id
CONFIG
        openssl x509 -req -in "$stage/node.csr" -CA "$target/public/ca.pem" \
            -CAkey "$target/ca.key" -set_serial "$id" -days 30 -extfile "$stage/node.conf" \
            -out "$stage/node.pem"
        chmod 0444 "$stage/node.pem"
        ln "$stage/node.pem" "$pin" || test -f "$pin"
    fi
    openssl verify -CAfile "$target/public/ca.pem" -purpose sslclient "$pin"
    openssl pkey -in "$key" -pubout -out "$stage/key.pub"
    openssl x509 -in "$pin" -pubkey -noout > "$stage/pin.pub"
    cmp "$stage/key.pub" "$stage/pin.pub"
)

if [[ "${1:-}" == --add ]]; then
    target="${2:?pass the existing certificate directory}"
    id="${3:?pass the new broker ID}"
    validate_id "$id" "node ID"
    test -f "$target/ca.key" && test -f "$target/public/ca.pem" || {
        echo "existing Compose CA files are required" >&2
        exit 2
    }
    issue_node "$id"
    exit
fi

target="${1:?pass a new directory for local certificates}"
nodes="${2:-3}"
validate_id "$nodes" "node count"

mkdir -- "$target"
mkdir -- "$target/public"
cat > "$target/ca.conf" <<'CONFIG'
[req]
distinguished_name = dn
prompt = no
x509_extensions = extensions
[dn]
CN = Rusta Compose CA
[extensions]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
CONFIG
openssl req -new -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
    -days 30 -config "$target/ca.conf" -keyout "$target/ca.key" -out "$target/public/ca.pem"
for ((id = 1; id <= nodes; id++)); do
    issue_node "$id"
done
touch "$target/public/key.pem"
chmod 0755 "$target/public"
chmod 0444 "$target/public/ca.pem" "$target/public/key.pem"
