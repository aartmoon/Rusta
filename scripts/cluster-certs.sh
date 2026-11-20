#!/usr/bin/env bash
set -euo pipefail
target="${1:?pass a new directory for local certificates}"
nodes="${2:-3}"
if [[ ! "$nodes" =~ ^[1-9][0-9]{0,2}$ ]] || (( nodes > 255 )); then
    echo "node count must be between 1 and 255" >&2
    exit 2
fi
umask 077
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
    openssl req -new -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
        -subj "/CN=broker$id" -keyout "$target/node-$id.key" -out "$target/node-$id.csr"
    cat > "$target/node-$id.conf" <<CONFIG
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature
extendedKeyUsage = serverAuth,clientAuth
subjectAltName = DNS:raft$id,DNS:broker$id
CONFIG
    openssl x509 -req -in "$target/node-$id.csr" -CA "$target/public/ca.pem" \
        -CAkey "$target/ca.key" -set_serial "$id" -days 30 -extfile "$target/node-$id.conf" \
        -out "$target/public/node-$id.pem"
    chmod 0444 "$target/node-$id.key" "$target/public/node-$id.pem"
done
touch "$target/public/key.pem"
chmod 0755 "$target/public"
chmod 0444 "$target/public/ca.pem" "$target/public/key.pem"
