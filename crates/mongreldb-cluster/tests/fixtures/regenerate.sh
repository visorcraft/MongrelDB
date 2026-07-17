#!/usr/bin/env bash
# Regenerates the mTLS test fixtures used by crates/mongreldb-cluster/tests/transport.rs.
# Requires OpenSSL 3.x. All material is TEST-ONLY; never use it in production.
set -euo pipefail
cd "$(dirname "$0")"

# Node-certificate binding scheme (see cluster/src/network.rs): the subject CN
# and the SAN dNSName are both `node-<lowercase hex of the 16-byte NodeId>.mongreldb.cluster`.
NODE1=01010101010101010101010101010101
NODE2=02020202020202020202020202020202
NODE3=03030303030303030303030303030303
ROGUE=09090909090909090909090909090909

make_ca() { # $1 = prefix, $2 = CN
    openssl ecparam -genkey -name prime256v1 -noout -out "$1.key.pem"
    openssl req -x509 -new -key "$1.key.pem" -out "$1.crt.pem" -days 3650 -sha256 \
        -subj "/CN=$2" \
        -addext "basicConstraints=critical,CA:TRUE" \
        -addext "keyUsage=critical,keyCertSign,cRLSign"
}

make_node() { # $1 = prefix, $2 = node hex id, $3 = signing CA prefix
    local name="node-$2.mongreldb.cluster"
    openssl ecparam -genkey -name prime256v1 -noout -out "$1.key.pem"
    openssl req -new -key "$1.key.pem" -out "$1.csr.pem" -subj "/CN=$name"
    cat > "$1.ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:$name
EOF
    openssl x509 -req -in "$1.csr.pem" -CA "$3.crt.pem" -CAkey "$3.key.pem" -CAcreateserial \
        -out "$1.crt.pem" -days 3650 -sha256 -extfile "$1.ext"
    rm -f "$1.csr.pem" "$1.ext" "$3.crt.srl"
}

make_ca ca mongreldb-test-ca
make_node node1 "$NODE1" ca
make_node node2 "$NODE2" ca
make_node node3 "$NODE3" ca
# A second CA and a certificate it signs: valid X.509, wrong trust domain.
make_ca rogue-ca mongreldb-rogue-ca
make_node rogue-node "$ROGUE" rogue-ca
