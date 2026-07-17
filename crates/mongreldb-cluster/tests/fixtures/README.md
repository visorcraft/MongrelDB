# mTLS test fixtures (TEST-ONLY — never use in production)

Certificates and keys for the cluster transport tests
(`crates/mongreldb-cluster/tests/transport.rs` and the `network` module's unit
tests). Everything here is throwaway test material with 10-year validity.

## Node-certificate binding scheme

A node certificate binds a durable 128-bit `NodeId`: the subject Common Name
and the SAN dNSName are both

```text
node-<lowercase hex of the 16-byte NodeId>.mongreldb.cluster
```

Fixtures (see `node_cert_name` in `crates/mongreldb-cluster/src/network.rs`):

| File                            | Identity                                        |
| ------------------------------- | ----------------------------------------------- |
| `ca.crt.pem` / `ca.key.pem`     | cluster test CA (`mongreldb-test-ca`)           |
| `node1.{crt,key}.pem`           | `NodeId` = `01` x 16, signed by the test CA     |
| `node2.{crt,key}.pem`           | `NodeId` = `02` x 16, signed by the test CA     |
| `node3.{crt,key}.pem`           | `NodeId` = `03` x 16, signed by the test CA     |
| `rogue-ca.{crt,key}.pem`        | a second CA (`mongreldb-rogue-ca`)              |
| `rogue-node.{crt,key}.pem`      | `NodeId` = `09` x 16, signed by the rogue CA    |

`rogue-node` is valid X.509 with the right name form but the wrong trust
domain; the rejection tests use it for the wrong-CA case.

## Regenerating

Requires OpenSSL 3.x. From this directory:

```bash
./regenerate.sh
```

The script (kept beside this README) recreates every file in place: EC P-256
keys, a self-signed CA per trust domain, and node certificates signed with
`basicConstraints=CA:FALSE`, `keyUsage=digitalSignature`, and
`extendedKeyUsage=serverAuth,clientAuth`, carrying the SAN above.
