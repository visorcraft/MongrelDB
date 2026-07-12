# MinHash member hash contract v1

MongrelDB hashes raw set members with XXH3-64, seed `0`.

The hashed bytes are one type-domain byte followed by the canonical payload:

| Member | Domain | Payload |
|---|---:|---|
| String | `0x01` | exact UTF-8 bytes |
| Number | `0x02` | canonical JSON number text |
| Boolean | `0x03` | `0x00` or `0x01` |

MongrelDB does not normalize Unicode, case-fold, stem, or shingle members.
Applications own preprocessing and should store its version beside the set.

Raw `u64` query hashes are advanced, version-sensitive input. Public clients
should send raw members. Index checkpoint format v2 rebuilds old derived
MinHash state from stored row members.

## LSH candidate probability

The current 128 permutations and 32 bands use four signature rows per band:

```text
P(candidate | J) = 1 - (1 - J^4)^32
```

`P = 0.5` near `J = 0.3826`. Reference probabilities are about `0.050` at
`J = 0.20`, `0.229` at `0.30`, `0.490` at `0.38`, `0.564` at `0.40`, `0.873`
at `0.50`, and `0.99985` at `0.70`.

Exact verification removes false positives and improves final ranking. It does
not recover true neighbors omitted by LSH candidate generation.
