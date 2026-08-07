# encoding/binary ↔ Manual Bit-Shift Mapping

All Kafka wire fields are **big-endian**. `encoding/binary` uses `binary.BigEndian` to read/write them.

---

## Reading (parsing bytes → Go types)

| Go type wanted | Manual shift | `encoding/binary` |
|---|---|---|
| `int16` from 2 bytes | `int16(b[0])<<8 \| int16(b[1])` | `int16(binary.BigEndian.Uint16(b[0:2]))` |
| `int32` from 4 bytes | `int32(b[0])<<24 \| int32(b[1])<<16 \| int32(b[2])<<8 \| int32(b[3])` | `int32(binary.BigEndian.Uint32(b[0:4]))` |
| `int64` from 8 bytes | `int64(b[0])<<56 \| ... \| int64(b[7])` | `int64(binary.BigEndian.Uint64(b[0:8]))` |
| `uint32` from 4 bytes | `uint32(b[0])<<24 \| ...` | `binary.BigEndian.Uint32(b[0:4])` |

The rule: `binary.BigEndian.UintXX(slice)` returns **unsigned**. Cast to signed afterwards when needed (e.g. `int32(...)`, `int64(...)`).

### Concrete examples from this codebase

```
// --- RecieveRequest (kafka_helper.go) ---

// BEFORE
msg_size    := int32(buf[0])<<24 | int32(buf[1])<<16 | int32(buf[2])<<8 | int32(buf[3])
api_key     := int16(buf[4])<<8 | int16(buf[5])
api_version := int16(buf[6])<<8 | int16(buf[7])
corr_id     := int32(buf[8])<<24 | int32(buf[9])<<16 | int32(buf[10])<<8 | int32(buf[11])
clientIDLen := int(int16(buf[12])<<8 | int16(buf[13]))

// AFTER
msg_size    := int32(binary.BigEndian.Uint32(buf[0:4]))
api_key     := int16(binary.BigEndian.Uint16(buf[4:6]))
api_version := int16(binary.BigEndian.Uint16(buf[6:8]))
corr_id     := int32(binary.BigEndian.Uint32(buf[8:12]))
clientIDLen := int(int16(binary.BigEndian.Uint16(buf[12:14])))


// --- LogMetadataMapByUuid (kafka_describe_topic.go) ---

// BEFORE
batch1Len := int(content[8])<<24 | int(content[9])<<16 | int(content[10])<<8 | int(content[11])
batchLen  := int(content[s+8])<<24 | int(content[s+9])<<16 | int(content[s+10])<<8 | int(content[s+11])

// AFTER
batch1Len := int(binary.BigEndian.Uint32(content[8:12]))
batchLen  := int(binary.BigEndian.Uint32(content[s+8:s+12]))


// --- parseBatchRecords (kafka_describe_topic.go) ---

// BEFORE
PartitionIndex: int32(content[c])<<24 | int32(content[c+1])<<16 | int32(content[c+2])<<8 | int32(content[c+3])
LeaderID:       int32(content[c])<<24 | ...

// AFTER
PartitionIndex: int32(binary.BigEndian.Uint32(content[c:c+4]))
LeaderID:       int32(binary.BigEndian.Uint32(content[c:c+4]))


// --- ConstructResponseForFetch (kafka_fetch.go) ---

// BEFORE
baseOff      := int64(r[p])<<56 | int64(r[p+1])<<48 | ... | int64(r[p+7])
batchLen     := int(r[p+8])<<24 | int(r[p+9])<<16 | int(r[p+10])<<8 | int(r[p+11])
recordsCount := int64(r[p+57])<<24 | int64(r[p+58])<<16 | int64(r[p+59])<<8 | int64(r[p+60])

// AFTER
baseOff      := int64(binary.BigEndian.Uint64(r[p:p+8]))
batchLen     := int(binary.BigEndian.Uint32(r[p+8:p+12]))
recordsCount := int64(binary.BigEndian.Uint32(r[p+57:p+61]))
```

---

## Writing (appending Go types → bytes)

`binary.BigEndian.AppendUintXX(dst, val)` appends big-endian bytes to a slice in-place (no alloc if `dst` has capacity). Available since Go 1.21.

| Go type | Manual shift | `encoding/binary` |
|---|---|---|
| `int16` → 2 bytes | `append(b, byte(v>>8), byte(v))` | `binary.BigEndian.AppendUint16(b, uint16(v))` |
| `int32` → 4 bytes | `append(b, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))` | `binary.BigEndian.AppendUint32(b, uint32(v))` |
| `int64` → 8 bytes | `append(b, byte(v>>56), ..., byte(v))` | `binary.BigEndian.AppendUint64(b, uint64(v))` |

The rule: `AppendUintXX` takes **unsigned** — cast signed values first (`uint16(v)`, `uint32(v)`, `uint64(v)`).

### Concrete examples from this codebase

```
// --- appendInt16/32/64 helpers (kafka_helper.go) ---

// BEFORE
func appendInt16(res []byte, v int16) []byte {
    return append(res, byte(v>>8), byte(v))
}
func appendInt32(res []byte, v int32) []byte {
    return append(res, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}
func appendInt64(res []byte, v int64) []byte {
    return append(res, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
                  byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// AFTER
func appendInt16(res []byte, v int16) []byte {
    return binary.BigEndian.AppendUint16(res, uint16(v))
}
func appendInt32(res []byte, v int32) []byte {
    return binary.BigEndian.AppendUint32(res, uint32(v))
}
func appendInt64(res []byte, v int64) []byte {
    return binary.BigEndian.AppendUint64(res, uint64(v))
}


// --- SendResp frame length prefix (kafka_helper.go) ---

// BEFORE
frame := []byte{byte(msgSize>>24), byte(msgSize>>16), byte(msgSize>>8), byte(msgSize)}

// AFTER
frame := binary.BigEndian.AppendUint32(nil, uint32(msgSize))
```

---

## Why `AppendUint32(nil, v)` works as "create new"

`binary.BigEndian.AppendUint32(nil, v)` allocates a fresh 4-byte slice — `nil` is a valid empty slice in Go. It is equivalent to:
```go
buf := make([]byte, 0, 4)
buf = binary.BigEndian.AppendUint32(buf, v)
```

---

## Endianness reminder

All Kafka protocol fields use **network byte order = big-endian** (most significant byte first). `binary.BigEndian` matches exactly. Never use `binary.LittleEndian` for Kafka wire data.
