# WA6 — Parse Correlation ID

## Input

A Kafka client connects over TCP and sends a **raw binary message** (no JSON, no text).
The first message looks like this:

```
Byte offset:   0    1    2    3    4    5    6    7    8    9   10   11  ...
Hex:          00   00   00   23   00   12   00   04   6f   7f   c6   61  ...
Field:        |-- message_size --|  |- api_key-|  |-api_ver-|  |-- correlation_id --|
```

| Field                 | Type    | Bytes  | Example (hex) | Decoded value |
| --------------------- | ------- | ------ | ------------- | ------------- |
| `message_size`        | `INT32` | 0 – 3  | `00 00 00 23` | 35            |
| `request_api_key`     | `INT16` | 4 – 5  | `00 12`       | 18            |
| `request_api_version` | `INT16` | 6 – 7  | `00 04`       | 4             |
| `correlation_id`      | `INT32` | 8 – 11 | `6f 7f c6 61` | 1870644833    |

Everything uses **big-endian** byte order (most significant byte first).

---

## Output

Echo back 8 bytes over the same TCP connection:

```
Byte offset:   0    1    2    3    4    5    6    7
Hex:          00   00   00   00   6f   7f   c6   61
Field:        |-- message_size --|  |-- correlation_id --|
              (hardcoded 0)          (copied from request)
```

| Field            | Type    | Bytes | Value                          |
| ---------------- | ------- | ----- | ------------------------------ |
| `message_size`   | `INT32` | 0 – 3 | `0` (any value works here)     |
| `correlation_id` | `INT32` | 4 – 7 | Same value received in request |

---

## High-Level Steps

1. **Accept** a TCP connection on port `9092`.
2. **Read** the raw bytes sent by the client into a buffer.
3. **Decode** the `correlation_id` from bytes 8–11 (big-endian INT32).
4. **Encode** the response: 4 zero bytes for `message_size`, then the 4 bytes of `correlation_id` in big-endian order.
5. **Write** those 8 bytes back over the connection.

---

## Bit Manipulation Deep Dive

### Why bit manipulation at all?

TCP gives you a flat stream of `byte` values (0–255 each). But `correlation_id` is a **32-bit integer** — it spans 4 consecutive bytes. You need to **reassemble** those 4 bytes into one number.

### Decoding: 4 bytes → 1 INT32 (big-endian read)

A 32-bit integer has **32 bit-slots, numbered 0 (rightmost/lowest) → 31 (leftmost/highest)**:

```
bit position:  31 .. 24 | 23 .. 16 | 15 .. 8 | 7 .. 0
                byte 0  |  byte 1  |  byte 2 |  byte 3
```

Each byte occupies 8 slots. Byte 0 owns the HIGH end (bits 31–24), byte 3 owns the LOW end (bits 7–0).

**Why does `correlation_id` start at `buf[8]`?**
Because the fields before it consume exactly 8 bytes:

```
buf[0–3]  = message_size        (INT32, 4 bytes)
buf[4–5]  = request_api_key     (INT16, 2 bytes)
buf[6–7]  = request_api_version (INT16, 2 bytes)
buf[8–11] = correlation_id      (INT32, 4 bytes) ← 4+2+2 = offset 8
```

**Why `0x6f`?** It's just the example value from the Kafka docs — a random ID the client picked.
Any 32-bit number can appear here. The server's only job is to echo it back unchanged.

Big-endian means the **first byte received is the most significant** (goes into the high end):

```
buf[8]  = 0x6f  → must land in bits 31–24  (the highest byte slot)
buf[9]  = 0x7f  → must land in bits 23–16
buf[10] = 0xc6  → must land in bits 15–8
buf[11] = 0x61  → must land in bits  7–0   (the lowest byte slot)
```

> ### 💡 The core insight — why shifting exists at all
>
> Kafka sends `correlation_id = 1870644833` encoded as 4 bytes in order: `6f 7f c6 61`.
>
> When you read those 4 bytes, each is just a small number (0–255):
>
> - `0x6f` = 111
> - `0x7f` = 127
> - `0xc6` = 198
> - `0x61` = 97
>
> If you naively add them: `111 + 127 + 198 + 97 = 533` ← **completely wrong.**
>
> The bytes are **not meant to be added directly**. They are **digits of one big number**, and like decimal digits, their _position_ determines their _weight_:
>
> ```
> 0x6f × 256³  =  111 × 16777216  =  1862270976   ("hundred-millions" digit)
> 0x7f × 256²  =  127 ×    65536  =     8257536   ("millions" digit)
> 0xc6 × 256¹  =  198 ×      256  =       50688   ("hundreds" digit)
> 0x61 × 256⁰  =   97 ×        1  =          97   ("ones" digit)
>                                  ─────────────
>                                  1870644833  ✓
> ```
>
> **`<< 8` is the binary way of saying `× 256`.** So `<< 24` = `× 256³`, `<< 16` = `× 256²`, etc.
>
> Without shifting, you'd reconstruct the wrong number and echo back a `correlation_id` the client never sent.

To reconstruct the original 32-bit value, shift each byte into its correct bit position then OR them together:

```go
correlationID := int32(buf[8])<<24 | int32(buf[9])<<16 | int32(buf[10])<<8 | int32(buf[11])
```

Step-by-step with the example:

```
int32(0x6f) << 24  =  0x6f000000  =  1862270976
int32(0x7f) << 16  =  0x007f0000  =     8257536
int32(0xc6) <<  8  =  0x0000c600  =       50688
int32(0x61) <<  0  =  0x00000061  =          97
                      ----------
                OR  =  0x6f7fc661  =  1870644833  ✓
```

**`<<` is the left-shift operator.** `x << n` moves all the bits of `x` left by `n` positions, which is the same as multiplying by `2ⁿ`:

```
0x6f        = 0110 1111
0x6f << 24  = 0110 1111  0000 0000  0000 0000  0000 0000
                ↑ byte is now in the highest 8 bits of a 32-bit integer
```

### How do you decide 24, 16, 8, 0?

Simple rule: each byte is 8 bits wide, and you have 4 bytes to fit into 32 bits.

Work backwards from the right:

The pattern: (number of bytes after it) × 8

byte 0 has 3 bytes after it → 3 × 8 = 24
byte 1 has 2 bytes after it → 2 × 8 = 16
byte 2 has 1 byte after it → 1 × 8 = 8
byte 3 has 0 bytes after it → 0 × 8 = 0

### Encoding: 1 INT32 → 4 bytes (big-endian write)

The reverse: extract each byte by shifting the value **right** and masking off the low 8 bits with `& 0xff`:

```go
byte(correlationID >> 24)        // 0x6f7fc661 >> 24 = 0x0000006f → byte = 0x6f
byte(correlationID >> 16)        // 0x6f7fc661 >> 16 = 0x00006f7f → byte = 0x7f
byte(correlationID >> 8)         // 0x6f7fc661 >>  8 = 0x006f7fc6 → byte = 0xc6
byte(correlationID & 0xff)       // 0x6f7fc661 & 0xff = 0x00000061 → byte = 0x61
```

`byte(x)` in Go keeps only the lowest 8 bits — equivalent to `x & 0xff` — so the upper bits are discarded automatically.

Visual summary:

```
INT32:  [  byte 0  ] [  byte 1  ] [  byte 2  ] [  byte 3  ]
         bits 31-24   bits 23-16   bits 15-8    bits 7-0
         >> 24        >> 16        >> 8          & 0xff
         0x6f         0x7f         0xc6          0x61
```

### Why `int32(buf[n])` before shifting?

`buf[n]` is a Go `byte` (= `uint8`, 8 bits). If you shift it without converting first:

```go
buf[8] << 24  // compile error or overflow — byte only has 8 bits, can't shift 24
```

Casting to `int32` first widens it to 32 bits, giving the shift room to work:

```go
int32(buf[8]) << 24  // ✓ 32-bit integer with plenty of room
```
