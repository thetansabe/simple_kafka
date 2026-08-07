package main

import "encoding/binary"

// Reader is a stateful big-endian reader over a byte slice.
// It auto-advances an internal cursor with every read, eliminating
// the need to manually track byte offsets.
type Reader struct {
	data []byte
	pos  int
}

func newReader(data []byte) *Reader {
	return &Reader{data: data}
}

// Len returns the number of unread bytes remaining.
func (r *Reader) Len() int { return len(r.data) - r.pos }

// ReadByte reads one byte.
func (r *Reader) ReadByte() byte {
	b := r.data[r.pos]
	r.pos++
	return b
}

// ReadInt16 reads a big-endian int16.
func (r *Reader) ReadInt16() int16 {
	v := int16(binary.BigEndian.Uint16(r.data[r.pos : r.pos+2]))
	r.pos += 2
	return v
}

// ReadInt32 reads a big-endian int32.
func (r *Reader) ReadInt32() int32 {
	v := int32(binary.BigEndian.Uint32(r.data[r.pos : r.pos+4]))
	r.pos += 4
	return v
}

// ReadInt64 reads a big-endian int64.
func (r *Reader) ReadInt64() int64 {
	v := int64(binary.BigEndian.Uint64(r.data[r.pos : r.pos+8]))
	r.pos += 8
	return v
}

// ReadBytes returns a slice of n bytes from the current position.
// The slice references the underlying buffer — copy if lifetime exceeds the reader.
func (r *Reader) ReadBytes(n int) []byte {
	b := r.data[r.pos : r.pos+n]
	r.pos += n
	return b
}

// Skip advances the cursor by n bytes without reading.
func (r *Reader) Skip(n int) {
	r.pos += n
}

// ReadUvarint reads an unsigned LEB128 varint.
func (r *Reader) ReadUvarint() int {
	result, newPos := readUvarint(r.data, r.pos)
	r.pos = newPos
	return result
}

// ReadZigzag reads a Kafka zigzag-encoded signed varint (stored as 2*n, decode with >>1).
// Used for record value_length fields inside a RecordBatch.
func (r *Reader) ReadZigzag() int {
	if (r.data[r.pos]>>7)&1 == 1 {
		v := ((int(r.data[r.pos]) & 0x7F) | ((int(r.data[r.pos+1]) & 0x7F) << 7)) >> 1
		r.pos += 2
		return v
	}
	v := int(r.data[r.pos]) >> 1
	r.pos++
	return v
}

// ReadUUID reads 16 bytes as a fixed-size UUID array.
func (r *Reader) ReadUUID() [16]byte {
	var uuid [16]byte
	copy(uuid[:], r.data[r.pos:r.pos+16])
	r.pos += 16
	return uuid
}

// ReadCompactString reads a Kafka compact string: uvarint(len+1) then len bytes.
func (r *Reader) ReadCompactString() string {
	n := r.ReadUvarint() - 1
	s := string(r.data[r.pos : r.pos+n])
	r.pos += n
	return s
}
