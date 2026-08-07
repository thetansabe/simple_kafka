package main

import (
	"encoding/binary"
	"io"
	"log"
	"net"
)

// responseHeaderV0 builds a response header without TAG_BUFFER (used by ApiVersions).
func responseHeaderV0(correlationID int32) []byte {
	return appendInt32(nil, correlationID)
}

// responseHeaderV1 builds a flexible-version response header with TAG_BUFFER.
func responseHeaderV1(correlationID int32) []byte {
	return append(appendInt32(nil, correlationID), 0x00)
}

func appendInt16(res []byte, v int16) []byte {
	return binary.BigEndian.AppendUint16(res, uint16(v))
}

func appendInt32(res []byte, v int32) []byte {
	return binary.BigEndian.AppendUint32(res, uint32(v))
}

func appendInt64(res []byte, v int64) []byte {
	return binary.BigEndian.AppendUint64(res, uint64(v))
}

func (resp *Response) Init(req *Request) {
	resp.CorrelationID = req.CorrelationID

	switch req.APIKey {
	case APIKeyApiVersions:
		resp.initApiVersions(req)
	case DESCRIBE_TOPIC_PARTITIONS:
		resp.initDescribeTopicPartitions(req)
	case FETCH:
		resp.initFetch(req)
	case PRODUCE:
		resp.initProduce(req)
	default:
		resp.ErrorCode = ErrUnsupportedVersion
	}
}

func RecieveRequest(conn net.Conn) (*Request, error) {
	// read the 4-byte message size prefix first
	var sizeBuf [4]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return nil, err
	}
	msgSize := int(binary.BigEndian.Uint32(sizeBuf[:]))

	// allocate exact size and read the full message body
	buf := make([]byte, msgSize+4)
	copy(buf, sizeBuf[:])
	if _, err := io.ReadFull(conn, buf[4:]); err != nil {
		return nil, err
	}

	msg_size := int32(binary.BigEndian.Uint32(buf[0:4]))
	api_key := int16(binary.BigEndian.Uint16(buf[4:6]))
	api_version := int16(binary.BigEndian.Uint16(buf[6:8]))
	correlation_id := int32(binary.BigEndian.Uint32(buf[8:12]))

	// parse past client_id (2-byte length prefix) and header tag_buffer
	// buf[12..13] = client_id length, buf[14..14+len] = client_id, then 1 byte tag_buffer
	clientIDLen := int(int16(binary.BigEndian.Uint16(buf[12:14])))
	cursor := 14 + clientIDLen + 1 // skip client_id bytes + tag_buffer

	req := &Request{
		MsgSize: msg_size,
		RequestHeader: RequestHeader{
			APIKey:        api_key,
			APIVersion:    api_version,
			CorrelationID: correlation_id,
		},
	}

	// parse body for DescribeTopicPartitions
	if api_key == DESCRIBE_TOPIC_PARTITIONS {
		// compact array: length is N+1 encoded
		numTopics := int(buf[cursor]) - 1
		cursor++

		topics := make([]Topic, 0, numTopics)
		for range numTopics {
			// compact string: length is N+1 encoded
			nameLen := int(buf[cursor]) - 1
			cursor++
			name := string(buf[cursor : cursor+nameLen])
			cursor += nameLen
			cursor++ // tag_buffer for each topic entry
			topics = append(topics, Topic{TopicName: name})
		}

		req.RequestBody = RequestBody{
			Topics:           topics,
			RespPartitionLim: int32(binary.BigEndian.Uint32(buf[cursor : cursor+4])),
			Cursor:           buf[cursor+4],
		}
	}

	if api_key == FETCH {
		// Fetch Request (Version: 16) - https://kafka.apache.org/42/design/protocol/#The_Messages_Fetch
		// skip: max_wait_ms(4) + min_bytes(4) + max_bytes(4) + isolation_level(1) + session_id(4) + session_epoch(4) = 21 bytes
		cursor += 21

		numTopics := int(buf[cursor]) - 1 // compact array
		cursor++

		fetchTopics := make([]FetchTopic, 0, numTopics)
		for range numTopics {
			var topicId [16]byte
			copy(topicId[:], buf[cursor:cursor+16])
			cursor += 16

			numPartitions := int(buf[cursor]) - 1 // compact array
			cursor++

			partitions := make([]FetchPartition, 0, numPartitions)
			for range numPartitions {
				partitionIndex := int32(binary.BigEndian.Uint32(buf[cursor : cursor+4]))
				cursor += 4
				cursor += 4 // current_leader_epoch (int32)
				fetchOffset := int64(binary.BigEndian.Uint64(buf[cursor : cursor+8]))
				cursor += 8
				cursor += 4 // last_fetched_epoch (int32)
				cursor += 8 // log_start_offset (int64)
				cursor += 4 // partition_max_bytes (int32)
				cursor++    // TAG_BUFFER per partition
				partitions = append(partitions, FetchPartition{Partition: partitionIndex, FetchOffset: fetchOffset})
			}
			cursor++ // TAG_BUFFER per topic

			fetchTopics = append(fetchTopics, FetchTopic{TopicId: topicId, Partitions: partitions})
		}
		req.FetchTopics = fetchTopics
	}

	if api_key == PRODUCE {
		// Produce Request (Version: 11)
		// transactional_id: compact nullable string (null = varint 0)
		transactionalIdLen := int(buf[cursor]) - 1
		cursor++
		if transactionalIdLen > 0 {
			cursor += transactionalIdLen
		}
		cursor += 2 // acks (int16)
		cursor += 4 // timeout_ms (int32)

		numTopics := int(buf[cursor]) - 1 // compact array
		cursor++

		produceTopics := make([]ProduceTopic, 0, numTopics)
		for range numTopics {
			nameLen := int(buf[cursor]) - 1
			cursor++
			name := string(buf[cursor : cursor+nameLen])
			cursor += nameLen

			numPartitions := int(buf[cursor]) - 1 // compact array
			cursor++

			partitions := make([]ProducePartition, 0, numPartitions)
			for range numPartitions {
				partIdx := int32(binary.BigEndian.Uint32(buf[cursor : cursor+4]))
				cursor += 4
				// records: compact bytes — LEB128(actualLen+1) then actualLen raw bytes
				recordsCompactLen, newCursor := readUvarint(buf, cursor)
				cursor = newCursor
				var recordBytes []byte
				actualLen := recordsCompactLen - 1
				if actualLen > 0 {
					recordBytes = make([]byte, actualLen)
					copy(recordBytes, buf[cursor:cursor+actualLen])
					cursor += actualLen
				}
				cursor++ // TAG_BUFFER per partition
				partitions = append(partitions, ProducePartition{Index: partIdx, Records: recordBytes})
			}
			cursor++ // TAG_BUFFER per topic

			produceTopics = append(produceTopics, ProduceTopic{TopicName: name, Partitions: partitions})
		}
		req.ProduceTopics = produceTopics
	}

	return req, nil
}

func (resp *Response) SendResp(conn net.Conn, req *Request) error {
	log.Printf("Sending response: CorrelationID=%d, ErrorCode=%d", resp.CorrelationID, resp.ErrorCode)

	var res []byte
	switch req.APIKey {
	case APIKeyApiVersions:
		res = resp.ConstructResponseForApiVersions()
	case DESCRIBE_TOPIC_PARTITIONS:
		res = resp.ConstructResponseForDescribeTopic()
	case FETCH:
		res = resp.ConstructResponseForFetch()
	case PRODUCE:
		res = resp.ConstructResponseForProduce()
	}

	msgSize := int32(len(res))

	// frame is the first 4 bytes of the response, which is the size of the response
	// so it also means this is msgSize
	frame := binary.BigEndian.AppendUint32(nil, uint32(msgSize))

	// write the response to the connection
	_, err := conn.Write(append(frame, res...))
	return err
}

// readUvarint decodes a LEB128 unsigned varint from buf starting at cursor.
// Returns (value, newCursor).
func readUvarint(buf []byte, cursor int) (int, int) {
	result := 0
	shift := 0
	for {
		b := int(buf[cursor])
		cursor++
		result |= (b & 0x7F) << shift
		if b&0x80 == 0 {
			break
		}
		shift += 7
	}
	return result, cursor
}

func appendCompactInt32Array(res []byte, arr []int32) []byte {
	res = append(res, byte(len(arr)+1)) // compact array length = real length + 1
	for _, v := range arr {
		res = appendInt32(res, v)
	}
	return res
}

// appendCompactBytes writes data as a compact NULLABLE_BYTES field:
// length = len(data)+1 encoded as unsigned varint, then the raw bytes.
// nil data is encoded as length=0 (null).
func appendCompactBytes(res []byte, data []byte) []byte {
	if data == nil {
		return append(res, 0x00) // null
	}
	n := len(data) + 1 // compact encoding: stored length = real length + 1
	// encode n as unsigned varint (LEB128)
	for n >= 0x80 {
		res = append(res, byte(n)|0x80)
		n >>= 7
	}
	res = append(res, byte(n))
	return append(res, data...)
}
