package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

func (resp *Response) Init(req *Request) {
	resp.CorrelationID = req.CorrelationID

	switch req.APIKey {
	case APIKeyApiVersions:
		if req.APIVersion < 0 || req.APIVersion > 4 {
			resp.ErrorCode = ErrUnsupportedVersion
		} else {
			resp.ErrorCode = ErrNone
		}
		resp.APIKeys = []APIKey{
			// ApiVersions
			{APIKey: APIKeyApiVersions, MinVersion: 0, MaxVersion: 4, TagBuffer: 0},
			// DescribeTopicPartitions
			{APIKey: DESCRIBE_TOPIC_PARTITIONS, MinVersion: 0, MaxVersion: 0, TagBuffer: 0},
			// Fetch
			{APIKey: FETCH, MinVersion: 0, MaxVersion: 16, TagBuffer: 0},
			// Produce
			{APIKey: PRODUCE, MinVersion: 0, MaxVersion: 8, TagBuffer: 0},
		}

	case DESCRIBE_TOPIC_PARTITIONS:
		for _, topic := range req.Topics {
			resp.Topics = append(resp.Topics, TopicForResp{
				ErrCode:                   UNKNOWN_TOPIC_OR_PARTITION,
				TopicName:                 topic.TopicName,
				TopicID:                   [16]byte{0},
				IsInternal:                false,
				Partitions:                make([]Partition, 0),
				TopicAuthorizedOperations: 0,
			})
		}
		resp.NextCursor = 0xff     // null cursor
		resp.LoadClusterMetadata() // load cluster metadata from the log file

	case FETCH:
		resp.FetchTopics = req.FetchTopics
	case PRODUCE:
		m, _ := readLogFileToMap()
		for _, pt := range req.ProduceTopics {
			t := TopicForResp{TopicName: pt.TopicName}
			found := false
			for _, meta := range m {
				if meta.Name == pt.TopicName {
					found = true
					break
				}
			}
			errCode := UNKNOWN_TOPIC_OR_PARTITION
			if found {
				errCode = ErrNone
			}
			for _, pp := range pt.Partitions {
				if found {
					writeProduceLogFile(pt.TopicName, pp.Index, pp.Records)
				}
				t.Partitions = append(t.Partitions, Partition{
					PartitionIndex: pp.Index,
					ErrCode:        errCode,
				})
			}
			resp.Topics = append(resp.Topics, t)
		}
	default:
		resp.ErrorCode = ErrUnsupportedVersion
	}
}

// https://binspec.org/kafka-produce-unknown-topic-or-partition-response-v11?highlight=4-7
func (resp *Response) ConstructResponseForProduce() []byte {
	/* ====== response header v1 ====== */
	res := []byte{
		byte(resp.CorrelationID >> 24), byte(resp.CorrelationID >> 16),
		byte(resp.CorrelationID >> 8), byte(resp.CorrelationID),
		0x00, // TAG_BUFFER
	}

	/* ============= body ============= */
	// responses: compact array (N+1)
	res = append(res, byte(len(resp.Topics)+1))
	for _, topic := range resp.Topics {
		// name: compact string
		res = append(res, byte(len(topic.TopicName)+1))
		res = append(res, []byte(topic.TopicName)...)

		// partition_responses: compact array (N+1)
		res = append(res, byte(len(topic.Partitions)+1))
		for _, p := range topic.Partitions {
			res = append(res, byte(p.PartitionIndex>>24), byte(p.PartitionIndex>>16), byte(p.PartitionIndex>>8), byte(p.PartitionIndex)) // index (int32)
			res = append(res, byte(p.ErrCode>>8), byte(p.ErrCode))                                                                       // error_code (int16)

			if p.ErrCode != ErrNone {
				// error case: base_offset = -1, log_start_offset = -1
				res = append(res, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff) // base_offset = -1
				res = append(res, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff) // log_append_time_ms = -1
				res = append(res, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff) // log_start_offset = -1
			} else {
				// success case: base_offset = 0 (first record offset), log_start_offset = 0
				res = append(res, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) // base_offset = 0
				res = append(res, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff) // log_append_time_ms = -1
				res = append(res, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) // log_start_offset = 0
			}

			res = append(res, 0x01) // record_errors: empty compact array
			res = append(res, 0x00) // error_message: null compact string
			res = append(res, 0x00) // TAG_BUFFER
		}
		res = append(res, 0x00) // TAG_BUFFER per topic
	}

	res = append(res, byte(resp.ThrottleTimeMS>>24), byte(resp.ThrottleTimeMS>>16), byte(resp.ThrottleTimeMS>>8), byte(resp.ThrottleTimeMS)) // throttle_time_ms
	res = append(res, 0x00)                                                                                                                  // TAG_BUFFER
	return res
}

// https://kafka.apache.org/43/design/protocol/
// search for Fetch Response (Version: 16) - that's the body
// search for Response Header v1 - that's the header
func (resp *Response) ConstructResponseForFetch() []byte {
	/* ====== response header v1 ====== */
	res := []byte{
		byte(resp.CorrelationID >> 24), byte(resp.CorrelationID >> 16),
		byte(resp.CorrelationID >> 8), byte(resp.CorrelationID),
		0x00, // TAG_BUFFER
	}

	/* ============= body ============= */
	res = append(res, 0x00, 0x00, 0x00, 0x00) // throttle_time_ms (4 bytes)
	res = append(res, 0x00, 0x00)             // error_code (2 bytes)
	res = append(res, 0x00, 0x00, 0x00, 0x00) // session_id (4 bytes)

	// responses compact array (N+1)
	res = append(res, byte(len(resp.FetchTopics)+1))
	m, _ := readLogFileToMap() // read the cluster metadata to look up topic names by UUID

	for _, topic := range resp.FetchTopics {
		// topic_id (16 bytes UUID)
		res = append(res, topic.TopicId[:]...)

		metadata, exists := m[topic.TopicId]

		// partitions compact array (N+1)
		res = append(res, byte(len(topic.Partitions)+1))
		for _, p := range topic.Partitions {
			res = append(res, byte(p.Partition>>24), byte(p.Partition>>16), byte(p.Partition>>8), byte(p.Partition)) // partition_index

			if !exists {
				res = append(res, byte(UNKNOWN_TOPIC_ID>>8), byte(UNKNOWN_TOPIC_ID)) // error_code = 100
				res = append(res, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)    // high_watermark
				res = append(res, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)    // last_stable_offset
				res = append(res, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00)    // log_start_offset
				res = append(res, 0x01)                                              // aborted_transactions: empty
				res = append(res, 0xff, 0xff, 0xff, 0xff)                            // preferred_read_replica: -1
				res = append(res, 0x01)                                              // records: empty
				res = append(res, 0x00)                                              // TAG_BUFFER
				continue
			}

			// topic exists — read the partition log file from disk
			logPath := fmt.Sprintf("/tmp/kraft-combined-logs/%s-%d/00000000000000000000.log", metadata.Name, p.Partition)
			records, err := os.ReadFile(logPath)
			if err != nil {
				records = nil
			}

			// to know how many messages are in the partition, it inside record count field
			// https://kafka.apache.org/43/implementation/message-format/#record-batch
			// offset 57-60: recordsCount (4 bytes) ← THIS
			// offset 61: records: [Record]
			// why we store highWatermark = baseOffset + recordsCount
			// because the file name 00000000000000000000.log is also baseOffset = 0
			// so highWatermark = baseOffset + recordsCount = 0 + recordsCount = recordsCount

			// EXPLAINATION why we store highWatermark = baseOffset + recordsCount
			// a partition can have many batch of messages comes at different time
			// in replication scenario, maybe the follower node don't have all messages
			// to know which cursor the follower already have, we need to store as highWatermark
			highWatermark := int64(0)

			// to read all RecordBatch we need to read the first 12 bytes of each RecordBatch to know the batch length
			for pos := 0; pos+12 <= len(records); {
				baseOff := int64(records[pos])<<56 | int64(records[pos+1])<<48 | int64(records[pos+2])<<40 | int64(records[pos+3])<<32 |
					int64(records[pos+4])<<24 | int64(records[pos+5])<<16 | int64(records[pos+6])<<8 | int64(records[pos+7])
				batchLen := int(records[pos+8])<<24 | int(records[pos+9])<<16 | int(records[pos+10])<<8 | int(records[pos+11])
				batchEnd := pos + 12 + batchLen
				if batchEnd > len(records) {
					break
				}
				if pos+61 <= len(records) {
					recordsCount := int64(records[pos+57])<<24 | int64(records[pos+58])<<16 | int64(records[pos+59])<<8 | int64(records[pos+60])
					highWatermark = baseOff + recordsCount
				}
				pos = batchEnd
			}

			res = append(res, byte(ErrNone>>8), byte(ErrNone)) // error_code = 0
			res = append(res, byte(highWatermark>>56), byte(highWatermark>>48), byte(highWatermark>>40), byte(highWatermark>>32),
				byte(highWatermark>>24), byte(highWatermark>>16), byte(highWatermark>>8), byte(highWatermark)) // high_watermark
			res = append(res, byte(highWatermark>>56), byte(highWatermark>>48), byte(highWatermark>>40), byte(highWatermark>>32),
				byte(highWatermark>>24), byte(highWatermark>>16), byte(highWatermark>>8), byte(highWatermark)) // last_stable_offset = high_watermark
			res = append(res, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) // log_start_offset
			res = append(res, 0x01)                                           // aborted_transactions: empty
			res = append(res, 0xff, 0xff, 0xff, 0xff)                         // preferred_read_replica: -1
			// records: compact bytes — length = len(records)+1, then raw bytes
			res = appendCompactBytes(res, records)
			res = append(res, 0x00) // TAG_BUFFER
		}
		res = append(res, 0x00) // TAG_BUFFER per topic
	}

	res = append(res, 0x00) // body TAG_BUFFER
	return res
}

func RecieveRequest(conn net.Conn) (*Request, error) {
	// read the 4-byte message size prefix first
	var sizeBuf [4]byte
	if _, err := io.ReadFull(conn, sizeBuf[:]); err != nil {
		return nil, err
	}
	msgSize := int(sizeBuf[0])<<24 | int(sizeBuf[1])<<16 | int(sizeBuf[2])<<8 | int(sizeBuf[3])

	// allocate exact size and read the full message body
	buf := make([]byte, msgSize+4)
	copy(buf, sizeBuf[:])
	if _, err := io.ReadFull(conn, buf[4:]); err != nil {
		return nil, err
	}

	msg_size := int32(buf[0])<<24 | int32(buf[1])<<16 | int32(buf[2])<<8 | int32(buf[3])
	api_key := int16(buf[4])<<8 | int16(buf[5])
	api_version := int16(buf[6])<<8 | int16(buf[7])
	correlation_id := int32(buf[8])<<24 | int32(buf[9])<<16 | int32(buf[10])<<8 | int32(buf[11])

	// parse past client_id (2-byte length prefix) and header tag_buffer
	// buf[12..13] = client_id length, buf[14..14+len] = client_id, then 1 byte tag_buffer
	clientIDLen := int(int16(buf[12])<<8 | int16(buf[13]))
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
			RespPartitionLim: int32(buf[cursor])<<24 | int32(buf[cursor+1])<<16 | int32(buf[cursor+2])<<8 | int32(buf[cursor+3]),
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
				partitionIndex := int32(buf[cursor])<<24 | int32(buf[cursor+1])<<16 | int32(buf[cursor+2])<<8 | int32(buf[cursor+3])
				cursor += 4
				// skip: current_leader_epoch(4) + fetch_offset(8) + last_fetched_epoch(4) + log_start_offset(8) + partition_max_bytes(4) = 28 bytes
				cursor += 28
				cursor++ // TAG_BUFFER per partition
				partitions = append(partitions, FetchPartition{Partition: partitionIndex})
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
				partIdx := int32(buf[cursor])<<24 | int32(buf[cursor+1])<<16 | int32(buf[cursor+2])<<8 | int32(buf[cursor+3])
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
	frame := []byte{
		byte(msgSize >> 24), byte(msgSize >> 16), byte(msgSize >> 8), byte(msgSize),
	}

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

func (resp *Response) ConstructResponseForApiVersions() (res []byte) {
	/* ==================header================== */
	// correlation_id, 4 bytes
	res = []byte{
		// correlation_id (4 bytes)
		byte(resp.CorrelationID >> 24), byte(resp.CorrelationID >> 16),
		byte(resp.CorrelationID >> 8), byte(resp.CorrelationID),
	}

	/* ===================body=================== */
	// err_code, 2 bytes
	res = append(res,
		byte(resp.ErrorCode>>8), byte(resp.ErrorCode),
	)

	// api_keys array lenght, encoded as the real length plus 1
	res = append(res, byte(len(resp.APIKeys)+1))

	// APIKeys
	for _, apiKey := range resp.APIKeys {
		res = append(res,
			byte(apiKey.APIKey>>8), byte(apiKey.APIKey),
			byte(apiKey.MinVersion>>8), byte(apiKey.MinVersion),
			byte(apiKey.MaxVersion>>8), byte(apiKey.MaxVersion),
			apiKey.TagBuffer,
		)
	}

	// throttle_time_ms, 4 bytes
	res = append(res,
		byte(resp.ThrottleTimeMS>>24), byte(resp.ThrottleTimeMS>>16),
		byte(resp.ThrottleTimeMS>>8), byte(resp.ThrottleTimeMS),
	)

	// tag_buffer, 1 byte
	res = append(res, resp.TagBuffer)
	return res
}

func appendCompactInt32Array(res []byte, arr []int32) []byte {
	res = append(res, byte(len(arr)+1)) // compact array length = real length + 1
	for _, v := range arr {
		res = append(res, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
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

func (resp *Response) ConstructResponseForDescribeTopic() (res []byte) {
	/* ====== response header (flexible v1) ====== */
	res = []byte{
		// correlation_id (4 bytes)
		byte(resp.CorrelationID >> 24), byte(resp.CorrelationID >> 16),
		byte(resp.CorrelationID >> 8), byte(resp.CorrelationID),
		// header tag_buffer (1 byte)
		0x00,
	}

	/* ================== body =================== */
	// throttle_time_ms (4 bytes)
	res = append(res,
		byte(resp.ThrottleTimeMS>>24), byte(resp.ThrottleTimeMS>>16),
		byte(resp.ThrottleTimeMS>>8), byte(resp.ThrottleTimeMS),
	)

	// topics compact array length (N+1)
	res = append(res, byte(len(resp.Topics)+1))

	for _, topic := range resp.Topics {
		// error_code (2 bytes)
		res = append(res, byte(topic.ErrCode>>8), byte(topic.ErrCode))

		// topic_name compact string (len+1, then bytes)
		res = append(res, byte(len(topic.TopicName)+1))
		res = append(res, []byte(topic.TopicName)...)

		// topic_id (16 bytes, all zeros = unknown)
		res = append(res, topic.TopicID[:]...)

		// is_internal (1 byte bool)
		if topic.IsInternal {
			res = append(res, 1)
		} else {
			res = append(res, 0)
		}

		// partitions compact array length (N+1)
		res = append(res, byte(len(topic.Partitions)+1))
		for _, p := range topic.Partitions {
			res = append(res, byte(p.ErrCode>>8), byte(p.ErrCode))
			res = append(res, byte(p.PartitionIndex>>24), byte(p.PartitionIndex>>16), byte(p.PartitionIndex>>8), byte(p.PartitionIndex))
			res = append(res, byte(p.LeaderID>>24), byte(p.LeaderID>>16), byte(p.LeaderID>>8), byte(p.LeaderID))
			res = append(res, byte(p.LeaderEpoch>>24), byte(p.LeaderEpoch>>16), byte(p.LeaderEpoch>>8), byte(p.LeaderEpoch))
			res = appendCompactInt32Array(res, p.ReplicaNodes)
			res = appendCompactInt32Array(res, p.IsrNodes)
			res = appendCompactInt32Array(res, p.EligibleLeaderReplicas)
			res = appendCompactInt32Array(res, p.LastKnownElr)
			res = appendCompactInt32Array(res, p.OfflineReplicas)
			res = append(res, p.TagBuffer)
		}

		// topic_authorized_operations (4 bytes)
		res = append(res,
			byte(topic.TopicAuthorizedOperations>>24), byte(topic.TopicAuthorizedOperations>>16),
			byte(topic.TopicAuthorizedOperations>>8), byte(topic.TopicAuthorizedOperations),
		)

		// topic tag_buffer
		res = append(res, topic.TagBuffer)
	}

	// next_cursor (0xff = null)
	res = append(res, resp.NextCursor)

	// body tag_buffer
	res = append(res, resp.TagBuffer)
	return res
}

// read this: https://binspec.org/kafka-cluster-metadata?highlight=0-90
// to know the offset of each field in the input content []byte
func LogMetadataMapByUuid(content []byte) map[[16]byte]LogMetadata {
	m := make(map[[16]byte]LogMetadata)

	// skip batch 1 (only contains FeatureLevel/Broker records, no topic/partition data)
	batch1Len := int(content[8])<<24 | int(content[9])<<16 | int(content[10])<<8 | int(content[11])
	batchStart := 12 + batch1Len

	// now the log file contains many batches, each containing a topic record and one or more partition records
	// we need to read all batches
	for batchStart+12 <= len(content) {
		batchLen := int(content[batchStart+8])<<24 | int(content[batchStart+9])<<16 | int(content[batchStart+10])<<8 | int(content[batchStart+11])
		batchEnd := batchStart + 12 + batchLen
		if batchEnd > len(content) {
			break
		}
		parseBatchRecords(content, batchStart, batchEnd, m)
		batchStart = batchEnd
	}

	return m
}

// parseBatchRecords reads all records in one RecordBatch and populates the map.
// batchStart points to the start of the batch (baseOffset field).
// batchEnd  points to the first byte AFTER this batch.
func parseBatchRecords(content []byte, batchStart, batchEnd int, m map[[16]byte]LogMetadata) {
	// skip the 61-byte RecordBatch header (8+4+4+1+4+2+4+8+8+8+2+4+4 = 61 bytes)
	cursor := batchStart + 61

	for cursor < batchEnd {
		// Record length is a varint (LEB128): bit 7 = 1 means "one more byte follows"
		if (content[cursor]>>7)&1 == 1 {
			cursor++ // skip first byte of 2-byte varint
		}
		cursor++ // advance past last (or only) byte of record length

		// skip: attributes(1) + timestampDelta(1) + offsetDelta(1) + keyLength(1) + key(0, null)
		cursor += 4

		// value_length is a signed zigzag varint (stored as 2*n, decode with >>1)
		var valueLen int
		switch (content[cursor] >> 7) & 1 {
		case 0:
			valueLen = int(content[cursor]) >> 1
		case 1:
			valueLen = ((int(content[cursor]) & 0x7F) | ((int(content[cursor+1]) & 0x7F) << 7)) >> 1
			cursor++ // cursor now on last byte of 2-byte varint
		}

		// cursor+1 = frame_version, cursor+2 = type, cursor+3 = version
		valueStart := cursor + 1

		switch content[cursor+2] {
		case TopicRecord:
			cursor += 4                         // skip: value_length last byte + frame_version + type + version
			nameLen := int(content[cursor]) - 1 // compact string: stored len = real len + 1
			cursor++
			topicName := string(content[cursor : cursor+nameLen])
			cursor += nameLen
			var uuid [16]byte
			copy(uuid[:], content[cursor:cursor+16])
			m[uuid] = LogMetadata{Name: topicName, Partitions: m[uuid].Partitions}

		case PartitionRecord:
			cursor += 4 // skip: value_length last byte + frame_version + type + version
			partition := Partition{
				PartitionIndex: int32(content[cursor])<<24 | int32(content[cursor+1])<<16 | int32(content[cursor+2])<<8 | int32(content[cursor+3]),
			}
			cursor += 4

			var topicUUID [16]byte
			copy(topicUUID[:], content[cursor:cursor+16])
			cursor += 16

			replicaLen := int(content[cursor]) - 1
			cursor++
			for range replicaLen {
				partition.ReplicaNodes = append(partition.ReplicaNodes, int32(content[cursor])<<24|int32(content[cursor+1])<<16|int32(content[cursor+2])<<8|int32(content[cursor+3]))
				cursor += 4
			}

			isrLen := int(content[cursor]) - 1
			cursor++
			for range isrLen {
				partition.IsrNodes = append(partition.IsrNodes, int32(content[cursor])<<24|int32(content[cursor+1])<<16|int32(content[cursor+2])<<8|int32(content[cursor+3]))
				cursor += 4
			}

			removingLen := int(content[cursor]) - 1
			cursor += 1 + removingLen*4

			addingLen := int(content[cursor]) - 1
			cursor += 1 + addingLen*4

			partition.LeaderID = int32(content[cursor])<<24 | int32(content[cursor+1])<<16 | int32(content[cursor+2])<<8 | int32(content[cursor+3])
			cursor += 4
			partition.LeaderEpoch = int32(content[cursor])<<24 | int32(content[cursor+1])<<16 | int32(content[cursor+2])<<8 | int32(content[cursor+3])
			cursor += 4

			m[topicUUID] = LogMetadata{
				Name:       m[topicUUID].Name,
				Partitions: append(m[topicUUID].Partitions, partition),
			}
			// default: unknown type (BrokerRecord, FeatureLevelRecord, etc.) — skip via valueLen below
		}

		// always jump to the start of the next record, regardless of how much we parsed above
		// +1 skips the headersCount varint (always 0x00)
		cursor = valueStart + valueLen + 1
	}
}

func (resp *Response) LoadClusterMetadata() error {
	m, _ := readLogFileToMap()

	for i := range resp.Topics {
		for uuid, metadata := range m {
			if metadata.Name == resp.Topics[i].TopicName {
				resp.Topics[i].TopicID = uuid
				resp.Topics[i].ErrCode = ErrNone // set to 0
				break
			}
		}

		// append the partitions read from the log file into the response's topic partitions
		// resp.Topics[i].Partitions = m[resp.Topics[i].TopicID].Partitions
		for _, p := range m[resp.Topics[i].TopicID].Partitions {
			resp.Topics[i].Partitions = append(resp.Topics[i].Partitions, p)
		}
	}

	return nil
}

func readLogFileToMap() (map[[16]byte]LogMetadata, error) {
	content, err := os.ReadFile("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log")
	if err != nil {
		return nil, err
	}

	log.Println("Will ParseClusterMetadataLog fail?")

	m := LogMetadataMapByUuid(content)

	log.Println("ParseClusterMetadataLog didn't fail")

	return m, nil
}

// writeProduceLogFile appends a RecordBatch to the partition log file.
// The raw RecordBatch bytes come directly from the produce request wire format.
func writeProduceLogFile(topicName string, partitionIdx int32, records []byte) error {
	dir := fmt.Sprintf("/tmp/kraft-combined-logs/%s-%d", topicName, partitionIdx)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	filePath := fmt.Sprintf("%s/00000000000000000000.log", dir)
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(records)
	return err
}
