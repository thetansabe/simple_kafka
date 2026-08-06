package main

import (
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

	default:
		resp.ErrorCode = ErrUnsupportedVersion
	}
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
	m, _ := readLogFileToMap() // read the cluster metadata from the log file to get the topic_id for each topic

	for _, topic := range resp.FetchTopics {
		// topic_id (16 bytes UUID)
		res = append(res, topic.TopicId[:]...)

		// partitions compact array (N+1)
		res = append(res, byte(len(topic.Partitions)+1))
		for _, p := range topic.Partitions {
			res = append(res, byte(p.Partition>>24), byte(p.Partition>>16), byte(p.Partition>>8), byte(p.Partition)) // partition_index

			// if exists topic_id in the cluster metadata, then return error_code = 0, else return error_code = 100
			_, exists := m[topic.TopicId]
			if exists {
				res = append(res, byte(ErrNone>>8), byte(ErrNone)) // error_code = 0
			} else {
				res = append(res, byte(UNKNOWN_TOPIC_ID>>8), byte(UNKNOWN_TOPIC_ID)) // error_code = 100
			}

			res = append(res, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) // high_watermark (8 bytes)
			res = append(res, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) // last_stable_offset (8 bytes)
			res = append(res, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00) // log_start_offset (8 bytes)
			res = append(res, 0x01)                                           // aborted_transactions: empty compact array
			res = append(res, 0xff, 0xff, 0xff, 0xff)                         // preferred_read_replica: -1
			res = append(res, 0x01)                                           // records: empty compact bytes
			res = append(res, 0x00)                                           // TAG_BUFFER
		}
		res = append(res, 0x00) // TAG_BUFFER per topic
	}

	res = append(res, 0x00) // body TAG_BUFFER
	return res
}

func RecieveRequest(conn net.Conn) (*Request, error) {
	var buf = make([]byte, 1024)
	_, err := conn.Read(buf)
	if err != nil {
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
