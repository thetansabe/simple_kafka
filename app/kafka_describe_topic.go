package main

import (
	"log"
	"os"
)

func (resp *Response) initDescribeTopicPartitions(req *Request) {
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
}

func (resp *Response) ConstructResponseForDescribeTopic() []byte {
	/* ====== response header (flexible v1) ====== */
	res := responseHeaderV1(resp.CorrelationID)

	/* ================== body =================== */
	// throttle_time_ms (4 bytes)
	res = appendInt32(res, resp.ThrottleTimeMS)

	// topics compact array length (N+1)
	res = append(res, byte(len(resp.Topics)+1))

	for _, topic := range resp.Topics {
		// error_code (2 bytes)
		res = appendInt16(res, topic.ErrCode)

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
			res = appendInt16(res, p.ErrCode)
			res = appendInt32(res, p.PartitionIndex)
			res = appendInt32(res, p.LeaderID)
			res = appendInt32(res, p.LeaderEpoch)
			res = appendCompactInt32Array(res, p.ReplicaNodes)
			res = appendCompactInt32Array(res, p.IsrNodes)
			res = appendCompactInt32Array(res, p.EligibleLeaderReplicas)
			res = appendCompactInt32Array(res, p.LastKnownElr)
			res = appendCompactInt32Array(res, p.OfflineReplicas)
			res = append(res, p.TagBuffer)
		}

		// topic_authorized_operations (4 bytes)
		res = appendInt32(res, topic.TopicAuthorizedOperations)

		// topic tag_buffer
		res = append(res, topic.TagBuffer)
	}

	// next_cursor (0xff = null)
	res = append(res, resp.NextCursor)

	// body tag_buffer
	res = append(res, resp.TagBuffer)
	return res
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

// read this: https://binspec.org/kafka-cluster-metadata?highlight=0-90
// to know the offset of each field in the input content []byte
func LogMetadataMapByUuid(content []byte) map[[16]byte]LogMetadata {
	m := make(map[[16]byte]LogMetadata)

	r := newReader(content)
	// skip batch 1 (only contains FeatureLevel/Broker records, no topic/partition data)
	r.Skip(8)                       // baseOffset of batch 1
	batch1Len := int(r.ReadInt32()) // batchLength field counts bytes after this point
	r.Skip(batch1Len)               // skip rest of batch 1

	// now the log file contains many batches, each containing a topic record and one or more partition records
	// we need to read all batches
	for r.Len() >= 12 {
		r.Skip(8)                      // baseOffset
		batchLen := int(r.ReadInt32()) // batchLength
		if r.Len() < batchLen {
			break
		}
		// batchLen bytes: 49-byte remaining RecordBatch header + all records
		batchBody := newReader(r.ReadBytes(batchLen))
		parseBatchRecords(batchBody, m)
	}

	return m
}

// parseBatchRecords reads all records in one RecordBatch body and populates the map.
// r starts at the first byte after the 12-byte framing (baseOffset + batchLength).
func parseBatchRecords(r *Reader, m map[[16]byte]LogMetadata) {
	// skip the remaining 49 bytes of RecordBatch header
	// (61 total header - 12 framing already consumed = 49 left)
	r.Skip(49)

	for r.Len() > 0 {
		// Record length: LEB128 unsigned varint (discarded — boundary is driven by value_length below)
		r.ReadUvarint()

		// skip: attributes(1) + timestampDelta(1) + offsetDelta(1) + keyLength(1) + key(0, null)
		r.Skip(4)

		// value_length: zigzag-encoded signed varint (stored as 2*n, decode with >>1)
		valueLen := r.ReadZigzag()

		// consume exactly valueLen bytes as a sub-reader so parsing can't overrun the record boundary
		value := newReader(r.ReadBytes(valueLen))
		r.Skip(1) // headersCount varint (always 0x00)

		// inside value: frame_version(1) + type(1) + version(1) + type-specific data
		value.Skip(1) // frame_version
		recordType := value.ReadByte()
		value.Skip(1) // version

		switch recordType {
		case TopicRecord:
			topicName := value.ReadCompactString()
			uuid := value.ReadUUID()
			m[uuid] = LogMetadata{Name: topicName, Partitions: m[uuid].Partitions}

		case PartitionRecord:
			partIdx := value.ReadInt32()
			topicUUID := value.ReadUUID()

			replicaLen := value.ReadUvarint() - 1
			replicas := make([]int32, replicaLen)
			for i := range replicaLen {
				replicas[i] = value.ReadInt32()
			}

			isrLen := value.ReadUvarint() - 1
			isrs := make([]int32, isrLen)
			for i := range isrLen {
				isrs[i] = value.ReadInt32()
			}

			removingLen := value.ReadUvarint() - 1
			value.Skip(removingLen * 4)

			addingLen := value.ReadUvarint() - 1
			value.Skip(addingLen * 4)

			leaderID := value.ReadInt32()
			leaderEpoch := value.ReadInt32()

			m[topicUUID] = LogMetadata{
				Name: m[topicUUID].Name,
				Partitions: append(m[topicUUID].Partitions, Partition{
					PartitionIndex: partIdx,
					ReplicaNodes:   replicas,
					IsrNodes:       isrs,
					LeaderID:       leaderID,
					LeaderEpoch:    leaderEpoch,
				}),
			}
			// default: unknown type (BrokerRecord, FeatureLevelRecord, etc.) — value already consumed above
		}
	}
}

