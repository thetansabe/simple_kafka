package main

import (
	"encoding/binary"
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

	// skip batch 1 (only contains FeatureLevel/Broker records, no topic/partition data)
	batch1Len := int(binary.BigEndian.Uint32(content[8:12]))
	batchStart := 12 + batch1Len

	// now the log file contains many batches, each containing a topic record and one or more partition records
	// we need to read all batches
	for batchStart+12 <= len(content) {
		batchLen := int(binary.BigEndian.Uint32(content[batchStart+8 : batchStart+12]))
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
				PartitionIndex: int32(binary.BigEndian.Uint32(content[cursor : cursor+4])),
			}
			cursor += 4

			var topicUUID [16]byte
			copy(topicUUID[:], content[cursor:cursor+16])
			cursor += 16

			replicaLen := int(content[cursor]) - 1
			cursor++
			for range replicaLen {
				partition.ReplicaNodes = append(partition.ReplicaNodes, int32(binary.BigEndian.Uint32(content[cursor:cursor+4])))
				cursor += 4
			}

			isrLen := int(content[cursor]) - 1
			cursor++
			for range isrLen {
				partition.IsrNodes = append(partition.IsrNodes, int32(binary.BigEndian.Uint32(content[cursor:cursor+4])))
				cursor += 4
			}

			removingLen := int(content[cursor]) - 1
			cursor += 1 + removingLen*4

			addingLen := int(content[cursor]) - 1
			cursor += 1 + addingLen*4

			partition.LeaderID = int32(binary.BigEndian.Uint32(content[cursor : cursor+4]))
			cursor += 4
			partition.LeaderEpoch = int32(binary.BigEndian.Uint32(content[cursor : cursor+4]))
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

