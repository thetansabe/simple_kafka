package main

import (
	"encoding/binary"
	"fmt"
	"os"
)

func (resp *Response) initFetch(req *Request) {
	resp.FetchTopics = req.FetchTopics
}

// https://kafka.apache.org/43/design/protocol/
// search for Fetch Response (Version: 16) - that's the body
// search for Response Header v1 - that's the header
func (resp *Response) ConstructResponseForFetch() []byte {
	/* ====== response header v1 ====== */
	res := responseHeaderV1(resp.CorrelationID)

	/* ============= body ============= */
	res = appendInt32(res, 0) // throttle_time_ms (4 bytes)
	res = appendInt16(res, 0) // error_code (2 bytes)
	res = appendInt32(res, 0) // session_id (4 bytes)

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
			res = appendInt32(res, p.Partition) // partition_index

			if !exists {
				res = appendInt16(res, UNKNOWN_TOPIC_ID) // error_code = 100
				res = appendInt64(res, 0)                // high_watermark
				res = appendInt64(res, 0)                // last_stable_offset
				res = appendInt64(res, 0)                // log_start_offset
				res = append(res, 0x01)                  // aborted_transactions: empty
				res = appendInt32(res, -1)               // preferred_read_replica: -1
				res = append(res, 0x01)                  // records: empty
				res = append(res, 0x00)                  // TAG_BUFFER
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

			// Walk all RecordBatches in the file.
			// Only include batches whose last offset >= fetch_offset (p.FetchOffset).
			// highWatermark = last batch's baseOffset + recordsCount.
			highWatermark := int64(0)
			recordsToSend := []byte{}
			for pos := 0; pos+12 <= len(records); {
				baseOff := int64(binary.BigEndian.Uint64(records[pos : pos+8]))
				batchLen := int(binary.BigEndian.Uint32(records[pos+8 : pos+12]))
				batchEnd := pos + 12 + batchLen
				if batchEnd > len(records) {
					break
				}
				if pos+61 <= len(records) {
					recordsCount := int64(binary.BigEndian.Uint32(records[pos+57 : pos+61]))
					highWatermark = baseOff + recordsCount
					// include this batch only if it contains offsets >= fetch_offset
					lastOffsetInBatch := baseOff + recordsCount - 1
					if lastOffsetInBatch >= p.FetchOffset {
						recordsToSend = append(recordsToSend, records[pos:batchEnd]...)
					}
				}
				pos = batchEnd
			}

			res = appendInt16(res, ErrNone)       // error_code = 0
			res = appendInt64(res, highWatermark) // high_watermark
			res = appendInt64(res, highWatermark) // last_stable_offset = high_watermark
			res = appendInt64(res, 0)             // log_start_offset
			res = append(res, 0x01)               // aborted_transactions: empty
			res = appendInt32(res, -1)            // preferred_read_replica: -1
			// records: compact bytes — length = len(recordsToSend)+1, then raw bytes
			res = appendCompactBytes(res, recordsToSend)
			res = append(res, 0x00) // TAG_BUFFER
		}
		res = append(res, 0x00) // TAG_BUFFER per topic
	}

	res = append(res, 0x00) // body TAG_BUFFER
	return res
}

