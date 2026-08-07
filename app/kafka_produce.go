package main

import (
	"fmt"
	"os"
)

func (resp *Response) initProduce(req *Request) {
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
}

// https://binspec.org/kafka-produce-unknown-topic-or-partition-response-v11?highlight=4-7
func (resp *Response) ConstructResponseForProduce() []byte {
	/* ====== response header v1 ====== */
	res := responseHeaderV1(resp.CorrelationID)

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
			res = appendInt32(res, p.PartitionIndex) // index (int32)
			res = appendInt16(res, p.ErrCode)        // error_code (int16)

			if p.ErrCode != ErrNone {
				// error case: base_offset = -1, log_start_offset = -1
				res = appendInt64(res, -1) // base_offset = -1
				res = appendInt64(res, -1) // log_append_time_ms = -1
				res = appendInt64(res, -1) // log_start_offset = -1
			} else {
				// success case: base_offset = 0 (first record offset), log_start_offset = 0
				res = appendInt64(res, 0)  // base_offset = 0
				res = appendInt64(res, -1) // log_append_time_ms = -1
				res = appendInt64(res, 0)  // log_start_offset = 0
			}

			res = append(res, 0x01) // record_errors: empty compact array
			res = append(res, 0x00) // error_message: null compact string
			res = append(res, 0x00) // TAG_BUFFER
		}
		res = append(res, 0x00) // TAG_BUFFER per topic
	}

	res = appendInt32(res, resp.ThrottleTimeMS) // throttle_time_ms
	res = append(res, 0x00)                    // TAG_BUFFER
	return res
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
