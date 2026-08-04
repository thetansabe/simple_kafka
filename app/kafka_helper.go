package main

import (
	"log"
	"net"
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
			{APIKey: 18, MinVersion: 0, MaxVersion: 4, TagBuffer: 0},
			// DescribeTopicPartitions
			{APIKey: 75, MinVersion: 0, MaxVersion: 0, TagBuffer: 0},
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
		resp.NextCursor = 0xff // null cursor

	default:
		resp.ErrorCode = ErrUnsupportedVersion
	}
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

		// partitions compact array length (N+1, empty = 1)
		res = append(res, byte(len(topic.Partitions)+1))

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
