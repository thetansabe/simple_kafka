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
			{APIKey: 18, MinVersion: 0, MaxVersion: 4, TagBuffer: 0},
		}
	default:
		resp.ErrorCode = ErrUnsupportedVersion
	}
}

func RecieveRequest(conn net.Conn) (*Request, error) {
	var buf = make([]byte, 1024) // 1024 is reasonable buffer size, as kafka msg is limited to 255 bytes
	_, err := conn.Read(buf)     // Load conn data into the buffer

	if err != nil {
		return nil, err
	}

	// msg_size is first 4 bytes (), and remember to left shift (read wa6 doc for details)
	// Simple rule: each byte is 8 bits wide
	// shift = (number of bytes to the RIGHT of this byte) × 8
	msg_size := int32(buf[0])<<24 | int32(buf[1])<<16 | int32(buf[2])<<8 | int32(buf[3])

	api_key := int16(buf[4])<<8 | int16(buf[5])

	api_version := int16(buf[6])<<8 | int16(buf[7])

	correlation_id := int32(buf[8])<<24 | int32(buf[9])<<16 | int32(buf[10])<<8 | int32(buf[11])

	return &Request{
		MsgSize: msg_size,
		RequestHeader: RequestHeader{
			APIKey:        api_key,
			APIVersion:    api_version,
			CorrelationID: correlation_id,
			ClientID:      "", // ClientID is not used in this stage, so we can leave it empty
		},
		RequestBody: RequestBody{}, // RequestBody is not used in this stage, so we can leave it empty
	}, nil
}

func (resp *Response) SendResp(conn net.Conn) error {
	log.Printf("Sending response: CorrelationID=%d, ErrorCode=%d", resp.CorrelationID, resp.ErrorCode)

	/* ==================header================== */
	// correlation_id, 4 bytes
	res := []byte{
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
