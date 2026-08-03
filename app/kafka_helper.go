package app

import "net"

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
	_, err := conn.Write([]byte{byte(0), byte(0), byte(0), byte(0),
		byte(resp.CorrelationID >> 24), byte(resp.CorrelationID >> 16),
		byte(resp.CorrelationID >> 8), byte(resp.CorrelationID)})
	return err
}
