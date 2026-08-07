package main

func (resp *Response) initApiVersions(req *Request) {
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
}

func (resp *Response) ConstructResponseForApiVersions() []byte {
	/* ==================header================== */
	// ApiVersions uses response header v0 (no TAG_BUFFER)
	res := responseHeaderV0(resp.CorrelationID)

	/* ===================body=================== */
	// err_code, 2 bytes
	res = appendInt16(res, resp.ErrorCode)

	// api_keys array lenght, encoded as the real length plus 1
	res = append(res, byte(len(resp.APIKeys)+1))

	// APIKeys
	for _, apiKey := range resp.APIKeys {
		res = appendInt16(res, apiKey.APIKey)
		res = appendInt16(res, apiKey.MinVersion)
		res = appendInt16(res, apiKey.MaxVersion)
		res = append(res, apiKey.TagBuffer)
	}

	// throttle_time_ms, 4 bytes
	res = appendInt32(res, resp.ThrottleTimeMS)

	// tag_buffer, 1 byte
	res = append(res, resp.TagBuffer)
	return res
}
