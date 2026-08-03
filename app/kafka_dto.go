package main

const (
	APIKeyApiVersions int16 = 18

	ErrNone               int16 = 0
	ErrUnsupportedVersion int16 = 35
)

type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string
	OptionalTags  struct{}
}

type RequestBody struct{}

type Request struct {
	MsgSize int32
	RequestHeader
	RequestBody
}

type Response struct {
	MsgSize       int32
	CorrelationID int32
	ErrorCode     int16
}
