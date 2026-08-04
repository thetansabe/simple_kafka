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
	// header
	MsgSize       int32
	CorrelationID int32

	// body
	ErrorCode      int16
	APIKeys        []APIKey // compact array - list of specified struct
	ThrottleTimeMS int32    // throttle time in milliseconds
	TagBuffer      byte     //optional tagged fields just as RequestHeader.OptionalTags
}

type APIKey struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
	TagBuffer  byte
}
