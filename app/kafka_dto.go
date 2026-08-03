package app

type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string
	OptionalTags  struct{} // left empty so far
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
}
