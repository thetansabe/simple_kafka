package main

const (
	APIKeyApiVersions         int16 = 18
	DESCRIBE_TOPIC_PARTITIONS int16 = 75
	FETCH                     int16 = 1

	ErrNone                    int16 = 0
	ErrUnsupportedVersion      int16 = 35
	UNKNOWN_TOPIC_OR_PARTITION int16 = 3
	UNKNOWN_TOPIC_ID           int16 = 100

	BrokerRecord          = 0x00
	TopicRecordWait       = 0x01
	TopicRecord           = 0x02
	PartitionRecord       = 0x03
	ConfigRecord          = 0x04
	PartitionChangeRecord = 0x05
	FeatureLevelRecord    = 0x0C
)

type RequestHeader struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientID      string
	OptionalTags  struct{}
}

type Request struct {
	MsgSize int32
	RequestHeader
	RequestBody
	FetchTopics []FetchTopic // populated for FETCH requests
}

type Response struct {
	// header
	MsgSize       int32
	CorrelationID int32

	// body
	ErrorCode      int16
	APIKeys        []APIKey // compact array - list of specified struct
	Topics         []TopicForResp
	NextCursor     byte  // cursor for pagination
	ThrottleTimeMS int32 // throttle time in milliseconds
	TagBuffer      byte  //optional tagged fields just as RequestHeader.OptionalTags
	FetchTopics    []FetchTopic // populated for FETCH responses
}

type APIKey struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
	TagBuffer  byte
}

// Topics

type Topic struct {
	TopicName string
	TagBuffer byte
}

type RequestBody struct {
	Topics           []Topic // compact array - list of specified struct
	RespPartitionLim int32   // limits number of partitions returned in the response
	Cursor           byte    // cursor for pagination
	TagBuffer        byte    // optional tagged fields
}

type TopicForResp struct {
	ErrCode                   int16       // err code
	TopicName                 string      // the topic name from the request
	TopicID                   [16]byte    // topic uuid, 16 bytes
	IsInternal                bool        // whether topic is internal
	Partitions                []Partition //	array of partition metadata	(empty for unknown)
	TopicAuthorizedOperations int32       // authorized operations bitfield (0)
	TagBuffer                 byte        // tagged fields, 0 so far
}

// left blank, since IDK what it is so far
type Partition struct {
	ErrCode                int16   // 0 for valid partitions
	PartitionIndex         int32   // partition ID, fuck, no better naming?
	LeaderID               int32   // broker ID hosting this partition
	LeaderEpoch            int32   // leader epoch
	ReplicaNodes           []int32 // array of replica broker IDs
	IsrNodes               []int32 // array of in-sync replica broker IDs
	EligibleLeaderReplicas []int32 // array of eligible leader replica broker IDs
	LastKnownElr           []int32 // array of last known eligible leader replicas
	OfflineReplicas        []int32 // array of offline replica broker IDs
	TagBuffer              byte
}

type LogMetadata struct {
	Name       string
	Partitions []Partition
}

// Fetch

type FetchRequestBody struct {
	MaxWaitMs           int32
	MinBytes            int32
	MaxBytes            int32
	IsolationLevel      int8
	SessionID           int32
	SessionEpoch        int32
	Topics              []FetchTopic
	ForgottenTopicsData []ForgottenTopicData
	RackId              string
}

type ForgottenTopicData struct {
	TopicId    []byte
	Partitions []int32
}
type FetchTopic struct {
	TopicId    [16]byte
	Partitions []FetchPartition
}

type FetchPartition struct {
	Partition          int32
	CurrentLeaderEpoch int32
	FetchOffset        int64
	LastFetchedEpoch   int32
	LogStartOffset     int64
	PartitionMaxBytes  int32
}
