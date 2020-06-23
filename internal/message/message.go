package message

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

type Message interface {
	Topic() string
	Partition() int32
	Offset() int64
	Key() []byte
	Value() []byte
	Headers() []Header
	Timestamp() time.Time
	BlockTimestamp() time.Time

	EncodeBytes() []byte
}

type message struct {
	topic          string
	partition      int32
	offset         int64
	key            []byte
	value          []byte
	headers        []Header
	timestamp      time.Time
	blockTimestamp time.Time
}

func NewMessage(topic string, partition int32, offset int64, key, value []byte, timestamp, blockTimestamp time.Time, headers []Header) Message {
	return &message{
		topic:          topic,
		partition:      partition,
		offset:         offset,
		key:            key,
		value:          value,
		headers:        headers,
		timestamp:      timestamp,
		blockTimestamp: blockTimestamp,
	}
}

func NewMessageFromBytes(data []byte) (Message, error) {
	if len(data) <= 8 {
		return nil, errors.New("raw message data is too short")
	}

	dataLength := binary.BigEndian.Uint64(data[0:8])
	if dataLength != uint64(len(data[8:])) {
		return nil, errors.New(fmt.Sprintf("message: input data length mis-match; expected [%d], got [%d]", dataLength, len(data[8:])))
	}
	// 2 uint64 + some data
	if dataLength <= 16 {
		return nil, errors.New("data section is too short")
	}

	topicNameOffset := uint64(8)
	topicNameLength := uint64(data[topicNameOffset])
	topicName := string(data[topicNameOffset+1 : topicNameOffset+1+topicNameLength])

	partitionOffset := topicNameOffset + 1 + topicNameLength
	partition := int32(binary.BigEndian.Uint32(data[partitionOffset : partitionOffset+4]))

	offsetOffset := partitionOffset + 4
	offset := int64(binary.BigEndian.Uint64(data[offsetOffset : offsetOffset+8]))

	keyOffset := offsetOffset + 8
	keyLength := binary.BigEndian.Uint64(data[keyOffset : keyOffset+8])
	var key []byte = nil
	if keyLength > 0 {
		key = data[keyOffset+8 : keyOffset+8+keyLength]
	}

	valueOffset := keyOffset + 8 + keyLength
	valueLength := binary.BigEndian.Uint64(data[valueOffset : valueOffset+8])
	var value []byte = nil
	if valueLength > 0 {
		value = data[valueOffset+8 : valueOffset+8+valueLength]
	}

	headersOffset := valueOffset + 8 + valueLength
	headersLength := binary.BigEndian.Uint64(data[headersOffset : headersOffset+8])
	var headers []Header = nil
	if headersLength > 0 {
		var err error
		headersData := data[headersOffset+8 : headersOffset+8+headersLength]
		headers, err = NewHeaderListFromBytes(headersData)
		if err != nil {
			return nil, err
		}
	}

	timestampOffset := headersOffset + 8 + headersLength
	timestampLength := binary.BigEndian.Uint64(data[timestampOffset : timestampOffset+8])
	timestamp := time.Now()
	err := timestamp.UnmarshalBinary(data[timestampOffset+8 : timestampOffset+8+timestampLength])
	if err != nil {
		return nil, err
	}

	blockTimestampOffset := timestampOffset + 8 + timestampLength
	blockTimestampLength := binary.BigEndian.Uint64(data[blockTimestampOffset : blockTimestampOffset+8])
	blockTimestamp := time.Now()
	err = blockTimestamp.UnmarshalBinary(data[blockTimestampOffset+8 : blockTimestampOffset+8+blockTimestampLength])
	if err != nil {
		return nil, err
	}

	return NewMessage(topicName, partition, offset, key, value, timestamp, blockTimestamp, headers), nil
}

func (m *message) Topic() string {
	return m.topic
}

func (m *message) Partition() int32 {
	return m.partition
}

func (m *message) Offset() int64 {
	return m.offset
}

func (m *message) Key() []byte {
	return m.key
}

func (m *message) Value() []byte {
	return m.value
}

func (m *message) Headers() []Header {
	return m.headers
}

func (m *message) Timestamp() time.Time {
	return m.timestamp
}

func (m *message) BlockTimestamp() time.Time {
	return m.blockTimestamp
}

// format: uint64 - data length
//         data:
//             uint8  - topic name length
//             binary - topic name data
//             int32  - partition ID
//             int64  - offset
//             uint64 - key length
//             binary - key data
//             uint64 - value length
//             binary - value data
//             uint64 - headers length
//             binary - headers data
//             uint64 - timestamp length
//             binary - timestamp data
//             uint64 - block timestamp length
//             binary - block timestamp data
func (m *message) EncodeBytes() []byte {
	topicNameData := []byte(m.topic)
	topicNameLength := uint8(len(topicNameData))

	var keyLength uint64 = 0
	if m.key != nil {
		keyLength = uint64(len(m.key))
	}
	var valueLength uint64 = 0
	if m.value != nil {
		valueLength = uint64(len(m.value))
	}

	headersData := make([]byte, 0)
	for _, h := range m.headers {
		if h == nil {
			// TODO: fix this issue
			//log.Errorf("Header in byte encode function was nil for topic [%s], partition [%d], offset [%d]. Skipping this header", m.topic, m.partition, m.offset)
			continue
		}
		headersData = append(headersData, h.EncodeBytes()...)
	}
	headersLength := uint64(len(headersData))

	timestampData, err := m.timestamp.MarshalBinary()
	if err != nil {
		panic(err)
	}
	timestampLength := uint64(len(timestampData))

	blockTimestampData, err := m.blockTimestamp.MarshalBinary()
	if err != nil {
		panic(err)
	}
	blockTimestampLength := uint64(len(blockTimestampData))

	// total length of binary block
	dataLength := 1 + uint64(topicNameLength) + 4 + 8 + 8 + keyLength + 8 + valueLength + 8 + headersLength + 8 + timestampLength + 8 + blockTimestampLength

	result := make([]byte, dataLength+8)

	// Calculate consumer_offsets
	dataOffset := uint64(0)
	topicNameOffset := dataOffset + 8
	partitionOffset := topicNameOffset + 1 + uint64(topicNameLength)
	offsetOffset := partitionOffset + 4
	keyOffset := offsetOffset + 8
	valueOffset := keyOffset + 8 + keyLength
	headersOffset := valueOffset + 8 + valueLength
	timestampOffset := headersOffset + 8 + headersLength
	blockTimestampOffset := timestampOffset + 8 + timestampLength

	// data length
	binary.BigEndian.PutUint64(result[0:8], dataLength)
	// topic name
	result[topicNameOffset] = topicNameLength
	copy(result[topicNameOffset+1:partitionOffset], topicNameData[:])
	// partition
	binary.BigEndian.PutUint32(result[partitionOffset:offsetOffset], uint32(m.partition))
	// offset
	binary.BigEndian.PutUint64(result[offsetOffset:keyOffset], uint64(m.offset))
	// key
	binary.BigEndian.PutUint64(result[keyOffset:keyOffset+8], keyLength)
	if keyLength != 0 {
		copy(result[keyOffset+8:valueOffset], m.key[:])
	}
	// value
	binary.BigEndian.PutUint64(result[valueOffset:valueOffset+8], valueLength)
	if valueLength != 0 {
		copy(result[valueOffset+8:headersOffset], m.value[:])
	}
	// headers
	binary.BigEndian.PutUint64(result[headersOffset:headersOffset+8], headersLength)
	copy(result[headersOffset+8:timestampOffset], headersData[:])
	// timestamp
	binary.BigEndian.PutUint64(result[timestampOffset:timestampOffset+8], timestampLength)
	copy(result[timestampOffset+8:blockTimestampOffset], timestampData[:])
	// block timestamp
	binary.BigEndian.PutUint64(result[blockTimestampOffset:blockTimestampOffset+8], blockTimestampLength)
	copy(result[blockTimestampOffset+8:blockTimestampOffset+8+blockTimestampLength], timestampData[:])

	return result
}
