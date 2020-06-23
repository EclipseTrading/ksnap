package message

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type Header interface {
	Key() []byte
	Value() []byte

	EncodeBytes() []byte
}

type header struct {
	key   []byte
	value []byte
}

func NewHeader(key, value []byte) Header {
	return &header{
		key:   key,
		value: value,
	}
}

func NewHeaderFromBytes(data []byte) (Header, error) {
	if len(data) <= 8 {
		return nil, errors.New("input data is too short")
	}

	dataLength := binary.BigEndian.Uint64(data[0:8])
	if dataLength != uint64(len(data[8:])) {
		return nil, errors.New(fmt.Sprintf("header: input data length mis-match; expected [%d], got [%d]", dataLength, len(data[8:])))
	}
	// 2 uint64 + some data
	if dataLength <= 16 {
		return nil, errors.New("input data is too short")
	}

	keyLength := binary.BigEndian.Uint64(data[8:16])
	var keyData []byte = nil
	if keyLength > 0 {
		keyData = data[16 : 16+keyLength]
	}

	valueLength := binary.BigEndian.Uint64(data[16+keyLength : 16+keyLength+8])
	var valueData []byte = nil
	if valueLength > 0 {
		valueData = data[16+keyLength+8 : 16+keyLength+8+valueLength]
	}

	return NewHeader(keyData, valueData), nil
}

func NewHeaderListFromBytes(data []byte) ([]Header, error) {
	result := make([]Header, 0)

	for {
		if len(data) <= 8 {
			return nil, errors.New("input data is too short")
		}

		hLength := binary.BigEndian.Uint64(data[0:8])
		if uint64(len(data)) < 8+hLength {
			return nil, errors.New(fmt.Sprintf("header2: input data length mis-match; expected [%d], got [%d]", hLength+8, len(data)))
		}
		hData := data[0 : 8+hLength]
		h, err := NewHeaderFromBytes(hData)
		if err != nil {
			return nil, err
		}

		result = append(result, h)

		data = data[8+hLength:]

		if len(data) == 0 {
			break
		}
	}

	return result, nil
}

func (h *header) Key() []byte {
	return h.key
}

func (h *header) Value() []byte {
	return h.value
}

// format: uint64 - data length
//         data:
//             uint64 - key length
//             binary - key data
//             uint64 - value length
//             binary - value data
func (h *header) EncodeBytes() []byte {
	var keyLength uint64 = 0
	if h.key != nil {
		keyLength = uint64(len(h.key))
	}
	var valueLength uint64 = 0
	if h.value != nil {
		valueLength = uint64(len(h.value))
	}
	dataLength := keyLength + valueLength + 16

	result := make([]byte, dataLength+8)

	binary.BigEndian.PutUint64(result[0:8], dataLength)
	binary.BigEndian.PutUint64(result[8:16], keyLength)
	if keyLength != 0 {
		copy(result[16:16+keyLength], h.key[:])
	}
	binary.BigEndian.PutUint64(result[16+keyLength:16+keyLength+8], valueLength)
	if valueLength != 0 {
		copy(result[16+keyLength+8:16+keyLength+8+valueLength], h.value[:])
	}

	return result
}
