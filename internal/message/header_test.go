package message

import (
	"bytes"
	"reflect"
	"testing"
)

func TestHeader_EncodeDecode(t *testing.T) {
	// Input object
	inputHeader := NewHeader([]byte{0xDE, 0xAD}, []byte{0xBE, 0xEF})

	inputhBytes := inputHeader.EncodeBytes()
	hBytes := make([]byte, len(inputhBytes))
	copy(hBytes, inputhBytes)

	outputHeader, err := NewHeaderFromBytes(hBytes)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(inputHeader, outputHeader) {
		t.Error("input does not match output")
	}
}

func TestHeaderList_EncodeDecode(t *testing.T) {
	// Input object
	inputHeaders := make([]Header, 0)
	for i := uint8(0); i < 10; i++ {
		inputHeaders = append(inputHeaders, NewHeader([]byte{i}, []byte{i * i}))
	}

	inputhBytes := make([]byte, 0)

	for _, h := range inputHeaders {
		inputhBytes = append(inputhBytes, h.EncodeBytes()...)
	}

	hBytes := make([]byte, len(inputhBytes))
	copy(hBytes, inputhBytes)

	outputHeaders, err := NewHeaderListFromBytes(hBytes)
	if err != nil {
		t.Error(err)
	}

	for i := uint8(0); i < 10; i++ {
		h := outputHeaders[i]

		if !bytes.Equal(h.Key(), []byte{i}) {
			t.Errorf("input does not match output; expected [%x], got [%x]", []byte{i}, h.Key())
		}
		if !bytes.Equal(h.Value(), []byte{i * i}) {
			t.Errorf("input does not match output; expected [%x], got [%x]", []byte{i * i}, h.Value())
		}
	}
}
