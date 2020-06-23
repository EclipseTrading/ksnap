package message

import (
	"bytes"
	"reflect"
	"testing"
	"time"
)

func TestMessage_EncodeDecode(t *testing.T) {
	// Input object
	inputHeaders := make([]Header, 0)
	inputHeaders = append(inputHeaders, NewHeader([]byte{0xDE, 0xAD}, []byte{0xBE, 0xEF}))
	inputTime := time.Now()
	inputMessage := NewMessage("test-topic", 0, 100, []byte{0xDE, 0xAD}, []byte{0xBE, 0xEF}, inputTime, inputTime, inputHeaders)

	inputMsgBytes := inputMessage.EncodeBytes()
	msgBytes := make([]byte, len(inputMsgBytes))
	copy(msgBytes[:], inputMsgBytes[:])

	outputMessage, err := NewMessageFromBytes(msgBytes)
	if err != nil {
		t.Error(err)
	}

	if inputMessage.Topic() != outputMessage.Topic() {
		t.Errorf("topic name: expected [%s], got [%s]", inputMessage.Topic(), outputMessage.Topic())
	}
	if inputMessage.Partition() != outputMessage.Partition() {
		t.Errorf("partition: expected [%d], got [%d]", inputMessage.Partition(), outputMessage.Partition())
	}
	if inputMessage.Offset() != outputMessage.Offset() {
		t.Errorf("offset: expected [%d], got [%d]", inputMessage.Offset(), outputMessage.Offset())
	}
	if !bytes.Equal(inputMessage.Key(), outputMessage.Key()) {
		t.Errorf("key: expected [%x], got [%x]", inputMessage.Key(), outputMessage.Key())
	}
	if !bytes.Equal(inputMessage.Value(), outputMessage.Value()) {
		t.Errorf("value: expected [%x], got [%x]", inputMessage.Value(), outputMessage.Value())
	}
	if !reflect.DeepEqual(inputMessage.Headers(), outputMessage.Headers()) {
		t.Errorf("headers do not match")
	}
	if !inputMessage.Timestamp().Equal(outputMessage.Timestamp()) {
		t.Errorf("timestamp: expected [%s], got [%s]", inputMessage.Timestamp().String(), outputMessage.Timestamp().String())
	}
	if !inputMessage.BlockTimestamp().Equal(outputMessage.BlockTimestamp()) {
		t.Errorf("block timestamp: expected [%s], got [%s]", inputMessage.BlockTimestamp().String(), outputMessage.BlockTimestamp().String())
	}
}
