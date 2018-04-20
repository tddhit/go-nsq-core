package nsq

import (
	"encoding/binary"
	"errors"
	"io"
	"sync/atomic"
	"time"
)

const MsgIDLength = 16

type MessageID [MsgIDLength]byte

type Message struct {
	ID        MessageID
	Body      []byte
	Timestamp int64
	Attempts  uint16

	NSQDAddress string

	Delegate MessageDelegate

	autoResponseDisabled int32
	responded            int32
}

func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

func (m *Message) DisableAutoResponse() {
	atomic.StoreInt32(&m.autoResponseDisabled, 1)
}

func (m *Message) IsAutoResponseDisabled() bool {
	return atomic.LoadInt32(&m.autoResponseDisabled) == 1
}

func (m *Message) HasResponded() bool {
	return atomic.LoadInt32(&m.responded) == 1
}

func (m *Message) Finish() {
	if !atomic.CompareAndSwapInt32(&m.responded, 0, 1) {
		return
	}
	m.Delegate.OnFinish(m)
}

func (m *Message) Touch() {
	if m.HasResponded() {
		return
	}
	m.Delegate.OnTouch(m)
}

func (m *Message) Requeue(delay time.Duration) {
	m.doRequeue(delay, true)
}

func (m *Message) RequeueWithoutBackoff(delay time.Duration) {
	m.doRequeue(delay, false)
}

func (m *Message) doRequeue(delay time.Duration, backoff bool) {
	if !atomic.CompareAndSwapInt32(&m.responded, 0, 1) {
		return
	}
	m.Delegate.OnRequeue(m, delay, backoff)
}

func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64

	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))

	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.ID[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

// DecodeMessage deserializes data (as []byte) and creates a new Message
// message format:
//  [x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
//  |       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
//  |       8-byte         ||    ||                 16-byte                      || N-byte
//  ------------------------------------------------------------------------------------------...
//    nanosecond timestamp    ^^                   message ID                       message body
//                         (uint16)
//                          2-byte
//                         attempts
func DecodeMessage(b []byte) (*Message, error) {
	var msg Message

	if len(b) < 10+MsgIDLength {
		return nil, errors.New("not enough data to decode valid message")
	}

	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])
	copy(msg.ID[:], b[10:10+MsgIDLength])
	msg.Body = b[10+MsgIDLength:]

	return &msg, nil
}
