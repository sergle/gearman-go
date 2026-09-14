package worker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
)

// Worker side job
type inPack struct {
	dataType             uint32
	data                 []byte
	handle, uniqueId, fn string
	a                    *agent
}

// Create a new job
func getInPack() *inPack {
	return &inPack{}
}

func (inpack *inPack) Data() []byte {
	return inpack.data
}

func (inpack *inPack) Fn() string {
	return inpack.fn
}

func (inpack *inPack) Handle() string {
	return inpack.handle
}

func (inpack *inPack) UniqueId() string {
	return inpack.uniqueId
}

func (inpack *inPack) Err() error {
	if inpack.dataType == dtError {
		return getError(inpack.data)
	}
	return nil
}

// Send some datas to client.
// Using this in a job's executing.
func (inpack *inPack) SendData(data []byte) {
	outpack := getOutPack()
	outpack.dataType = dtWorkData
	hl := len(inpack.handle)
	l := hl + len(data) + 1
	outpack.data = getBuffer(l)
	copy(outpack.data, []byte(inpack.handle))
	copy(outpack.data[hl+1:], data)
	inpack.a.write(outpack)
}

func (inpack *inPack) SendWarning(data []byte) {
	outpack := getOutPack()
	outpack.dataType = dtWorkWarning
	hl := len(inpack.handle)
	l := hl + len(data) + 1
	outpack.data = getBuffer(l)
	copy(outpack.data, []byte(inpack.handle))
	copy(outpack.data[hl+1:], data)
	inpack.a.write(outpack)
}

// Update status.
// Tall client how many percent job has been executed.
func (inpack *inPack) UpdateStatus(numerator, denominator int) {
	n := []byte(strconv.Itoa(numerator))
	d := []byte(strconv.Itoa(denominator))
	outpack := getOutPack()
	outpack.dataType = dtWorkStatus
	hl := len(inpack.handle)
	nl := len(n)
	dl := len(d)
	outpack.data = getBuffer(hl + nl + dl + 2)
	copy(outpack.data, []byte(inpack.handle))
	copy(outpack.data[hl+1:], n)
	copy(outpack.data[hl+nl+2:], d)
	inpack.a.write(outpack)
}

// Decode job from byte slice
func decodeInPack(data []byte) (inpack *inPack, l int, err error) {
	if len(data) < minPacketLength { // valid package should not less 12 bytes
		err = fmt.Errorf("Invalid data: %v", data)
		return
	}
	dl := int(binary.BigEndian.Uint32(data[8:12]))
	if len(data) < (dl + minPacketLength) {
		err = fmt.Errorf("Not enough data: %v", data)
		return
	}
	dt := data[minPacketLength : dl+minPacketLength]
	if len(dt) != int(dl) { // length not equal
		err = fmt.Errorf("Invalid data: %v", data)
		return
	}
	inpack = getInPack()
	inpack.dataType = binary.BigEndian.Uint32(data[4:8])
	switch inpack.dataType {
	// Scanned in place, not SplitN: the field slice is an alloc per job.
	// Assignment stays all-or-nothing -- too few separators leaves every field
	// zero, which exec relies on -- and the last field keeps its NULs.
	case dtJobAssign:
		i := bytes.IndexByte(dt, '\x00')
		if i < 0 {
			break
		}
		j := bytes.IndexByte(dt[i+1:], '\x00')
		if j < 0 {
			break
		}
		j += i + 1
		inpack.handle = string(dt[:i])
		inpack.fn = string(dt[i+1 : j])
		inpack.data = dt[j+1:]
	case dtJobAssignUniq:
		i := bytes.IndexByte(dt, '\x00')
		if i < 0 {
			break
		}
		j := bytes.IndexByte(dt[i+1:], '\x00')
		if j < 0 {
			break
		}
		j += i + 1
		k := bytes.IndexByte(dt[j+1:], '\x00')
		if k < 0 {
			break
		}
		k += j + 1
		inpack.handle = string(dt[:i])
		inpack.fn = string(dt[i+1 : j])
		inpack.uniqueId = string(dt[j+1 : k])
		inpack.data = dt[k+1:]
	default:
		inpack.data = dt
	}
	l = dl + minPacketLength
	return
}
