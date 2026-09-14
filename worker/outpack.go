package worker

import (
	"encoding/binary"
)

// Worker side job
type outPack struct {
	dataType uint32
	data     []byte
	handle   string
}

func getOutPack() (outpack *outPack) {
	// TODO pool
	return &outPack{}
}

// Encode a job to byte slice
func (outpack *outPack) Encode() (data []byte) {
	return outpack.encodeInto(nil)
}

// encodeInto encodes into buf when it has the room, allocating otherwise. Every
// byte of the returned slice is written -- header, handle, the separator after
// it, body -- so a reused buffer needs no re-zeroing, which is most of what the
// allocation costs on a large packet.
func (outpack *outPack) encodeInto(buf []byte) (data []byte) {
	var l int
	if outpack.dataType == dtWorkFail {
		l = len(outpack.handle)
	} else {
		l = len(outpack.data)
		if outpack.handle != "" {
			l += len(outpack.handle) + 1
		}
	}
	if n := l + minPacketLength; cap(buf) < n {
		data = getBuffer(n)
	} else {
		data = buf[:n]
	}
	binary.BigEndian.PutUint32(data[:4], req)
	binary.BigEndian.PutUint32(data[4:8], outpack.dataType)
	binary.BigEndian.PutUint32(data[8:minPacketLength], uint32(l))
	i := minPacketLength
	if outpack.handle != "" {
		hi := len(outpack.handle) + i
		copy(data[i:hi], []byte(outpack.handle))
		if outpack.dataType != dtWorkFail {
			data[hi] = '\x00'
		}
		i = hi + 1
	}
	if outpack.dataType != dtWorkFail {
		copy(data[i:], outpack.data)
	}
	return
}
