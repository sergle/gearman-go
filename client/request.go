package client

import (
	"encoding/binary"
)

// newPacket writes the 12-byte header and returns the packet plus its body
// region, for the caller to fill in place. One buffer per packet: building the
// body separately would copy every payload twice.
func newPacket(dataType uint32, bodyLen int) (packet, body []byte) {
	packet = getBuffer(minPacketLength + bodyLen)
	copy(packet[:4], reqStr)
	binary.BigEndian.PutUint32(packet[4:8], dataType)
	binary.BigEndian.PutUint32(packet[8:12], uint32(bodyLen))
	return packet, packet[minPacketLength:]
}

// encodeRequest frames a body of one opaque argument: ECHO_REQ.
func encodeRequest(dataType uint32, arg []byte) (packet []byte) {
	packet, body := newPacket(dataType, len(arg))
	copy(body, arg)
	return
}

// encodeRequestString is encodeRequest for a string argument: GET_STATUS, and
// OPTION_REQ, which takes the bare option name with no trailing NULL. copy
// takes the string directly; []byte(s) first would allocate a second copy.
func encodeRequestString(dataType uint32, arg string) (packet []byte) {
	packet, body := newPacket(dataType, len(arg))
	copy(body, arg)
	return
}

// encodeJob frames a SUBMIT_JOB*: funcname, id and data joined by NUL. The
// separators are written, not left to getBuffer's zeroing, so a pool behind it
// would stay safe here.
func encodeJob(dataType uint32, funcname, id string, data []byte) (packet []byte) {
	packet, body := newPacket(dataType, len(funcname)+1+len(id)+1+len(data))
	n := copy(body, funcname)
	body[n] = 0
	n++
	n += copy(body[n:], id)
	body[n] = 0
	n++
	copy(body[n:], data)
	return
}
