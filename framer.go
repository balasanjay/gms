package gms

import (
	"fmt"
	"io"
)

type writeFlusher interface {
	io.Writer
	Flush() error
}

// framer is an implementation of io.Reader and io.Writer that encapsulates many
// of the low level details of the MySQL wire protocol. In particular, it is
// responsible for handling sequence numbers, splitting writes into packets,
// and merging incoming packets into contiguous streams.
type framer struct {
	// Read-related fields
	reader             io.Reader
	maxRecvPacketSize  uint32
	mergeNextPacket    bool
	curPacketRemaining uint32

	// Write-related fields
	writer            writeFlusher
	maxSendPacketSize uint32
	curWritePacketLen int64 // -1 => no packet being sent
	availableWriteCap uint32
	needsTrailer      bool

	// Common fields
	scratch         [4]byte
	nextExpectedSeq uint8
}

// Read fulfills the io.Reader contract.
func (f *framer) Read(buf []byte) (int, error) {
	// If we are done reading the current packet, but are supposed to merge the
	// next one, then we do so.
	if f.curPacketRemaining <= 0 && f.mergeNextPacket {
		nextPacketLen, nextSeq, err := f.readPacketHeader()
		if err != nil {
			return 0, err
		}

		if nextPacketLen < f.maxRecvPacketSize {
			f.mergeNextPacket = false
			f.curPacketRemaining = nextPacketLen
		} else {
			f.mergeNextPacket = true
			f.curPacketRemaining = f.maxRecvPacketSize
		}

		if nextSeq != f.nextExpectedSeq {
			return 0, fmt.Errorf("While reading packet header, was expecting sequence id %d, got %d.", f.nextExpectedSeq, nextSeq)
		}
		f.nextExpectedSeq++
	}

	// If we're done reading the current packet, and don't need to merge the
	// next one, then we return EOF.
	if f.curPacketRemaining <= 0 && !f.mergeNextPacket {
		return 0, io.EOF
	}

	// If we reached this far, then f.curPacketRemaining must be greater than 0.

	// Limit our buffer size if it is greater than the remaining packet size.
	if len(buf) > int(f.curPacketRemaining) {
		buf = buf[:f.curPacketRemaining]
	}

	// Do the actual read.
	rd, err := f.reader.Read(buf)
	if rd > 0 {
		f.curPacketRemaining -= uint32(rd)
	}
	if err == io.EOF && f.curPacketRemaining > 0 {
		err = io.ErrUnexpectedEOF
	}
	return rd, err
}

// readPacketHeader reads the next 4 bytes from f.reader, parses them as a
// packet header, and returns a (packetLength, sequenceNumber, error) triple.
// If f.reader returns EOF before yielding 4 bytes, then io.ErrUnexpectedEOF is
// returned as the error. Any other error is passed through verbatim.
func (f *framer) readPacketHeader() (uint32, uint8, error) {
	// First, we read 4 bytes.
	buf := f.scratch[:]
	for len(buf) > 0 {
		n, err := f.reader.Read(buf)
		if n > 0 {
			buf = buf[n:]
		}
		if err == io.EOF {
			if len(buf) > 0 {
				return 0, 0, io.ErrUnexpectedEOF
			} else {
				break
			}
		} else if err != nil {
			return 0, 0, err
		}
	}

	// Extract packet length
	packetLen := uint32(buf[0]) | uint32(buf[1])<<8 | uint32(buf[2])<<16

	// Extract sequence number
	nextSeq := buf[3]

	return packetLen, nextSeq, nil
}

func (f *framer) BeginPacket(packetSize int64) {
	if f.curWritePacketLen != -1 {
		panic("currently writing a packet, perhaps EndPacket was not called?")
	}
	if f.availableWriteCap != 0 {
		panic("internal error, availableWriteCap should be 0 before starting a new packet")
	}
	if packetSize <= 0 {
		panic("trying to BeginPacket with a non-positive size")
	}

	f.curWritePacketLen = packetSize
}

func (f *framer) Write(buf []byte) (int, error) {
	if f.curWritePacketLen == -1 {
		panic("not writing a packet, perhaps BeginPacket was not called?")
	} else if int64(len(buf)) > f.curWritePacketLen {
		panic("cannot write more than precalculated packet size.")
	}

	written := 0
	for len(buf) > 0 || f.needsTrailer {
		// First, if we need to put a new packet header, we do that.
		if f.availableWriteCap == 0 {
			var newWriteCap uint32
			if f.needsTrailer {
				newWriteCap = 0
				f.needsTrailer = false
			} else if f.curWritePacketLen == int64(f.maxSendPacketSize) {
				newWriteCap = f.maxSendPacketSize
				f.needsTrailer = true
			} else if f.curWritePacketLen < int64(f.maxSendPacketSize) {
				newWriteCap = uint32(f.curWritePacketLen)
				f.needsTrailer = false
			} else {
				newWriteCap = f.maxSendPacketSize
				f.needsTrailer = false
			}

			f.scratch[0] = byte(newWriteCap)
			f.scratch[1] = byte(newWriteCap >> 8)
			f.scratch[2] = byte(newWriteCap >> 16)
			f.scratch[3] = f.nextExpectedSeq
			f.nextExpectedSeq++

			n, err := f.writer.Write(f.scratch[:4])
			if err == io.EOF && n < 4 {
				return written, io.ErrUnexpectedEOF
			} else if err == io.EOF {
				// no-op, this could be OK if it is the trailer for the last
				// packet
			} else if err != nil {
				return written, err
			}

			f.availableWriteCap = newWriteCap
			continue
		}

		nextSendSize := f.availableWriteCap
		if bufsize := uint32(len(buf)); bufsize < nextSendSize {
			nextSendSize = bufsize
		}

		n, err := f.writer.Write(buf[:nextSendSize])
		buf = buf[n:]
		f.curWritePacketLen -= int64(n)
		f.availableWriteCap -= uint32(n)
		written += n
		if err != nil {
			return written, err
		}
	}

	return written, nil
}

func (f *framer) EndPacket(flush bool) error {
	if f.curWritePacketLen != 0 {
		panic(fmt.Sprintf("internal error, miscalculated packet size, still %v bytes on previous packet", f.curWritePacketLen))
	}

	if f.needsTrailer {
		_, err := f.Write(nil)
		if err != nil {
			return err
		}
	}

	if !flush {
		return nil
	}
	return f.writer.Flush()
}

var (
	_ io.Reader = (*framer)(nil)
	_ io.Writer = (*framer)(nil)
)
