package gms

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	drv "database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
)

type conn struct {
	// Original connection.
	rwc io.ReadWriteCloser

	// Buffered writer, wrapping rwc.
	bw *bufio.Writer

	// The size of the packet being written to bw. Call BeginPacketWrite to set
	// this size.
	curPacketSizeRemaining int64
	writeCap               int64

	// Buffered reader, wrapping rwc.
	br *bufio.Reader

	// Limited reader, for reading packets.
	lr io.LimitedReader

	// If true, then the next packet should be transparently merged into this
	// one.
	mergeNextPacket bool

	// The maximum size of a packet received from (sent to) the server.
	maxRecvPacketSize uint32
	maxSendPacketSize uint32

	// A scratch space buffer
	scratchBuf *bytes.Buffer

	// Capabilities flags for this connection
	serverFlags connectionFlag

	// The charset sent by the server during the initial handshake
	charset byte

	// The current sequence id.
	seqId uint8
}

func newConn(rwc io.ReadWriteCloser) *conn {
	// TODO(sanjay): tune these
	const (
		defaultWriteBufSize = 8192
		defaultReadBufSize  = 4096
	)

	c := &conn{}

	c.rwc = rwc

	c.bw = bufio.NewWriterSize(c.rwc, defaultWriteBufSize)

	c.curPacketSizeRemaining = 0
	c.writeCap = 0

	c.br = bufio.NewReaderSize(c.rwc, defaultReadBufSize)

	c.lr.N = 0
	c.lr.R = c.br

	c.mergeNextPacket = false

	// This are only the initial values, the starting handshake might update it.
	c.maxRecvPacketSize = (1 << 24) - 1
	c.maxSendPacketSize = (1 << 24) - 1

	c.scratchBuf = bytes.NewBuffer(nil)

	return c
}

func (c *conn) AdvancePacket() error {
	_, err := io.Copy(ioutil.Discard, c)
	if err != nil {
		return err
	}

	err = c.readPacketHeader()
	if err != nil {
		return err
	}

	return nil
}

func (c *conn) Read(buf []byte) (int, error) {
	// No data remaining on this packet, and we don't have to merge the next
	// packet.
	if c.lr.N <= 0 && !c.mergeNextPacket {
		return 0, io.EOF
	}

	// No data remaining on this packet, and we have to merge the next packet.
	// So we merge the next packet header, and carry on.
	if c.lr.N <= 0 && c.mergeNextPacket {
		// readPacketHeader guarantees that the next packet has size greater
		// than 0, so we can safely fall-through to our regular reading code.
		err := c.readPacketHeader()
		if err != nil {
			return 0, err
		}
	}

	// Some data remaining on current packet
	n, err := c.lr.Read(buf)
	if err == io.EOF && c.lr.N > 0 {
		return n, io.ErrUnexpectedEOF
	} else if err == io.EOF && c.mergeNextPacket {
		return n, nil
	}
	return n, err
}

// BeginPacketWrite sets up the conn to write a packet with size bytes.
func (c *conn) BeginPacketWrite(size int64) {
	if c.curPacketSizeRemaining != 0 || size == 0 || c.writeCap != 0 {
		panic("internal error, miscalculated packet size")
	}

	c.curPacketSizeRemaining = size
}

func (c *conn) Write(b []byte) (int, error) {
	if int64(len(b)) > c.curPacketSizeRemaining {
		panic("internal error, write larger than calculated packet size")
	}

	written := 0

	for len(b) > 0 {
		if c.writeCap == 0 {
			newWriteCap := int64(c.maxSendPacketSize)
			if c.curPacketSizeRemaining < newWriteCap {
				newWriteCap = c.curPacketSizeRemaining
			}

			var header [4]byte

			header[0] = byte(newWriteCap)
			header[1] = byte(newWriteCap >> 8)
			header[2] = byte(newWriteCap >> 16)
			header[3] = c.seqId
			c.seqId++

			n, err := c.bw.Write(header[:])
			if err != nil {
				return written, err
			} else if n != 4 {
				err = io.ErrShortWrite
				return written, err
			}

			c.writeCap = newWriteCap
			continue
		}

		n, err := c.bw.Write(b[:c.writeCap])

		b = b[c.writeCap:]
		c.curPacketSizeRemaining -= int64(n)
		c.writeCap -= int64(n)
		written += n

		if err != nil {
			return written, err
		}
	}

	return written, nil
}

// readPacket header reads the next 4 bytes from the connection, and configures
// the readers in the connection correctly. Note that it should only be called
// at packet boundaries, or at the beginning of a connection.
func (c *conn) readPacketHeader() error {
	var (
		buf       [4]byte
		err       error
		packetLen uint32
		nextSeq   uint8
	)

	_, err = io.ReadAtLeast(c.br, buf[:], len(buf))
	if err == nil {
		// Read packet length
		packetLen = uint32(buf[0]) |
			uint32(buf[1])<<8 |
			uint32(buf[2])<<16

		// Read sequence number
		nextSeq = buf[3]
	}

	if err == io.EOF {
		return io.ErrUnexpectedEOF
	} else if err != nil {
		return err
	} else if packetLen == 0 {
		return errors.New("unexpected 0-length packet")
	} else if nextSeq != c.seqId {
		return errors.New("unexpected sequence number")
	}

	c.seqId++

	c.lr.N = int64(packetLen)
	c.mergeNextPacket = (packetLen >= c.maxRecvPacketSize)
	return nil
}

var (
	zero = bytes.Repeat([]byte{0}, 32)
)

func (c *conn) handshake(username, password, db string) error {
	// TODO(sanjay): this currently buffers in memory. Switch to calculating
	// size and streaming it instead.

	err := c.AdvancePacket()
	if err != nil {
		return err
	}

	c.scratchBuf.Reset()

	_, err = io.Copy(c.scratchBuf, c)
	if err != nil {
		panic(fmt.Errorf("io.Copy error: %v", err))
		return err
	}

	buf := c.scratchBuf.Bytes()

	// First, we have the protocol version.
	if buf[0] != 0xa {
		return fmt.Errorf("Unexpected protocol version: %x", buf[0])
	}

	// Next, we have the server version as a NULL-terminated string. We simply
	// skip this section.
	afterVers := bytes.IndexByte(buf[1:], 0x0) + 1
	buf = buf[afterVers:]

	// Next, we have the connection id as a uint32. We skip this section.
	buf = buf[4:]

	var (
		passwdChallenge [20]byte
		passwdLen       = 0
		tempbuf         [20]byte
	)

	// Next, we have the first 8 bytes of the password challenge data.
	copy(passwdChallenge[:8], buf[:8])
	buf = buf[8:]
	passwdLen += 8

	// Next, we have a one-byte pad
	buf = buf[1:]

	// Next, we have the 2 byte capability flag in little-endian format.
	serverFlag := connectionFlag(uint16(buf[0]) | uint16(buf[1])<<8)
	if serverFlag&flagProtocol41 != flagProtocol41 {
		return errors.New("Server does not support 4.1 wire protocol")
	}
	c.serverFlags = serverFlag

	if len(buf) > 0 {
		// Read the character set, so we can echo it later
		c.charset = buf[0]
		buf = buf[1:]

		// Ignore the server status.
		buf = buf[2:]

		// TODO(sanjay): Disabled this for compatibility, revisit this issue?
		// Read the other 2-byte capability flag
		// c.serverFlags |= connectionFlag(uint16(buf[0])|uint16(buf[1])<<8) << 16
		buf = buf[2:]

		// Skip 1 byte that shows the length of auth-plugin-data. We do not
		// support this feature.
		buf = buf[1:]

		// Skip 10 reserved bytes
		buf = buf[10:]

		// TODO(sanjay): before we assume the password is here, should we be
		// checking (serverFlags & flagSecureConnection)?

		// Read 12 bytes of password challenge
		// Next, we have the first 8 bytes of the password challenge data.
		copy(passwdChallenge[8:], buf[:12])
		buf = buf[12:]
		passwdLen += 12
	}

	// NOTE(sanjay): scratchBuf is an in-memory buffer, so we don't check write
	// errors in this next section.

	// Now that we've read the server's half of the handshake, let's write our
	// half to the scratchBuf.
	c.scratchBuf.Reset()

	// These are the capabilities this prototype supports
	clientFlags := flagProtocol41 |
		flagSecureConn |
		flagLongPassword |
		flagTransactions

	binary.Write(c.scratchBuf, binary.LittleEndian, uint32(clientFlags))
	binary.Write(c.scratchBuf, binary.LittleEndian, uint32(0))
	binary.Write(c.scratchBuf, binary.LittleEndian, uint8(c.charset))
	c.scratchBuf.Write(zero[0:23])
	fmt.Fprintf(c.scratchBuf, "%s\x00", username)

	if len(password) > 0 {
		// Do some password magic here
		hash := sha1.New()
		hash.Write([]byte(password))
		stage1 := hash.Sum(nil)

		hash.Reset()
		hash.Write(tempbuf[:])
		hash.Sum(tempbuf[:])

		hash.Reset()
		hash.Write(passwdChallenge[:passwdLen])
		hash.Write(tempbuf[:])
		hash.Sum(tempbuf[:])

		for i := range tempbuf {
			tempbuf[i] ^= stage1[i]
		}

		fmt.Fprintf(c.scratchBuf, "%c", len(tempbuf))
		c.scratchBuf.Write(tempbuf[:])
	} else {
		fmt.Fprintf(c.scratchBuf, "%c", 0)
	}

	if len(db) > 0 {
		fmt.Fprintf(c.scratchBuf, "%s\x00", db)
	}

	c.BeginPacketWrite(int64(c.scratchBuf.Len()))

	_, err = c.Write(c.scratchBuf.Bytes())
	c.scratchBuf.Reset()
	if err != nil {
		return err
	}

	err = c.bw.Flush()
	if err != nil {
		return err
	}

	err = c.AdvancePacket()
	if err != nil {
		return err
	}

	_, err = io.ReadAtLeast(c, tempbuf[0:1], 1)
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	} else if err != nil {
		return err
	}

	if tempbuf[0] != 0 {
		// TODO(sanjay): explain this and retrieve the rest of the info
		// from the connection
		return errors.New("auth failed")
	}
	return nil
}

func (c *conn) Begin() (drv.Tx, error) {
	panic("unimplemented")
}

func (c *conn) Close() error {
	panic("unimplemented")
}

func (c *conn) Prepare(string) (drv.Stmt, error) {
	panic("unimplemented")
}

var (
	_ drv.Conn = (*conn)(nil)
	// _ drv.Execer  = (*conn)(nil) TODO(sanjay): implement this
	// _ drv.Queryer = (*conn)(nil) TODO(sanjay): implement this
)
