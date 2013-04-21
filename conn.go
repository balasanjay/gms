package gms

import (
	"bufio"
	drv "database/sql/driver"
	"errors"
	"io"
	"io/ioutil"
)

type conn struct {
	// Original connection.
	rwc io.ReadWriteCloser

	// Buffered writer, wrapping rwc.
	bw *bufio.Writer

	// Buffered reader, wrapping rwc.
	br *bufio.Reader

	// Limited reader, for reading packets.
	lr io.LimitedReader

	// If true, then the next packet should be transparently merged into this
	// one.
	mergeNextPacket bool

	// The maximum size of a packet received from the server.
	maxRecvPacketSize uint32
}

func newConn(rwc io.ReadWriteCloser) *conn {
	// TODO(sanjay): tune these
	const (
		defaultWriteBufSize = 4096
		defaultReadBufSize  = 4096
	)

	c := &conn{}

	c.rwc = rwc

	c.bw = bufio.NewWriterSize(c.rwc, defaultWriteBufSize)
	c.br = bufio.NewReaderSize(c.rwc, defaultReadBufSize)

	c.lr.N = 0
	c.lr.R = c.br

	c.mergeNextPacket = false

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

// readPacket header reads the next 4 bytes from the connection, and configures
// the readers in the connection correctly. Note that it should only be called
// at packet boundaries, or at the beginning of a connection.
func (c *conn) readPacketHeader() error {
	var (
		buf       [4]byte
		err       error
		packetLen uint32
		nextSeq   uint32
	)

	_, err = io.ReadAtLeast(c.br, buf[:], len(buf))
	if err == nil {
		// Read packet length
		packetLen = uint32(buf[0]) |
			uint32(buf[1])<<8 |
			uint32(buf[2])<<16

		// Read sequence number
		nextSeq = uint32(buf[3])
	}

	if err == io.EOF {
		return io.ErrUnexpectedEOF
	} else if err != nil {
		return err
	} else if packetLen == 0 {
		return errors.New("unexpected 0-length packet")
	}

	// TODO(sanjay): handle sequence number
	_ = nextSeq

	c.lr.N = int64(packetLen)
	c.mergeNextPacket = (packetLen >= c.maxRecvPacketSize)
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
