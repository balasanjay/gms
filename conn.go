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

	// The size of the packet being written to bw. Call BeginPacket to set
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
	reuseBuf *bytes.Buffer

	// Capabilities flags for this connection
	serverFlags connectionFlag

	// The charset sent by the server during the initial handshake
	charset byte

	// The current sequence id.
	seqId uint8

	// Temporary writing area for many functions to avoid allocating.
	scratch [512]byte
}

func newConn(rwc io.ReadWriteCloser) *conn {
	// TODO(sanjay): tune these
	const (
		defaultWriteBufSize = 16384
		defaultReadBufSize  = 8192
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

	c.reuseBuf = bytes.NewBuffer(nil)

	return c
}

func (c *conn) AdvancePacket() error {
	err := c.AdvanceToEOF()
	if err != nil {
		return err
	}

	err = c.readPacketHeader()
	if err != nil {
		return err
	}

	return nil
}

func (c *conn) AdvanceToEOF() error {
	for {
		_, err := c.Read(c.scratch[:])
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
	}
	panic("unreachable")
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

// BeginPacket sets up the conn to write a packet with size bytes.
func (c *conn) BeginPacket(size int64) {
	if c.curPacketSizeRemaining != 0 || size == 0 || c.writeCap != 0 {
		panic(fmt.Sprintf("internal error, miscalculated packet size, still %v bytes on previous packet", c.curPacketSizeRemaining))
	}

	c.curPacketSizeRemaining = size
}

func (c *conn) EndPacket(flush flushPolicy) error {
	if c.curPacketSizeRemaining != 0 || c.writeCap != 0 {
		panic(fmt.Sprintf("internal error, miscalculated packet size, still %v bytes on previous packet", c.curPacketSizeRemaining))
	}

	if !flush {
		return nil
	}
	return c.bw.Flush()
}

func (c *conn) Write(b []byte) (int, error) {
	if int64(len(b)) > c.curPacketSizeRemaining {
		panic("internal error, write larger than calculated packet size")
	}

	var buf [4]byte
	written := 0

	for len(b) > 0 {
		if c.writeCap == 0 {
			newWriteCap := int64(c.maxSendPacketSize)
			if c.curPacketSizeRemaining < newWriteCap {
				newWriteCap = c.curPacketSizeRemaining
			}

			buf[0] = byte(newWriteCap)
			buf[1] = byte(newWriteCap >> 8)
			buf[2] = byte(newWriteCap >> 16)
			buf[3] = c.seqId
			c.seqId++

			n, err := c.bw.Write(buf[:4])
			// fmt.Printf("Sent header with size=%v, seq=%v\n", newWriteCap, c.seqId-1)

			if err != nil {
				return written, err
			} else if n != 4 {
				err = io.ErrShortWrite
				return written, err
			}

			c.writeCap = newWriteCap
			continue
		}

		nextSendSize := c.writeCap
		if bsize := int64(len(b)); bsize < nextSendSize {
			nextSendSize = bsize
		}

		n, err := c.bw.Write(b[:nextSendSize])
		// fmt.Printf("Sent bytes=[%x]\n", b[:nextSendSize])
		b = b[n:]
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
		err       error
		packetLen uint32
		nextSeq   uint8
		buf       [4]byte
	)

	err = readExactly(c.br, buf[:4])
	if err != nil {
		return err
	}

	// Read packet length
	packetLen = uint32(buf[0]) |
		uint32(buf[1])<<8 |
		uint32(buf[2])<<16

	// Read sequence number
	nextSeq = buf[3]

	// fmt.Printf("Read packet with sequence number %v\n", nextSeq)

	if packetLen == 0 {
		// BUG(sanjay): this is actually OK if we are merging two packets...
		return errors.New("unexpected 0-length packet")
	} else if nextSeq != c.seqId {
		return fmt.Errorf("Expecting sequence id %v, got %v.", c.seqId, nextSeq)
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

	c.reuseBuf.Reset()

	_, err = io.Copy(c.reuseBuf, c)
	if err != nil {
		panic(fmt.Errorf("io.Copy error: %v", err))
		return err
	}

	buf := c.reuseBuf.Bytes()

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

	// NOTE(sanjay): reuseBuf is an in-memory buffer, so we don't check write
	// errors in this next section.

	// Now that we've read the server's half of the handshake, let's write our
	// half to the reuseBuf.
	c.reuseBuf.Reset()

	// These are the capabilities this prototype supports
	clientFlags := flagProtocol41 |
		flagSecureConn |
		flagLongPassword |
		flagTransactions

	if len(db) > 0 {
		clientFlags |= flagConnectWithDB
	}

	binary.Write(c.reuseBuf, binary.LittleEndian, uint32(clientFlags))
	binary.Write(c.reuseBuf, binary.LittleEndian, uint32(0))
	binary.Write(c.reuseBuf, binary.LittleEndian, uint8(c.charset))
	c.reuseBuf.Write(zero[0:23])
	fmt.Fprintf(c.reuseBuf, "%s\x00", username)

	if len(password) > 0 {
		// Do some password magic here
		hash := sha1.New()
		hash.Write([]byte(password))
		hash.Sum(c.scratch[:20])

		// c.scratch[0:20] == SHA1(password)

		hash.Reset()
		hash.Write(c.scratch[:20])
		hash.Sum(c.scratch[20:40])

		// c.scratch[0:20] == SHA1(password)
		// c.scratch[20:40] == SHA1(SHA1(password))

		hash.Reset()
		hash.Write(passwdChallenge[:passwdLen])
		hash.Write(c.scratch[20:40])
		hash.Sum(c.scratch[20:40])

		// c.scratch[0:20] = SHA1(password)
		// c.scratch[20:40] = SHA1(challenge + SHA1(SHA1(password)))

		for i := 0; i < 20; i++ {
			c.scratch[i+20] ^= c.scratch[i]
		}

		fmt.Fprintf(c.reuseBuf, "%c", 20)
		c.reuseBuf.Write(c.scratch[20:40])
	} else {
		fmt.Fprintf(c.reuseBuf, "%c", 0)
	}

	if len(db) > 0 {
		fmt.Fprintf(c.reuseBuf, "%s\x00", db)
	}

	c.BeginPacket(int64(c.reuseBuf.Len()))

	_, err = c.Write(c.reuseBuf.Bytes())
	c.reuseBuf.Reset()
	if err != nil {
		return err
	}

	err = c.EndPacket(FLUSH)
	if err != nil {
		return err
	}

	err = c.AdvancePacket()
	if err != nil {
		return err
	}

	err = readExactly(c, c.scratch[:1])
	if err != nil {
		return err
	}

	if c.scratch[0] != 0 {
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
	err := c.rwc.Close()
	if err != nil {
		return err
	}
	return nil
}

func (c *conn) Prepare(sqlStr string) (drv.Stmt, error) {
	c.seqId = 0

	c.BeginPacket(1 + int64(len(sqlStr)))

	c.scratch[0] = comStmtPrepare
	_, err := c.Write(c.scratch[:1])
	if err != nil {
		return nil, err
	}

	_, err = io.WriteString(c, sqlStr)
	if err != nil {
		return nil, err
	}

	err = c.EndPacket(FLUSH)
	if err != nil {
		return nil, err
	}

	err = c.AdvancePacket()
	if err != nil {
		return nil, err
	}

	err = readExactly(c, c.scratch[:1])
	if err != nil {
		return nil, err
	}

	if c.scratch[0] != 0 {

		// TODO(sanjay): explain this and retrieve the rest of the info
		// from the connection
		return nil, errors.New("prepare failed")
	}

	s := &stmt{c: c}

	err = readExactly(c, c.scratch[1:12])
	if err != nil {
		return nil, err
	}

	s.id = binary.LittleEndian.Uint32(c.scratch[1:5])
	numColumns := binary.LittleEndian.Uint16(c.scratch[5:7])
	numParams := binary.LittleEndian.Uint16(c.scratch[7:9])
	// c.scratch[9] is reserved, skip it
	// c.scratch[10:12] is the warning count, skip it

	s.inputFields = make([]inputFieldData, numParams)
	s.outputFields = make([]outputFieldData, numColumns)

	for i := uint16(0); i < numParams; i++ {
		err = c.ReadFieldDefinition(&s.inputFields[i].field)
		if err != nil {
			return nil, err
		}
	}

	if numParams > 0 {
		err = c.ReadEOFPacket()
		if err != nil {
			return nil, err
		}
	}

	for i := uint16(0); i < numColumns; i++ {
		err = c.ReadFieldDefinition(&s.outputFields[i].field)
		if err != nil {
			return nil, err
		}
	}

	if numColumns > 0 {
		err = c.ReadEOFPacket()
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

// Read the data form an error packet and make a Go error value.
// This function assumes that you've already read the first packet of the
// error packet.
func (c *conn) ErrorFromErrPacket() error {
	var ret serverError

	// Read the error code
	err := readExactly(c, c.scratch[:2])
	if err != nil {
		return err
	}
	ret.errorCode = binary.LittleEndian.Uint16(c.scratch[:2])

	// Skip the '#' character
	err = readExactly(c, c.scratch[:1])
	if err != nil {
		return err
	}

	// Store the SQL state
	err = readExactly(c, ret.sqlState[:])
	if err != nil {
		return err
	}

	// Read the human readable message.
	c.reuseBuf.Reset()
	c.reuseBuf.Grow(int(c.lr.N))
	_, err = io.Copy(c.reuseBuf, c)
	if err != nil {
		return err
	}
	ret.errorMsg = c.reuseBuf.String()
	return &ret
}

type serverError struct {
	errorCode uint16
	sqlState  [5]byte
	errorMsg  string
}

func (s *serverError) Error() string {
	return fmt.Sprintf("MySQL Server Error. Error Code = %d, Sql State = #%s, Message = %q", s.errorCode, s.sqlState, s.errorMsg)
}

func (c *conn) ReadFieldDefinition(f *field) error {
	err := c.AdvancePacket()
	if err != nil {
		return err
	}

	// Catalog name
	err = c.SkipLengthEncodedString()
	if err != nil {
		return err
	}

	// Schema name
	err = c.SkipLengthEncodedString()
	if err != nil {
		return err
	}

	// 	Table name
	tableNameLen, err := c.ReadLengthEncodedInt(c)
	if err != nil {
		return err
	}

	c.reuseBuf.Reset()

	c.reuseBuf.Grow(int(tableNameLen))
	_, err = io.CopyN(c.reuseBuf, c, int64(tableNameLen))
	if err != nil {
		return err
	}

	// Physical table name
	err = c.SkipLengthEncodedString()
	if err != nil {
		return err
	}

	// Column name
	colNameLen, err := c.ReadLengthEncodedInt(c)
	if err != nil {
		return err
	}

	if c.reuseBuf.Len() > 0 {
		_ = c.reuseBuf.WriteByte('.') // in-memory buffer
	}

	c.reuseBuf.Grow(int(colNameLen))
	_, err = io.CopyN(c.reuseBuf, c, int64(colNameLen))
	if err != nil {
		return err
	}

	f.name = c.reuseBuf.String()

	// Physical column name
	err = c.SkipLengthEncodedString()
	if err != nil {
		return err
	}

	err = readExactly(c, c.scratch[:11])
	if err != nil {
		return err
	}

	f.ftype = fieldType(c.scratch[7])
	f.flag = fieldFlag(binary.LittleEndian.Uint16(c.scratch[8:10]))

	return nil
}

func (c *conn) ReadEOFPacket() error {
	err := c.AdvancePacket()
	if err != nil {
		return err
	}

	err = readExactly(c, c.scratch[:1])
	if err != nil {
		return err
	}

	if c.scratch[0] == 0xfe && c.lr.N <= 4 {
		return nil
	}

	return errors.New("Did not find EOF packet, where expected")
}

func (c *conn) SkipPacketsUntilEOFPacket() error {
	for {
		err := c.AdvancePacket()
		if err != nil {
			return err
		}

		err = readExactly(c, c.scratch[:1])
		if err != nil {
			return err
		}

		if c.scratch[0] == 0xfe && c.lr.N <= 4 {
			break
		}
	}

	err := c.AdvanceToEOF()
	if err != nil {
		return err
	}

	return nil
}

var (
	_ drv.Conn = (*conn)(nil)
	// _ drv.Execer  = (*conn)(nil) TODO(sanjay): implement this
	// _ drv.Queryer = (*conn)(nil) TODO(sanjay): implement this
)
