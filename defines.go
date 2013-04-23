package gms

import (
	"errors"
)

type flushPolicy bool

const (
	FLUSH    flushPolicy = true
	NO_FLUSH flushPolicy = false
)

type connectionFlag uint32

const (
	flagLongPassword connectionFlag = 1 << iota
	flagFoundRows
	flagLongFlag
	flagConnectWithDB
	flagNoSchema
	flagCompress
	flagODBC
	flagLocalFiles
	flagIgnoreSpace
	flagProtocol41
	flagInteractive
	flagSSL
	flagIgnoreSIGPIPE
	flagTransactions
	flagReserved
	flagSecureConn
	flagMultiStatements
	flagMultiResults
)

const (
	comQuit byte = iota + 1
	comInitDB
	comQuery
	comFieldList
	comCreateDB
	comDropDB
	comRefresh
	comShutdown
	comStatistics
	comProcessInfo
	comConnect
	comProcessKill
	comDebug
	comPing
	comTime
	comDelayedInsert
	comChangeUser
	comBinlogDump
	comTableDump
	comConnectOut
	comRegiserSlave
	comStmtPrepare
	comStmtExecute
	comStmtSendLongData
	comStmtClose
	comStmtReset
	comSetOption
	comStmtFetch
)

type fieldType byte

const (
	fieldTypeDecimal fieldType = iota
	fieldTypeTiny
	fieldTypeShort
	fieldTypeLong
	fieldTypeFloat
	fieldTypeDouble
	fieldTypeNULL
	fieldTypeTimestamp
	fieldTypeLongLong
	fieldTypeInt24
	fieldTypeDate
	fieldTypeTime
	fieldTypeDateTime
	fieldTypeYear
	fieldTypeNewDate
	fieldTypeVarChar
	fieldTypeBit
)
const (
	fieldTypeNewDecimal fieldType = iota + 0xf6
	fieldTypeEnum
	fieldTypeSet
	fieldTypeTinyBLOB
	fieldTypeMediumBLOB
	fieldTypeLongBLOB
	fieldTypeBLOB
	fieldTypeVarString
	fieldTypeString
	fieldTypeGeometry
)

type fieldFlag uint16

const (
	flagNotNULL fieldFlag = 1 << iota
	flagPriKey
	flagUniqueKey
	flagMultipleKey
	flagBLOB
	flagUnsigned
	flagZeroFill
	flagBinary
	flagEnum
	flagAutoIncrement
	flagTimestamp
	flagSet
	flagUnknown1
	flagUnknown2
	flagUnknown3
	flagUnknown4
)

type results struct {
	affectedRows int64
	lastInsertId int64
}

func (r results) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

func (r results) RowsAffected() (int64, error) {
	return r.affectedRows, nil
}

var (
	errUnknownLastInsertId = errors.New("Server did not send last-insert-id")
	errUnknownRowsAffected = errors.New("Server did not send number of rows affected")
)

type unknownResults int

func (u unknownResults) LastInsertId() (int64, error) {
	return 0, errUnknownLastInsertId
}

func (u unknownResults) RowsAffected() (int64, error) {
	return 0, errUnknownRowsAffected
}
