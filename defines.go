package gms

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
