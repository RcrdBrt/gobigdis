package wal

import "github.com/RcrdBrt/gobigdis/ops"

type LogRecord struct {
	DBNum     int
	Seq       int64
	Op        ops.RedisOp
	Key       string
	Timestamp int64
	Value     []byte
}

func (l *LogRecord) Reset() {
	l.DBNum = 0
	l.Seq = 0
	l.Op = ops.RedisOp(0)
	l.Key = ""
	l.Timestamp = 0
	l.Value = nil
}
