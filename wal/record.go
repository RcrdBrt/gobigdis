package wal

import "github.com/RcrdBrt/gobigdis/ops"

type LogRecord struct {
	Seq       int64
	Op        ops.RedisOp
	key       string
	timestamp int64
	value     []byte
}

func (l *LogRecord) Reset() {
	l.Seq = 0
	l.Op = ops.RedisOp(0)
	l.key = ""
	l.timestamp = 0
	l.value = nil
}
