package wal

import "github.com/RcrdBrt/gobigdis/utils"

type logRecord struct {
	seq       uint64
	op        utils.RedisOp
	key       string
	value     []byte
	timestamp int64
}

func (r *logRecord) reset() {
	r.seq = 0
	r.op = 0
	r.key = ""
	r.value = nil
	r.timestamp = 0
}
