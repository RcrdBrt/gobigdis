package storage

import "github.com/RcrdBrt/gobigdis/ops"

type logRecord struct {
	seq       uint64
	op        ops.RedisOp
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

func (r *logRecord) Apply() {
	switch r.op {
	case ops.Get:
	}
}
