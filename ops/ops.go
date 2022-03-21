package ops

type RedisOp int

const (
	GET RedisOp = iota
	SET
	DEL
	INCR
	FLUSHDB
	FLUSHALL
	EXISTS
	SELECT
)
