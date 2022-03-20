package ops

type RedisOp int

const (
	Ping RedisOp = iota
	Select
	Get
	Set
	Del
	FlushDB
	FlushAll
)
