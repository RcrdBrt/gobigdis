# GoBigdis

GoBigdis is a persistent database that implements the Redis server protocol. Any Redis client can interface with it and start to use it right away. It's basically a *Redis-for-secondary-memory*.

The main feature of GoBigdis is that it's very friendly with huge keys and huge values. Much friendlier than Redis itself, as the Redis author states (see the credits section).

It has no external dependencies of any kind. It gets away by simply using the comprehensive Go's standard library. Also, since it uses 1 goroutine per client connection it gets to scale to multiple cores "for free".


## Status
GoBigdis is my weekend attempt at [Bigdis](https://github.com/antirez/Bigdis) (see the credits section for further infos).

This is the subset of commands it currently implements:

|Command |Status
--- | --- 
|`PING`|Fully implemented :heavy_check_mark:|
|`GET`|Fully implemented :heavy_check_mark:|
|`SET`|Only setting values works, no keys expiration logic as of now :wrench:|
|`DEL`|Fully implemented :heavy_check_mark:|
|`COMMAND`|Placeholder reply only :wrench:|
|`SELECT`|Fully implemented :heavy_check_mark:|
|`FLUSHDB`|Does what expected, but only without arguments :wrench:|

Nothing other than the basic KV type has been implemented as of now.

## Command parameters
GoBigdis, with its `gobigdis` command, currently accepts the following command flags:
- `-h STRING` specifies on what IP the server TCP socket should listen on (defaults to `localhost` if not set)
- `-p INTEGER` tells on what port (defaults to `6389` if not set)
- `-d PATH` sets the root database directory to use, it proceeds to create it if it doesn't already exist (defaults to `$HOME/.gobigdis` if not set)

## Installation
You need `go` installed on your system. If you do, simply run:
```
go install github.com/RcrdBrt/gobigdis@latest
```

Make sure you have put the go binaries folder in your PATH and you're good to *go* with the command `gobigdis`.
Launch `gobigdis -h` for a basic command overview.

## Benchmarks
I used the benchmark suite `redis-benchmark`. It ships with the default `redis 6.2.4` [package](https://archlinux.org/packages/community/x86_64/redis/) of my Linux distribution. The Redis stock server is used with default configuration provided by said package.

I used the same host for both server and (benchmark) client.

My machine specs:
- Ryzen 7 2700x
- 32GB DDR4 3200Mhz RAM (2x16GB)
- NVMe Samsung 970evo 512GB

### First configuration (2 kB value size)
100,000 requests; 50 connections; 2,000 bytes size of values; TCP KeepAlive ON
```
redis-benchmark -t get,set -n 100000 -c 50 -d 2000 -r 2000 -k 1 -q -p PORT
```
Results:
|database|`GET` (req/s)|`GET` delay (p50 msec)|`SET` (req/s)|`SET` delay (p50 msec)|
---|---|---|---|---
|GoBigdis|93545.37|0.279|13681.76|3.199|
|stock Redis|124533.01|0.207|115340.26|0.215|

As expected, GoBigdis shows a hit in terms of performance for the `SET` command since it has to write the file on the NVMe every time the command is issued and it adopts the Copy-On-Write pattern: there is an order of magnitude of difference both from the requests-per-second and the delay perspective. Still, not that bad though and it performs surprisingly well for the `GET`s.

### Second configuration (20 kB value size)
100,000 requests; 50 connections; 20,000 bytes size of values; TCP KeepAlive ON
```
redis-benchmark -t get,set -n 100000 -c 50 -d 20000 -r 2000 -k 1 -q -p PORT
```
Results:
|database|`GET` (req/s)|`GET` delay (p50 msec)|`SET` (req/s)|`SET` delay (p50 msec)|
---|---|---|---|---
|GoBigdis|68399.45|0.327|5688.93|4.079|
|stock Redis|71530.76|0.423|74962.52|0.551|

With bigger values, stock Redis starts to suffer a little while GoBigdis keeps going almost at the same speed, narrowing down the difference between the 2. `SET` performance for GoBigdis halves considering the previous benchmark configuration but access delays stay almost the same.

### Third configuration (2 MB value size)
100,000 requests; 50 connections; 2,000,000 bytes size of values; TCP KeepAlive ON
```
redis-benchmark -t get,set -n 100000 -c 50 -d 2000000 -r 2000 -k 1 -q -p PORT
```
Results:
|database|`GET` (req/s)|`GET` delay (p50 msec)|`SET` (req/s)|`SET` delay (p50 msec)|
---|---|---|---|---
|GoBigdis|945.42|1.847|178.83|178.815|
|stock Redis|1071.27|31.599|1418.58|18.703|

At this point, almost unexpectedly both the databases balloon in terms of delays and performance degradation.

I thought they would have showed better latencies at least. Since 2 MB values is basically a picture for each key stored, it apparently makes them suffer quite a bit. `GET` commands now have almost the same requests-per-second performance between the two but GoBigdis has an order of magnitude better *p50* delays compared to the stock Redis server.

## Implementation details
GoBigdis uses the Filesystem-As-A-Database pattern. I respected the convention *antirez* implemented in his project and extended it by 1 depth level. It currently stores the keys as a SHA256 filename using the following format:
```
ROOT_DBDIR/DATABASE_NUMBER/FIRST_BYTE_OF_SHA/SECOND_BYTE_OF_SHA/THIRD_BYTE_OF_SHA/FULL_SHA
```
so the path of a key with SHA256 of `f2ca1bb6c7e907d06dafe4687e579fce76b37e4e93b7605022da52e6ccc26fd2` stored in the *third* database (e. g.) would become:
```
ROOT_DBDIR/2/f2/ca/1b/f2ca1bb6c7e907d06dafe4687e579fce76b37e4e93b7605022da52e6ccc26fd2
```

GoBigdis implements the Copy-On-Write pattern, so `SET` is expensive while `GET` is relatively cheap. It also has a coarse-grained RWLock for filesystem access. An expansion of this project should take into consideration a more fine-grained approach and probably use some more sophistication on top of or beside the Copy-On-Write. GoBigdis has a cache layer that makes the `GET` super-fast in case of some non-existent keys by avoiding to hit the filesystem entirely under certain circumstances.

With any experimental database project it should come a reasonable expectation of low overall stability. Although the persistence part simply uses filesystem primitives with no trickery of any sort and could be considered "working good enough", no battle-testing has been done other than the benchmarks above in this README, nevermind put it in production.

## Credits
This project is heavily inspired by the TCL lang experiment that *antirez* - the creator of Redis - did [in this repo](https://github.com/antirez/Bigdis) in July 2010. My project is an answer to the question in his README "Do you think this idea is useful?". I think it really is so I implemented it in Go.

Most parsing code of client requests and replying is taken [from here](https://github.com/r0123r/go-redis-server) to jumpstart the implementation.

## Future
The choice of Go opens up a lot of *easy concurrency* - it's an oximoron, I know, *ndr* - optimizations. Check out the `DEL` command implementation code for an example of it.