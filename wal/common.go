package wal

import (
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"sort"
	"strings"

	"github.com/RcrdBrt/gobigdis/utils"
)

const (
	// MaxRecordBytes is the largest size a single record can be.
	MaxRecordBytes uint32 = 100 * 1024 * 1024

	// MaxLogBytes is the largest size a single log file can be.
	MaxLogBytes int64 = 1 * 1024 * 1024 * 1024
)

var (
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

type filenameInfo struct {
	name  string
	seqNo int64
}

func listLogFiles(dirname string) ([]filenameInfo, error) {
	fis, err := ioutil.ReadDir(dirname)
	if err != nil {
		return nil, err
	}
	parsedNames := make([]filenameInfo, 0, len(fis))
	for _, fi := range fis {
		name := fi.Name()
		if !(strings.HasPrefix(name, "wal-") && strings.HasSuffix(name, ".log")) {
			utils.Debugf("Skipping file %v in WAL directory, does not appear to be a WAL file.", name)
			continue
		}

		pn, err := parseFilename(name)
		if err != nil {
			return nil, err
		}
		parsedNames = append(parsedNames, pn)
	}

	sort.Slice(parsedNames, func(i, j int) bool {
		return parsedNames[i].seqNo < parsedNames[j].seqNo
	})

	return parsedNames, nil
}

func parseFilename(n string) (filenameInfo, error) {
	var seqNo int64
	if _, err := fmt.Sscanf(n, "wal-%d.log", &seqNo); err != nil {
		return filenameInfo{}, err
	}

	return filenameInfo{
		name:  n,
		seqNo: seqNo,
	}, nil
}
