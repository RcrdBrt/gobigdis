package wal

import (
	"fmt"
	"hash/crc32"
	"io/fs"
	"log"
	"path/filepath"
	"sort"
	"strings"

	"github.com/RcrdBrt/gobigdis/config"
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

func listLogFiles() ([]filenameInfo, error) {
	var parsedLogFileNames []filenameInfo
	if err := filepath.WalkDir(config.Config.DBConfig.InternalDirPath, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() || !strings.HasPrefix(d.Name(), "wal-") || !strings.HasSuffix(d.Name(), ".log") {
			return nil
		}

		pn, err := parseFilename(d.Name())
		if err != nil {
			return err
		}

		parsedLogFileNames = append(parsedLogFileNames, pn)

		return nil
	}); err != nil {
		log.Fatal(err)
	}

	sort.Slice(parsedLogFileNames, func(i, j int) bool {
		return parsedLogFileNames[i].seqNo < parsedLogFileNames[j].seqNo
	})

	return parsedLogFileNames, nil
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
