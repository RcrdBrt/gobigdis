package db

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/RcrdBrt/gobigdis/config"
	"github.com/RcrdBrt/gobigdis/sst"
	"github.com/RcrdBrt/gobigdis/utils"
)

const descriptorFilePrefix = "MANIFEST-"

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Descriptor describes all important DB state (MANIFEST-* files in the internal dir).
// Not thread-safe, access should be externally synchronized.
type descriptor struct {
	SstMetas []sst.SstMeta

	H hash.Hash32

	Version int64
}

func loadLatestDescriptor() *descriptor {
	var descriptors []string
	// scan internal dir for all MANIFEST-* files
	if err := filepath.WalkDir(config.Config.DBConfig.InternalDirPath, func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() || !strings.HasPrefix(d.Name(), descriptorFilePrefix) {
			return nil
		}

		descriptors = append(descriptors, filepath.Base(path))

		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// find the latest MANIFEST-* file
	sort.Strings(descriptors)

	if len(descriptors) == 0 {
		// new db?
		return &descriptor{
			Version:  0,
			SstMetas: nil,
			H:        nil,
		}
	}

	latestFilePath := filepath.Join(config.Config.DBConfig.InternalDirPath, descriptors[len(descriptors)-1])

	f, err := os.Open(latestFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	var scratch [8]byte
	if _, err := io.ReadFull(f, scratch[:]); err != nil {
		log.Fatal(err)
	}
	dataLen := binary.LittleEndian.Uint32(scratch[:4])
	crc := binary.LittleEndian.Uint32(scratch[4:])

	content := make([]byte, dataLen)
	if _, err := io.ReadFull(f, content); err != nil {
		log.Fatal(err)
	}

	// verify crc
	if crc != crc32.Checksum(content, crcTable) {
		log.Fatal("crc mismatch")
	}

	var latestDescriptor descriptor
	if err := gob.NewDecoder(bytes.NewReader(content)).Decode(&latestDescriptor); err != nil {
		log.Fatal(err)
	}

	return &latestDescriptor
}

func (d *descriptor) Save() error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(d); err != nil {
		return err
	}

	crc := crc32.Checksum(buf.Bytes(), crcTable)

	var scratch [8]byte
	binary.LittleEndian.PutUint32(scratch[:4], uint32(len(buf.Bytes())))
	binary.LittleEndian.PutUint32(scratch[4:], crc)

	filename := fmt.Sprintf("%s%d", descriptorFilePrefix, d.Version)
	d.Version++
	nextFilename := fmt.Sprintf("%s%d", descriptorFilePrefix, d.Version)

	manifestFile, err := os.OpenFile(filepath.Join(config.Config.DBConfig.InternalDirPath, nextFilename), os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(manifestFile)

	if _, err := w.Write(scratch[:]); err != nil {
		manifestFile.Close()
		return err
	}

	if _, err := w.Write(buf.Bytes()); err != nil {
		manifestFile.Close()
		return err
	}

	if err := w.Flush(); err != nil {
		manifestFile.Close()
		return err
	}

	if err := manifestFile.Sync(); err != nil {
		manifestFile.Close()
		return err
	}

	if err := manifestFile.Close(); err != nil {
		return err
	}

	utils.Debugf("Descriptor saved to %s", nextFilename)

	// delete the previous MANIFEST-* file
	if d.Version > 1 {
		if err := os.Remove(filepath.Join(config.Config.DBConfig.InternalDirPath, filename)); err != nil {
			utils.Debugf("Failed to delete %s: %s", filename, err)
		}
	}

	return nil
}
