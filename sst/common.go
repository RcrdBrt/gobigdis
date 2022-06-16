//    Copyright 2018 Google Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package sst

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"

	"github.com/RcrdBrt/gobigdis/config"
	"github.com/RcrdBrt/gobigdis/utils"
)

const (
	SstMagic = uint64(0xe489f8a9d479536b)
	// MaxSstKeySize is the max encoded keysize in an SST.
	// Slightly larger than DB MaxKeySize due to additional data.
	MaxSstKeySize = config.MaxKeySize + 16

	footerSize = 4*binary.MaxVarintLen64 + 4 + 8
)

const (
	typeNil   = 1
	typeBytes = 2
)

const blockSize = 16 * 1024

var crcTable = crc32.MakeTable(crc32.Castagnoli)

var (
	ErrNotFound   = errors.New("not found")
	ErrCorruption = errors.New("corruption detected")
)

type SstMeta struct {
	AppliedUntil int64
	Filename     string
}

type blockHandle struct {
	offset uint64
	// size is the size of the block. Does not include checksum.
	size uint64
}

func newBlockHandle(r io.ByteReader) (blockHandle, error) {
	bh := blockHandle{}
	var err error
	if bh.offset, err = binary.ReadUvarint(r); err != nil {
		return bh, err
	}
	if bh.size, err = binary.ReadUvarint(r); err != nil {
		return bh, err
	}
	return bh, nil
}

func (h *blockHandle) EncodeTo(w io.Writer) error {
	if err := utils.WriteUvarInt64(w, h.offset); err != nil {
		return err
	}
	return utils.WriteUvarInt64(w, h.size)
}