package storage

import (
	"os"
	"path/filepath"

	"github.com/RcrdBrt/gobigdis/utils"
)

func Set(dbNum int, key string, value []byte) error {
	if err := utils.ValidateKey(key); err != nil {
		return err
	}

	path := utils.PathFromKey(dbNum, key)

	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}

	if err := os.WriteFile(path, value, 0600); err != nil {
		return err
	}

	return nil
}

func Del(dbNum int, keys [][]byte) (int, error) {
	var deleted int
	// best-effort deletion, doesn't revert in case of mid-iteration errors
	for _, v := range keys {
		path := utils.PathFromKey(dbNum, string(v))

		if err := os.Remove(path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return deleted, err
		}

		deleted++
	}

	return deleted, nil
}

func Get(dbNum int, key string) ([]byte, error) {
	path := utils.PathFromKey(dbNum, key)

	value, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	return value, nil
}
