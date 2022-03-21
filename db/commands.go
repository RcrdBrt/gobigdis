package db

import "github.com/RcrdBrt/gobigdis/storage"

func (d *database) Select(dbNum int) error {
	d.RLock()
	defer d.RUnlock()

	if err := storage.NewDB(dbNum); err != nil {
		return err
	}

	d.dbNum = dbNum

	return nil
}

func (d *database) Get(args [][]byte) ([]byte, error) {
	d.RLock()
	defer d.RUnlock()

	content, err := storage.Get(d.dbNum, args)
	if err != nil {
		return nil, err
	}

	return content, nil
}
