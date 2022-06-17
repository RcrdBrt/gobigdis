package db

import "github.com/RcrdBrt/gobigdis/storage"

func (d *database) Select(dbNum int) error {
	d.Lock()
	defer d.Unlock()

	if err := storage.NewDB(dbNum); err != nil {
		return err
	}

	d.dbNum = dbNum

	return nil
}
