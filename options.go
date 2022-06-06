package gobitcaskdb

type DBOptions struct {
	readWrite bool
	syncOnPut bool
}

type DBOption = func(*DBOptions)

func ReadWriteMode() DBOption {
	return func(ops *DBOptions) {
		ops.readWrite = false
	}
}

func SyncOnPut() DBOption {
	return func(ops *DBOptions) {
		ops.syncOnPut = false
	}
}
