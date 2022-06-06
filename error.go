package gobitcaskdb

import "errors"

var (
	ErrLogFileNotFound     = errors.New("log file not found")
	ErrFileClosed          = errors.New("file closed")
	ErrInvalidHeaderBuffer = errors.New("invalid header buffer")
	ErrInvalidEntityBuffer = errors.New("invalid entity buffer")
)
