package local

import (
	"errors"

	"github.com/derektruong/fxfer/protoc"
	"github.com/go-logr/logr"
)

// IO represents the local storage config.
type IO struct{}

// NewIO creates a new S3 config.
func NewIO() (c *IO) {
	return &IO{}
}

func (io IO) GetConnectionPool(logger logr.Logger) protoc.ConnectionPool {
	panic(errors.ErrUnsupported)
}

func (io IO) GetS3API() protoc.S3API {
	panic(errors.ErrUnsupported)
}

func (io IO) GetCredential() any {
	return io
}

func (io IO) GetConnectionID() string {
	return ""
}
