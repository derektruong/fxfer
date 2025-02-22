package storage

import (
	"context"
	"io"

	"github.com/derektruong/fxfer/internal/xferfile"
	"github.com/derektruong/fxfer/protoc"
)

type Source interface {
	// GetFileInfo retrieves the size of the file at the given path
	//
	// Parameters:
	//  - ctx: the context of the request
	//  - filePath: the path of the file
	//  - client: the client used to fetch the file
	//
	// Returns:
	//  - size: the size of the file
	//  - err: the error if any occurred, nil otherwise
	GetFileInfo(ctx context.Context, filePath string, client protoc.Client) (info xferfile.Info, err error)

	// GetFileFromOffset fetches a file from the storage via
	// the protocol client. This is useful when the file is stored
	// in a remote server like FTP, SFTP, S3, etc.
	//
	// Parameters:
	//  - ctx: the context of the request
	//  - filePath: the path of the file you want to fetch
	//  - offset: the offset of the file you want to fetch
	//  - client: the client used to fetch the file
	//
	// Returns:
	//  - reader: the reader that contains the content of the file
	//  - err: the error if any occurred, nil otherwise
	GetFileFromOffset(ctx context.Context, filePath string, offset int64, client protoc.Client) (reader io.ReadCloser, err error)

	// Close closes the source
	Close()
}
