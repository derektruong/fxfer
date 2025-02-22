package storage

import (
	"context"
	"io"
	"time"

	"github.com/derektruong/fxfer/internal/xferfile"
	"github.com/derektruong/fxfer/protoc"
)

type Destination interface {
	// GetFileInfo fetches a file from the storage.
	//
	// Parameters:
	//  - ctx: the context of the request
	//  - filePath: the path of the file you want to fetch
	//  - client: the client used to fetch the file
	//
	// Returns:
	//  - reader: the reader that contains the content of the file
	//  - info: the information of the file
	//  - err: the error if any occurred, nil otherwise
	GetFileInfo(ctx context.Context, filePath string, client protoc.Client) (info xferfile.Info, err error)

	// CreateFile creates a file at the specified path
	//
	// Parameters:
	//  - ctx: the context of the request
	//  - path: the path of the file you want to create
	//  - size: the size of the file in bytes
	//  - modTime: the modification time of the source file
	//  - client: the client used to create the file
	//
	// Returns:
	//  - err: the error if any occurred, nil otherwise
	CreateFile(ctx context.Context, path string, size int64, modTime time.Time, client protoc.Client) (err error)

	// TransferFileChunk writes a chunk of data to the file from the reader
	//
	// Parameters:
	//  - ctx: the context of the request
	//  - filePath: the path of the file you want to write to
	//  - reader: the reader that contains the content of the file
	//  - offset: the offset in bytes (zero-based), indicating the position of the last byte written
	//  - client: the client used to write the file
	//
	// Returns:
	//  - n: the number of bytes written
	//  - err: the error if any occurred, nil otherwise
	TransferFileChunk(ctx context.Context, filePath string, reader io.Reader, offset int64, client protoc.Client) (n int64, err error)

	// FinalizeTransfer finalizes the file at the specified path (make the transfer complete)
	//
	// Parameters:
	//  - ctx: the context of the request
	//  - filePath: the path of the file you want to finalize
	//  - client: the client used to finalize the file
	//
	// Returns:
	//  - err: the error if any occurred, nil otherwise
	FinalizeTransfer(ctx context.Context, filePath string, client protoc.Client) (err error)

	// DeleteFile deletes the file at the specified path
	//
	// Parameters:
	//  - ctx: the context of the request
	//  - filePath: the path of the file you want to delete
	//  - client: the client used to delete the file
	//
	// Returns:
	//  - err: the error if any occurred, nil otherwise
	DeleteFile(ctx context.Context, filePath string, client protoc.Client) (err error)

	// Close closes the destination
	Close()
}
