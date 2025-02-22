package protoc

import (
	"context"
	"io"
	"time"
)

type ConnectionPool interface {
	// InitializeIdleConnection caches the FTP connection with the given credential
	// into the connection
	//
	// Parameters:
	//  - credential: the FTP credential that will be used to establish the connection
	//
	// Returns:
	//  - err: the error if any occurred, nil otherwise
	InitializeIdleConnection(credential any) (err error)

	// GetFileSizeAndModTime retrieves the size and mod time of the file at the given path
	//
	// Parameters:
	//  - ctx: the context
	//  - filePath: the path of the file
	//
	// Returns:
	//  - size: the size of the file
	//  - modTime: the mod time of the file
	//  - err: the error if any occurred, nil otherwise
	GetFileSizeAndModTime(ctx context.Context, filePath string) (size int64, modTime time.Time, err error)

	// RetrieveFileFromOffset retrieves the content of the file at the given path
	// starting from the given offset
	//
	// Parameters:
	//  - ctx: the context
	//  - filePath: the path of the file
	//  - offset: the offset to start reading the file
	//  - callback: the callback function that will be called with the reader
	//    that contains the content of the file
	//
	// Returns:
	//  - err: the error if any occurred, nil otherwise
	RetrieveFileFromOffset(ctx context.Context, filePath string, offset int64) (reader io.ReadCloser, err error)

	// MakeDirectoryAll creates a directory at the given path and all its parent directories
	// if they do not exist
	//
	// Parameters:
	//  - ctx: the context
	//  - connectionID: the ID of the connection used to establish the connection
	//  - dirPath: the path of the directory
	//
	// Returns:
	//  - err: the error if any occurred, nil otherwise occurred, nil otherwise
	MakeDirectoryAll(ctx context.Context, dirPath string) (err error)

	// CreateOrOverwriteFile stores the content of the reader to the file at the given path.
	//
	// Parameters:
	//  - ctx: the context
	//  - filePath: the path of the file
	//  - reader: the reader that contains the content of the file
	//
	// Returns:
	//  - err: the error if any occurred, nil otherwise
	CreateOrOverwriteFile(ctx context.Context, filePath string, reader io.Reader) (err error)

	// AppendToFile appends the content of the reader to the file at the given path.
	//
	// Parameters:
	//  - ctx: the context
	//  - filePath: the path of the file
	//  - reader: the reader that contains the content of the file
	//
	// Returns:
	//  - err: the error if any occurred, nil otherwise
	AppendToFile(ctx context.Context, filePath string, reader io.Reader, offset int64) (err error)

	// DeleteFile deletes the file at the given path
	//
	// Parameters:
	//  - ctx: the context
	//  - filePath: the path of the file
	//
	// Returns:
	//  - err: the error if any occurred, nil otherwise occurred, nil otherwise
	DeleteFile(ctx context.Context, filePath string) (err error)

	// Close closes the connection gracefully
	Close()
}
