package protoc

import (
	"github.com/go-logr/logr"
)

// Client represents the client used to connect to the storage.
type Client interface {
	// GetConnectionPool returns the connection pool representing the protocol.
	//
	// Parameters:
	//   - logger: the logger used to log messages
	//
	// Returns:
	//   - ConnectionPool: the connection pool
	//
	// Notice: only FTP and SFTP protocols are supported
	GetConnectionPool(logger logr.Logger) ConnectionPool

	// GetS3API returns the S3 API.
	//
	// Returns:
	//   - S3API: the S3 API
	//
	// Notice: only S3 protocol is supported
	GetS3API() S3API

	// GetConnectionID returns the connection ID.
	//
	// Returns:
	//   - string: the connection ID
	GetConnectionID() string

	// GetCredential returns the credential used to connect to the storage.
	//
	// Returns:
	//   - any: the credential, it must be asserted to the correct type
	GetCredential() any
}
