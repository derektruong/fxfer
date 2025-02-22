package storage

import "errors"

var ErrLocalProtocolIOInvalid = errors.New("protocol: local IO invalid, expected local")
var ErrFTPProtocolClientInvalid = errors.New("protocol: client invalid, expected FTP")
var ErrSFTPProtocolClientInvalid = errors.New("protocol: client invalid, expected SFTP")
var ErrS3ProtocolClientInvalid = errors.New("protocol: client invalid, expected S3")
var ErrFileOrObjectCannotFinalize = errors.New("file or object cannot finalize, please retry")
