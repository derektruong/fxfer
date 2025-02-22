package protoc

import "fmt"

var ErrFTPClientConfigInvalid = fmt.Errorf("client: config invalid, expected FTP")
var ErrSFTPClientConfigInvalid = fmt.Errorf("client: config invalid, expected SFTP")
