package protoc

import "fmt"

type Protoc string

const (
	FTPProtoc   Protoc = "FTP"
	SFTPProtoc  Protoc = "SFTP"
	S3Protoc    Protoc = "S3"
	LocalProtoc Protoc = "Local"
)

var ErrClientConfigInvalid = func(protoc Protoc) error {
	return fmt.Errorf("client: config invalid, expected %s", protoc)
}

var ErrProtocolInvalid = func(protoc Protoc) error {
	return fmt.Errorf("protocol: invalid, expected %s", protoc)
}
