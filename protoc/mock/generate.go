//go:generate go run go.uber.org/mock/mockgen -destination=./mock_protoc.go -package=mock_protoc github.com/derektruong/fxfer/protoc ConnectionPool,S3API,Client

package mock_protoc
