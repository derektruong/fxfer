//go:generate go run go.uber.org/mock/mockgen -destination=./mock_storage.go -package=mock_storage github.com/derektruong/fxfer/storage Source,Destination

package mock_storage
