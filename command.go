package fxfer

import (
	"context"

	"github.com/derektruong/fxfer/protoc"
	"github.com/derektruong/fxfer/storage"
	"github.com/go-playground/validator/v10"
)

// validate use a single instance of validate, it caches struct info
var validate *validator.Validate

func init() {
	validate = validator.New(validator.WithRequiredStructEnabled())
}

// SourceCommand represents the source file to transfer.
// It contains the file path, storage, and client.
type SourceCommand struct {
	// FilePath is the path of the source file
	FilePath string `json:"filePath" yaml:"filePath" validate:"required"`
	// Storage: see storage.Source
	Storage storage.Source `json:"storage" yaml:"storage" validate:"required"`
	// Client: see protoc.Client
	Client protoc.Client `json:"client" yaml:"client" validate:"required"`
}

// DestinationCommand represents the destination file to transfer.
// It contains the file path, storage, and client.
type DestinationCommand struct {
	// FilePath is the path of the destination file
	FilePath string `json:"filePath" yaml:"filePath" validate:"required"`
	// Storage: see storage.Destination
	Storage storage.Destination `json:"storage" yaml:"storage" validate:"required"`
	// Client: see protoc.Client
	Client protoc.Client `json:"client" yaml:"client" validate:"required"`
}

func (src SourceCommand) Validate(ctx context.Context) error {
	return validate.StructCtx(ctx, src)
}

func (dest DestinationCommand) Validate(ctx context.Context) error {
	return validate.StructCtx(ctx, dest)
}