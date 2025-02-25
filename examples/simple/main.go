package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"log/slog"

	fxfer "github.com/derektruong/fxfer"
	"github.com/derektruong/fxfer/examples"
	"github.com/derektruong/fxfer/protoc"
	localio "github.com/derektruong/fxfer/protoc/local"
	s3protoc "github.com/derektruong/fxfer/protoc/s3"
	"github.com/derektruong/fxfer/storage"
	"github.com/derektruong/fxfer/storage/local"
	"github.com/derektruong/fxfer/storage/s3"
	"github.com/go-logr/logr"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := logr.FromSlogHandler(slog.NewJSONHandler(os.Stdout, nil))

	src, dest, srcPath, destPath, err := examples.ExtractPathsViaCliArgs(os.Args, logger)
	if err != nil {
		return
	}

	// s3 envs
	s3Endpoint := examples.MustGetEnv("S3_ENDPOINT")
	s3Bucket := examples.MustGetEnv("S3_BUCKET_NAME")
	s3Region := examples.MustGetEnv("S3_REGION")
	s3AccessKey := examples.MustGetEnv("S3_ACCESS_KEY")
	s3SecretKey := examples.MustGetEnv("S3_SECRET_KEY")

	var srcClient, destClient protoc.Client
	var srcStorage storage.Source
	var destStorage storage.Destination

	// setup source
	switch src {
	case "local":
		srcClient = localio.NewIO()
		srcStorage, err = local.NewSource(logger)
		if err != nil {
			logger.Error(err, "failed to setup local source storage")
			return
		}
	case "s3":
		srcClient = s3protoc.NewClient(s3Endpoint, s3Bucket, s3Region, s3AccessKey, s3SecretKey)
		srcStorage = s3.NewSource(logger)
	default:
		panic("invalid source storage")
	}
	defer srcStorage.Close()

	// setup destination
	switch dest {
	case "local":
		destClient = localio.NewIO()
		destStorage, err = local.NewDestination(logger)
		if err != nil {
			logger.Error(err, "failed to setup local source storage")
			return
		}
	case "s3":
		destClient = s3protoc.NewClient(s3Endpoint, s3Bucket, s3Region, s3AccessKey, s3SecretKey)
		destStorage = s3.NewDestination(logger)
	default:
		panic("invalid destination storage")
	}
	defer destStorage.Close()

	transfer := fxfer.NewTransfer(logger, fxfer.WithMaxFileSize(5<<40))

	if err = transfer.Transfer(
		ctx,
		fxfer.SourceConfig{
			FilePath: srcPath,
			Storage:  srcStorage,
			Client:   srcClient,
		},
		fxfer.DestinationConfig{
			FilePath: destPath,
			Storage:  destStorage,
			Client:   destClient,
		},
		examples.HandleProgressUpdated(logger),
	); err != nil {
		logger.Error(err, "failed to transfer", "src", src, "dest", dest)
		return
	}
}
