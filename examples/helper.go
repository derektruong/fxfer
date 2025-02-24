package examples

import (
	"fmt"
	"os"
	"strings"

	"github.com/derektruong/fxfer"
	"github.com/go-logr/logr"
)

func MustGetEnv(key string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		panic(fmt.Sprintf("missing env: %q", key))
	}
	return value
}

func ExtractPathsViaCliArgs(args []string, logger logr.Logger) (src, dest, srcPath, destPath string, err error) {
	if len(args) == 5 {
		src = strings.ToLower(args[1])
		dest = strings.ToLower(args[2])
		srcPath = args[3]
		destPath = args[4]
	} else {
		logger.Info(
			fmt.Sprintf(
				"invalid cli args, expected: %s <source> <destination> <source_path> <destination_path>."+
					" Where <source> and <destination> are one of: [local, ftp, sftp, s3]",
				args[0],
			),
			"args", args,
		)
		err = fmt.Errorf("invalid cli args")
	}
	return
}

func HandleProgressUpdated(logger logr.Logger) fxfer.ProgressUpdatedCallback {
	return func(progress fxfer.Progress) {
		switch progress.Status {
		case fxfer.ProgressStatusInProgress:
			speedMbps := float64(progress.Speed) / 1024 / 1024
			logger.Info("==========> Transfer in progress:",
				"percentage (%)", progress.Percentage,
				"transferred (bytes)", progress.TransferredSize,
				"total (bytes)", progress.TotalSize,
				"speed (MB/s)", fmt.Sprintf("%.2f", speedMbps),
				"duration (s)", progress.Duration.Seconds(),
			)
		case fxfer.ProgressStatusFinalizing:
			logger.Info("==========> Transfer finalizing",
				"duration (s)", progress.Duration.Seconds())
		case fxfer.ProgressStatusFinished:
			logger.Info("==========> Transfer finished",
				"duration (s)", progress.Duration.Seconds())
		case fxfer.ProgressStatusInError:
			logger.Error(
				progress.Error,
				"==========> Transfer in error",
				"duration (s)", progress.Duration.Seconds())
		}
	}
}
