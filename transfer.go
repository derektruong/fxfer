package fxfer

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/derektruong/fxfer/internal/xferfile"
	"github.com/derektruong/fxfer/storage"
	"github.com/go-logr/logr"
)

var errRetryable = errors.New("retryable error")

// Transfer is the interface for handling file transfers.
type Transfer interface {
	// Transfer handles the transfer of a file from a source to a destination,
	// ensuring validation, sanitization, and resumption of incomplete transfers.
	// It supports progress updates via a callback function.
	//
	// Parameters:
	//   - ctx: the context for managing the transfer lifecycle.
	//   - src: see SourceConfig for more details.
	//   - dest: see DestinationConfig for more details.
	//   - cb: the callback function to handle progress updates (see ProgressUpdatedCallback).
	//
	// Returns:
	//   - err: if any step in the transfer process fails, nil otherwise
	Transfer(ctx context.Context, src SourceConfig, dest DestinationConfig, cb ProgressUpdatedCallback) (err error)
}

// transfer handles file transfers with configurations
type transfer struct {
	logger logr.Logger

	// options
	fileRule                *fileRule
	refreshProgressInterval time.Duration
	checksumAlgorithm       ChecksumAlgorithm
	disabledRetry           bool
	retryConfig             RetryConfig
}

// NewTransfer creates a new transfer with the optional TransferOption(s).
func NewTransfer(
	logger logr.Logger,
	options ...TransferOption,
) (t Transfer) {
	tr := &transfer{
		logger:                  logger.WithName("transfer"),
		fileRule:                new(fileRule),
		refreshProgressInterval: defaultRefreshInterval,
		checksumAlgorithm:       NoneChecksumAlgorithm,
		retryConfig: RetryConfig{
			MaxRetryAttempts: defaultMaxRetryAttempts,
			InitialDelay:     defaultInitialDelay,
			MaxDelay:         defaultMaxDelay,
		},
	}
	for _, opt := range options {
		opt(tr)
	}
	return tr
}

func (t *transfer) Transfer(
	ctx context.Context,
	src SourceConfig,
	dest DestinationConfig,
	cb ProgressUpdatedCallback,
) (err error) {
	if err = src.Validate(ctx); err != nil {
		return
	}
	if err = dest.Validate(ctx); err != nil {
		return
	}

	var srcInfo xferfile.Info
	if srcInfo, err = src.Storage.GetFileInfo(
		ctx,
		src.FilePath,
		src.Client,
	); err != nil {
		return
	}

	if err = t.fileRule.Check(srcInfo); err != nil {
		return
	}

	if t.disabledRetry {
		return t.processResumableTransfer(ctx, srcInfo, src, dest, cb)
	}

	if err = retry.Do(
		func() error {
			return t.processResumableTransfer(ctx, srcInfo, src, dest, cb)
		},
		retry.Context(ctx),
		retry.Delay(t.retryConfig.InitialDelay),
		retry.MaxDelay(t.retryConfig.MaxDelay),
		retry.Attempts(uint(t.retryConfig.MaxRetryAttempts)),
		retry.RetryIf(func(err error) bool {
			return errors.Is(err, errRetryable)
		}),
		retry.OnRetry(func(n uint, err error) {
			t.logger.Info("retrying file transfer",
				"srcPath", src.FilePath, "dstPath", dest.FilePath,
				"errorMessage", err.Error(),
				"retryAttempts", n+1)
		}),
	); err != nil {
		err = errors.Unwrap(err)
		if err == nil {
			return
		}
		t.logger.Info("failed to transfer file",
			"srcPath", src.FilePath, "dstPath", dest.FilePath,
			"errorMessage", err.Error(),
		)
	}

	return
}

func (t *transfer) processResumableTransfer(
	ctx context.Context,
	srcInfo xferfile.Info,
	src SourceConfig,
	dest DestinationConfig,
	cb ProgressUpdatedCallback,
) (err error) {
	var destInfo xferfile.Info
	if destInfo, err = t.getOrCreateDestinationFile(ctx, dest, srcInfo); err != nil {
		return
	}

	if destInfo.Offset == srcInfo.Size && !destInfo.FinishTime.IsZero() {
		t.logger.Info("file transfer is finished, please re-check the destination file",
			"srcPath", src.FilePath, "dstPath", dest.FilePath)
		return
	}

	// verify if the source file has been modified
	if destInfo, err = t.verifyFileChanges(ctx, dest, srcInfo, destInfo); err != nil {
		return
	}

	// if file transfer is not finished, get the file from the offset
	var reader io.ReadCloser
	if reader, err = src.Storage.GetFileFromOffset(
		ctx,
		src.FilePath,
		destInfo.Offset,
		src.Client,
	); err != nil {
		return
	}
	defer reader.Close()

	// write chunk to destination
	interruptedChan := make(chan struct{})
	completedChan := make(chan struct{})
	proxy := newProxyReader(reader, destInfo.Offset)
	defer proxy.Close()

	go proxy.trackProgress(
		ctx,
		destInfo.StartTime, srcInfo.Size, t.refreshProgressInterval,
		interruptedChan, completedChan, cb,
	)

	if destInfo.Offset == 0 {
		t.logger.Info("starting file transfer",
			"srcPath", src.FilePath, "dstPath", dest.FilePath, "totalSize", srcInfo.Size,
		)
	} else {
		t.logger.Info("resuming file transfer",
			"srcPath", src.FilePath, "dstPath", dest.FilePath,
			"fromOffset", destInfo.Offset, "toOffset", srcInfo.Size,
		)
	}

	if _, err = dest.Storage.TransferFileChunk(ctx, dest.FilePath, proxy, destInfo.Offset, dest.Client); err != nil {
		if errors.Is(err, context.Canceled) {
			err = nil
			t.logger.Info("file transfer is canceled in the middle",
				"srcPath", src.FilePath, "dstPath", dest.FilePath)
			return
		}
		cb(Progress{
			Error:    err,
			Status:   ProgressStatusInError,
			Duration: time.Since(destInfo.StartTime),
		})
		close(interruptedChan)
		return errors.Join(err, errRetryable)
	}

	// finalize the transfer
	if err = dest.Storage.FinalizeTransfer(ctx, dest.FilePath, dest.Client); err != nil {
		if errors.Is(err, storage.ErrFileOrObjectCannotFinalize) {
			if proxy.transferReader.TransferredSize() < srcInfo.Size {
				close(interruptedChan)
				return errors.Join(err, errRetryable)
			}
			if delErr := dest.Storage.DeleteFile(ctx, dest.FilePath, dest.Client); delErr != nil {
				return
			}
		}
		cb(Progress{
			Error:    err,
			Status:   ProgressStatusInError,
			Duration: time.Since(destInfo.StartTime),
		})
		return
	}
	close(completedChan)

	// notify the progress is finished
	cb(Progress{
		Status:     ProgressStatusFinished,
		Duration:   time.Since(destInfo.StartTime),
		StartAt:    destInfo.StartTime,
		FinishAt:   time.Now(),
		Percentage: finishedProgress,
	})

	t.logger.Info("file transfer is finished",
		"srcPath", src.FilePath, "dstPath", dest.FilePath, "totalSize", srcInfo.Size)
	return
}

// getOrCreateDestinationFile gets the destination file info or creates it if it does not exist.
func (t *transfer) getOrCreateDestinationFile(
	ctx context.Context,
	dest DestinationConfig,
	srcInfo xferfile.Info,
) (destInfo xferfile.Info, err error) {
	if destInfo, err = dest.Storage.GetFileInfo(ctx, dest.FilePath, dest.Client); err != nil {
		// if file does not exist, create it
		if errors.Is(err, xferfile.ErrFileNotExists) {
			if err = dest.Storage.CreateFile(
				ctx,
				dest.FilePath, srcInfo.Size, srcInfo.ModTime,
				dest.Client,
			); err != nil {
				return
			}
		} else {
			return
		}
		// get the file info again
		if destInfo, err = dest.Storage.GetFileInfo(ctx, dest.FilePath, dest.Client); err != nil {
			return
		}
	}
	return
}

// verifyFileChanges verifies if the source file has been modified and re-creates the destination file.
func (t *transfer) verifyFileChanges(
	ctx context.Context,
	dest DestinationConfig,
	srcInfo xferfile.Info,
	destInfo xferfile.Info,
) (updatedInfo xferfile.Info, err error) {
	updatedInfo = destInfo
	if srcInfo.ModTime.UTC().Equal(destInfo.ModTime.UTC()) {
		return
	}
	t.logger.Info("source file has been modified, re-creating destination file",
		"srcModTime", srcInfo.ModTime, "dstModTime", destInfo.ModTime,
	)
	if err = dest.Storage.DeleteFile(ctx, dest.FilePath, dest.Client); err != nil {
		return
	}
	if err = dest.Storage.CreateFile(
		ctx,
		dest.FilePath, srcInfo.Size, srcInfo.ModTime,
		dest.Client,
	); err != nil {
		return
	}
	// get the file info again
	if updatedInfo, err = dest.Storage.GetFileInfo(ctx, dest.FilePath, dest.Client); err != nil {
		return
	}
	return
}
