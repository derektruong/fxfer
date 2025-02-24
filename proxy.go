package fxfer

import (
	"context"
	"io"
	"math"
	"sync/atomic"
	"time"

	"github.com/derektruong/fxfer/internal/iometer"
	"github.com/samber/lo"
)

// ProgressUpdatedCallback is a function that is called when the
// progress of a transfer is updated.
type ProgressUpdatedCallback func(progress Progress)

// ProgressStatus is an enum that represents the status of the progress
type ProgressStatus int

const (
	// ProgressStatusInProgress is the status of the progress when the transfer is in progress
	ProgressStatusInProgress ProgressStatus = iota
	// ProgressStatusFinalizing is the status of the progress when the transfer is finalizing
	ProgressStatusFinalizing
	// ProgressStatusFinished is the status of the progress when the transfer is finished
	ProgressStatusFinished
	// ProgressStatusInError is the status of the progress when the transfer is in error
	ProgressStatusInError
)

// Progress is a struct that contains information about the progress
type Progress struct {
	// Status is the status of the progress
	Status ProgressStatus

	// TotalSize is the total number of bytes that need to be transferred
	TotalSize int64

	// TransferredSize is the number of bytes that have been transferred
	TransferredSize int64

	// Percentage is the percentage of the transfer that has been completed
	Percentage int

	// Speed is the speed of the transfer in bytes per second
	Speed int64

	// Duration is the duration of the transfer
	Duration time.Duration

	// Error is the error that occurred during the transfer (when Status is ProgressStatusInError)
	Error error

	// StartAt is the time when the transfer started
	StartAt time.Time

	// FinishAt is the time when the transfer finished
	FinishAt time.Time
}

const (
	// finalizingProgress is the progress value that is used when
	// the transfer is finalizing (may assemble file parts, etc.)
	finalizingProgress = 99
	// finishedProgress is the progress value that is used when
	// the transfer is finished (100%)
	finishedProgress = 100
)

// proxyReader is a wrapper around an io.Reader that keeps
// track of the number of bytes transferred.
type proxyReader struct {
	transferReader *iometer.TransferReader

	// done and doneCtx are used to signal when the reader is done
	doneCtx context.Context
	done    context.CancelFunc

	// closed is a flag that indicates if the proxyReader is closed
	closed bool
}

// newProxyReader creates a new proxyReader with the specified io.Reader
// and transferredSize.
func newProxyReader(r io.Reader, transferredSize int64) (p *proxyReader) {
	p = &proxyReader{
		transferReader: iometer.NewTransferReader(r, &transferredSize),
	}
	p.doneCtx, p.done = context.WithCancel(context.Background())
	return
}

// Read reads data from the underlying io.Reader and updates the
// transferredSize field using atomic operations.
func (p *proxyReader) Read(data []byte) (n int, err error) {
	select {
	case <-p.doneCtx.Done():
		return 0, context.Canceled
	default:
		return p.transferReader.Read(data)
	}
}

// Close closes the underlying io.Reader if it implements the
// io.Closer interface.
func (p *proxyReader) Close() (err error) {
	if p.closed {
		return
	}
	p.done()
	if err = p.transferReader.Close(); err != nil {
		return
	}
	p.closed = true
	return
}

// trackProgress tracks the progress of the transfer and calls the
// specified callback function when the progress is updated.
func (p *proxyReader) trackProgress(
	ctx context.Context,
	startTime time.Time,
	totalSize int64,
	refreshInterval time.Duration,
	interrupted <-chan struct{},
	completed <-chan struct{},
	cb ProgressUpdatedCallback,
) {
	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()

	updateProgressFunc := func(isDone bool) (exit bool) {
		var progressPercentage int
		status := ProgressStatusInProgress
		transferredSize := atomic.LoadInt64(lo.ToPtr(p.transferReader.TransferredSize()))
		if totalSize == 0 || isDone {
			progressPercentage = finishedProgress
			status = ProgressStatusFinished
			exit = true
		} else {
			progressPercentage = int(math.Min(
				finishedProgress,
				math.Round(float64(transferredSize)/float64(totalSize)*100),
			))
		}
		if progressPercentage == finishedProgress && !isDone {
			progressPercentage = finalizingProgress
			status = ProgressStatusFinalizing
		}

		cb(Progress{
			Status:          status,
			TotalSize:       totalSize,
			TransferredSize: transferredSize,
			Percentage:      progressPercentage,
			Duration:        time.Since(startTime),
			Speed:           transferredSize / int64(math.Max(1, time.Since(startTime).Seconds())),
			StartAt:         startTime,
		})
		return
	}

	for {
		select {
		case <-ctx.Done():
			p.done()
			return
		case <-interrupted:
			p.done()
			return
		case <-completed:
			updateProgressFunc(true)
			return
		case <-ticker.C:
			if exit := updateProgressFunc(false); exit {
				return
			}
		}
	}
}
