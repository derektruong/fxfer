package iometer

import (
	"context"
	"golang.org/x/time/rate"
	"io"
	"sync/atomic"
	"time"
)

const burstLimit = 1024 * 1024 * 1024 // 1GB

// TransferReader wraps an io.Reader and counts the number of bytes read
// from it.
type TransferReader struct {
	reader  io.Reader
	limiter *rate.Limiter

	// transferredSize is a pointer to an int64 that stores the number of
	// bytes transferred
	transferredSize *int64

	// ctx is the context of the TransferReader
	ctx context.Context

	// closed is a flag that indicates if the readerProxy is closed
	closed bool
}

// NewTransferReader constructs a new TransferReader.
func NewTransferReader(reader io.Reader, transferredSize *int64) (mr *TransferReader) {
	mr = &TransferReader{
		reader:          reader,
		transferredSize: transferredSize,
		ctx:             context.Background(),
	}
	return
}

// Read reads from the underlying reader and increments the counter.
func (tr *TransferReader) Read(p []byte) (n int, err error) {
	if tr.limiter == nil {
		if n, err = tr.reader.Read(p); err != nil {
			return
		}
	} else {
		if n, err = tr.reader.Read(p); err != nil {
			return
		}
		if err = tr.limiter.WaitN(tr.ctx, n); err != nil {
			return
		}
	}
	if n > 0 && tr.transferredSize != nil {
		atomic.AddInt64(tr.transferredSize, int64(n))
	}
	return
}

// Close closes the underlying io.Reader if it implements the
// io.Closer interface.
func (tr *TransferReader) Close() (err error) {
	if tr.closed {
		return
	}
	if closer, ok := tr.reader.(io.Closer); ok {
		err = closer.Close()
	}
	tr.closed = true
	return
}

// TransferredSize returns the number of bytes transferred.
func (tr *TransferReader) TransferredSize() int64 {
	return atomic.LoadInt64(tr.transferredSize)
}

// SetRateLimit sets rate limit (bytes/sec) to the reader.
func (tr *TransferReader) SetRateLimit(bytesPerSec float64) {
	tr.limiter = rate.NewLimiter(rate.Limit(bytesPerSec), burstLimit)
	tr.limiter.AllowN(time.Now(), burstLimit) // spend initial burst
}
