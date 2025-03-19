package s3

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type InfiniteZeroReader struct{}

func (izr InfiniteZeroReader) Read(b []byte) (int, error) {
	b[0] = 0
	return 1, nil
}

type ErrorReader struct{}

func (ErrorReader) Read(b []byte) (int, error) {
	return 0, errors.New("error from ErrorReader")
}

var _ = Describe("S3storePartProducer", func() {
	It("should use memory when FILE_TRANSFERER_S3_TEMP_MEMORY is set", func() {
		Expect(os.Setenv("FILE_TRANSFERER_S3_TEMP_MEMORY", "1")).To(Succeed())
		defer os.Unsetenv("FILE_TRANSFERER_S3_TEMP_MEMORY")
		s3PartProducer, _ := newS3PartProducer(strings.NewReader(""), 0, "")
		Expect(s3PartProducer.tmpDir).To(Equal(TempDirUseMemory))
	})

	It("part producer should consumes entire reader without error", func() {
		expectedStr := "test"
		r := strings.NewReader(expectedStr)
		pp, fileChan := newS3PartProducer(r, 0, "")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go pp.produce(ctx, 1)

		actualStr := ""
		b := make([]byte, 1)
		for chunk := range fileChan {
			n, err := chunk.reader.Read(b)
			if err != nil {
				Fail("unexpected error: " + err.Error())
			}
			if n != 1 {
				Fail(fmt.Sprintf("incorrect number of bytes read: wanted %d, got %d", 1, n))
			}
			if chunk.size != 1 {
				Fail(fmt.Sprintf("incorrect number of bytes in struct: wanted %d, got %d", 1, chunk.size))
			}
			actualStr += string(b)

			Expect(chunk.closeReader()).To(Succeed())
		}

		if actualStr != expectedStr {
			_, _ = fmt.Fprintf(GinkgoWriter, "incorrect string read from channel: wanted %s, got %s", expectedStr, actualStr)
			return
		}

		if pp.err != nil {
			_, _ = fmt.Fprintf(GinkgoWriter, "unexpected error from part producer: %s", pp.err)
			return
		}
	})

	It("part producer should exist when context is cancelled", func() {
		pp, fileChan := newS3PartProducer(InfiniteZeroReader{}, 0, "")

		ctx, cancel := context.WithCancel(context.Background())
		completedChan := make(chan struct{})
		go func() {
			pp.produce(ctx, 10)
			completedChan <- struct{}{}
		}()

		cancel()

		select {
		case <-completedChan:
			// producer exited cleanly
		case <-time.After(2 * time.Second):
			_, _ = fmt.Fprintf(GinkgoWriter, "timed out waiting for producer to exit")
			return
		}

		safelyDrainChannelOrFail(fileChan)
	})

	It("part producer should exist when unable to read from file", func() {
		pp, fileChan := newS3PartProducer(ErrorReader{}, 0, "")

		completedChan := make(chan struct{})
		go func() {
			pp.produce(context.Background(), 10)
			completedChan <- struct{}{}
		}()

		select {
		case <-completedChan:
			// producer exited cleanly
		case <-time.After(2 * time.Second):
			_, _ = fmt.Fprintf(GinkgoWriter, "timed out waiting for producer to exit")
			return
		}

		safelyDrainChannelOrFail(fileChan)
	})
})

func safelyDrainChannelOrFail(c <-chan fileChunk) {
	// At this point, we've signaled that the producer should exit, but it may write a few files
	// into the channel before closing it and exiting. Make sure that we get a nil value
	// eventually.
	for i := 0; i < 100; i++ {
		if _, more := <-c; !more {
			return
		}
	}

	Fail("timed out waiting for channel to drain")
}
