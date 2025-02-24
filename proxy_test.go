package fxfer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	mock_iometer "github.com/derektruong/fxfer/internal/iometer/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
)

var _ = Describe("proxyReader", func() {
	var (
		mockCtrl        *gomock.Controller
		mockReadCloser  *mock_iometer.MockReadCloser
		reader          io.Reader
		transferredSize int64
		proxy           *proxyReader
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		DeferCleanup(mockCtrl.Finish)
		mockReadCloser = mock_iometer.NewMockReadCloser(mockCtrl)
		reader = bytes.NewBufferString("test data")
		transferredSize = 0
		proxy = newProxyReader(reader, transferredSize)
	})

	Describe("newProxyReader", func() {
		It("should create a new transferProgress with the given reader and size", func() {
			Expect(proxy.transferReader.TransferredSize()).To(Equal(int64(0)))
		})
	})

	Describe("Read", func() {
		It("should read data and update transferredSize", func(ctx context.Context) {
			data := make([]byte, 5)
			n, err := proxy.Read(data)

			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(5))
			Expect(string(data)).To(Equal("test "))
			Expect(proxy.transferReader.TransferredSize()).To(Equal(int64(5)))
		}, NodeTimeout(10*time.Second))

		It("should handle reading all data correctly", func(ctx context.Context) {
			data := make([]byte, 100)
			n, err := proxy.Read(data)

			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(9))
			Expect(string(data[:n])).To(Equal("test data"))
			Expect(proxy.transferReader.TransferredSize()).To(Equal(int64(9)))

			n, err = proxy.Read(data)
			Expect(err).To(Equal(io.EOF))
			Expect(n).To(Equal(0))
			Expect(proxy.transferReader.TransferredSize()).To(Equal(int64(9)))
		}, NodeTimeout(10*time.Second))

		It("should propagate errors from the underlying reader", func(ctx context.Context) {
			errorProgress := newProxyReader(mockReadCloser, 0)
			mockReadCloser.EXPECT().Read(gomock.Any()).Return(0, errors.New("read error"))
			data := make([]byte, 5)
			n, err := errorProgress.Read(data)

			Expect(err).To(MatchError("read error"))
			Expect(n).To(Equal(0))
			Expect(errorProgress.transferReader.TransferredSize()).To(Equal(int64(0)))
		}, NodeTimeout(10*time.Second))
	})

	Describe("Close", func() {
		It("should close the underlying reader if it implements io.Closer", func(ctx context.Context) {
			closableProgress := newProxyReader(mockReadCloser, 0)
			mockReadCloser.EXPECT().Close().Return(nil)
			Expect(closableProgress.Close()).To(Succeed())
		}, NodeTimeout(10*time.Second))

		It("should do nothing if the underlying reader doesn't implement io.Closer", func(ctx context.Context) {
			err := proxy.Close()
			Expect(err).NotTo(HaveOccurred())
		}, NodeTimeout(10*time.Second))
	})

	Describe("trackProgress", func() {
		var (
			readCloser      io.ReadCloser
			content         string
			startTime       time.Time
			totalSize       int64
			interrupted     chan struct{}
			completed       chan struct{}
			progressUpdates []Progress
		)

		BeforeEach(func() {
			content = gofakeit.Sentence(100)
			readCloser = io.NopCloser(bytes.NewBufferString(content))
			DeferCleanup(readCloser.Close)
			proxy = newProxyReader(readCloser, 0)
			DeferCleanup(proxy.Close)
			startTime = time.Now()
			totalSize = int64(len(content))
			interrupted = make(chan struct{})
			completed = make(chan struct{})
			progressUpdates = make([]Progress, 0)
		})

		It("should update progress periodically", func(ctx context.Context) {
			cb := func(progress Progress) {
				progressUpdates = append(progressUpdates, progress)
			}

			go proxy.trackProgress(ctx, startTime, totalSize, 10*time.Millisecond, interrupted, completed, cb)

			buf := new(bytes.Buffer)
			_, err := io.Copy(buf, proxy)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func(g Gomega) {
				g.Expect(len(progressUpdates)).To(BeNumerically(">", 0))
				g.Expect(progressUpdates[len(progressUpdates)-1].Status).
					To(Equal(ProgressStatusFinalizing))
			}).WithContext(ctx).Should(Succeed())

			close(completed)
		}, NodeTimeout(10*time.Second))

		It("should handle context cancellation", func(ctx context.Context) {
			cb := func(progress Progress) {
				progressUpdates = append(progressUpdates, progress)
			}

			ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()
			go proxy.trackProgress(ctx, startTime, totalSize, 10*time.Millisecond, interrupted, completed, cb)

			Eventually(func(g Gomega) {
				g.Expect(len(progressUpdates)).To(BeNumerically(">", 0))
				g.Expect(progressUpdates[len(progressUpdates)-1].Status).To(Equal(ProgressStatusInProgress))
			}).WithContext(ctx).Should(Succeed())

			close(completed)
		}, NodeTimeout(10*time.Second))

		It("should handle completed channel", func(ctx context.Context) {
			cb := func(progress Progress) {
				progressUpdates = append(progressUpdates, progress)
			}

			go proxy.trackProgress(ctx, startTime, totalSize, 10*time.Millisecond, interrupted, completed, cb)

			time.AfterFunc(100*time.Millisecond, func() {
				close(completed)
			})

			Eventually(func(g Gomega) {
				g.Expect(len(progressUpdates)).To(BeNumerically(">", 0))
				g.Expect(progressUpdates[len(progressUpdates)-1].Status).To(Equal(ProgressStatusFinished))
			}).WithContext(ctx).Should(Succeed())
		})

		It("should handle interrupted channel", func(ctx context.Context) {
			cb := func(progress Progress) {
				progressUpdates = append(progressUpdates, progress)
			}

			go proxy.trackProgress(ctx, startTime, totalSize, 10*time.Millisecond, interrupted, completed, cb)

			time.AfterFunc(100*time.Millisecond, func() {
				close(interrupted)
			})

			Eventually(func(g Gomega) {
				g.Expect(len(progressUpdates)).To(BeNumerically(">", 0))
				g.Expect(progressUpdates[len(progressUpdates)-1].Status).To(Equal(ProgressStatusInProgress))
			}).WithContext(ctx).Should(Succeed())
		}, NodeTimeout(10*time.Second))
	})
})
