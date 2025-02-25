package iometer_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	"github.com/derektruong/fxfer/internal/iometer"
	mock_iometer "github.com/derektruong/fxfer/internal/iometer/mock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
)

var _ = Describe("TransferReader", func() {
	var (
		mockCtrl        *gomock.Controller
		mockReadCloser  *mock_iometer.MockReadCloser
		reader          io.Reader
		transferredSize int64
		transferReader  *iometer.TransferReader
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		DeferCleanup(mockCtrl.Finish)
		mockReadCloser = mock_iometer.NewMockReadCloser(mockCtrl)
		reader = bytes.NewBufferString("test data")
		transferredSize = 0
		transferReader = iometer.NewTransferReader(reader, &transferredSize)
	})

	Describe("Read", func() {
		It("should read data and update transferredSize", func(ctx context.Context) {
			data := make([]byte, 5)
			n, err := transferReader.Read(data)

			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(5))
			Expect(string(data)).To(Equal("test "))
			Expect(transferReader.TransferredSize()).To(Equal(int64(5)))
		}, NodeTimeout(10*time.Second))

		It("should handle reading all data correctly", func(ctx context.Context) {
			data := make([]byte, 100)
			n, err := transferReader.Read(data)

			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(9))
			Expect(string(data[:n])).To(Equal("test data"))
			Expect(transferReader.TransferredSize()).To(Equal(int64(9)))

			n, err = transferReader.Read(data)
			Expect(err).To(Equal(io.EOF))
			Expect(n).To(Equal(0))
			Expect(transferReader.TransferredSize()).To(Equal(int64(9)))
		}, NodeTimeout(10*time.Second))

		It("should propagate errors from the underlying reader", func(ctx context.Context) {
			errorProgress := iometer.NewTransferReader(mockReadCloser, &transferredSize)
			mockReadCloser.EXPECT().Read(gomock.Any()).Return(0, errors.New("read error"))
			data := make([]byte, 5)
			n, err := errorProgress.Read(data)

			Expect(err).To(MatchError("read error"))
			Expect(n).To(Equal(0))
			Expect(errorProgress.TransferredSize()).To(Equal(int64(0)))
		}, NodeTimeout(10*time.Second))
	})

	Describe("TransferredSize", func() {
		It("should return the transferred size", func(ctx context.Context) {
			Expect(transferReader.TransferredSize()).To(Equal(int64(0)))
		}, NodeTimeout(10*time.Second))

		It("should return the transferred size after reading data", func(ctx context.Context) {
			data := make([]byte, 5)
			transferReader.Read(data)
			Expect(transferReader.TransferredSize()).To(Equal(int64(5)))
		}, NodeTimeout(10*time.Second))
	})

	Describe("SetLimiter", func() {
		It("should set the rate limit correctly", func(ctx context.Context) {
			transferReader.SetRateLimit(1)
			data := make([]byte, 3)

			since := time.Now()
			n, err := transferReader.Read(data)
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(3))
			Expect(time.Since(since)).To(BeNumerically("~", 3*time.Second, 1*time.Second))
		}, NodeTimeout(10*time.Second))
	})

	Describe("Close", func() {
		It("should close the underlying reader if it implements io.Closer", func(ctx context.Context) {
			closableProgress := iometer.NewTransferReader(mockReadCloser, &transferredSize)
			mockReadCloser.EXPECT().Close().Return(nil)
			Expect(closableProgress.Close()).To(Succeed())
		}, NodeTimeout(10*time.Second))

		It("should do nothing if the underlying reader doesn't implement io.Closer", func(ctx context.Context) {
			err := transferReader.Close()
			Expect(err).NotTo(HaveOccurred())
		}, NodeTimeout(10*time.Second))
	})
})
