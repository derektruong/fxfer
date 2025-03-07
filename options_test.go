package fxfer

import (
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Transfer options", func() {
	var tfr *transfer

	BeforeEach(func() {
		tfr = newTransfer(GinkgoLogr)
	})

	It("should set correct max file size", func() {
		tfr = newTransfer(GinkgoLogr, WithMaxFileSize(1024))
		Expect(tfr.fileRule.MaxFileSize).To(Equal(int64(1024)))
	})

	It("should set correct min file size", func() {
		tfr = newTransfer(GinkgoLogr, WithMinFileSize(1024))
		Expect(tfr.fileRule.MinFileSize).To(Equal(int64(1024)))
	})

	It("should set correct extension whitelist", func() {
		tfr = newTransfer(GinkgoLogr, WithExtensionWhitelist("png", "jpg"))
		Expect(tfr.fileRule.ExtensionWhitelist).To(Equal([]string{"png", "jpg"}))
	})

	It("should set correct extension blacklist", func() {
		tfr = newTransfer(GinkgoLogr, WithExtensionBlacklist("exe", "dll"))
		Expect(tfr.fileRule.ExtensionBlacklist).To(Equal([]string{"exe", "dll"}))
	})

	It("should set correct modified after", func() {
		testTime := time.Now()
		tfr = newTransfer(GinkgoLogr, WithModifiedAfter(testTime))
		Expect(tfr.fileRule.ModifiedAfter).To(Equal(testTime))
	})

	It("should set correct modified before", func() {
		testTime := time.Now()
		tfr = newTransfer(GinkgoLogr, WithModifiedBefore(testTime))
		Expect(tfr.fileRule.ModifiedBefore).To(Equal(testTime))
	})

	It("should set correct file name pattern", func() {
		tfr = newTransfer(GinkgoLogr, WithFileNamePattern(nil))
		Expect(tfr.fileRule.FileNamePattern).To(BeNil())
	})

	It("should set correct refresh progress interval", func() {
		tfr = newTransfer(GinkgoLogr, WithProgressRefreshInterval(5*time.Second))
		Expect(tfr.refreshProgressInterval).To(Equal(5 * time.Second))
	})

	It("should set correct checksum algorithm", func() {
		tfr = newTransfer(GinkgoLogr, WithChecksumAlgorithm(ChecksumAlgorithmCRC32))
		Expect(tfr.checksumAlgorithm).To(Equal(ChecksumAlgorithmCRC32))
	})

	It("should set disable retry", func() {
		tfr = newTransfer(GinkgoLogr, WithDisabledRetry())
		Expect(tfr.disabledRetry).To(BeTrue())
	})

	It("should set correct retry config", func() {
		tfr = newTransfer(GinkgoLogr, WithRetryConfig(RetryConfig{
			MaxRetryAttempts: 10,
			InitialDelay:     1 * time.Second,
			MaxDelay:         10 * time.Second,
		}))
		Expect(tfr.retryConfig.MaxRetryAttempts).To(Equal(10))
		Expect(tfr.retryConfig.InitialDelay).To(Equal(1 * time.Second))
		Expect(tfr.retryConfig.MaxDelay).To(Equal(10 * time.Second))
	})
})

func newTransfer(logger logr.Logger, options ...TransferOption) *transfer {
	tfr := &transfer{
		logger:                  logger,
		fileRule:                new(fileRule),
		refreshProgressInterval: 1 * time.Second,
		checksumAlgorithm:       NoneChecksumAlgorithm,
	}
	for _, opt := range options {
		opt(tfr)
	}
	return tfr
}
