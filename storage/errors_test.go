package storage_test

import (
	"context"
	"errors"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/derektruong/fxfer/storage"
)

var _ = Describe("Errors", func() {
	Describe("WrapErrorWithStorage", func() {
		It("should wrap error with storage", func(ctx context.Context) {
			err := storage.WrapErrorWithStorage(storage.LocalStorage, false, errors.New("test error"))
			Expect(err).To(HaveOccurred())
			Expect(err).To(MatchError(`at "Local" destination storage, occurred error: test error`))
		}, NodeTimeout(10*time.Second))
		
		It("should return nil if error is nil", func(ctx context.Context) {
			err := storage.WrapErrorWithStorage(storage.LocalStorage, false, nil)
			Expect(err).ToNot(HaveOccurred())
		}, NodeTimeout(10*time.Second))
	})
})
