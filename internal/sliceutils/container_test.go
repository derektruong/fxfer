package sliceutils_test

import (
	"github.com/derektruong/fxfer/internal/sliceutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Container", func() {
	Describe("Contains", func() {
		It("should return true if the item is in the slice", func() {
			slice := []string{"a", "b", "c"}
			item := "b"
			Expect(sliceutils.Contains(slice, item)).To(BeTrue())
		})

		It("should return true if the item is in the slice and case-insensitive", func() {
			slice := []string{"a", "b", "c"}
			item := "B"
			Expect(sliceutils.Contains(slice, item)).To(BeTrue())
		})

		It("should return false if the item is not in the slice", func() {
			slice := []string{"a", "b", "c"}
			item := "d"
			Expect(sliceutils.Contains(slice, item)).To(BeFalse())
		})

		It("should return false if the slice is empty", func() {
			var slice []string
			item := "a"
			Expect(sliceutils.Contains(slice, item)).To(BeFalse())
		})
	})
})
