package xferfile_test

import (
	"github.com/derektruong/fxfer/internal/xferfile"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Info", func() {
	Describe("GenerateInfoPath", func() {
		It("should return info file path", func() {
			infoPath, err := xferfile.GenerateInfoPath("sample-prefix/sample-object.txt")
			Expect(err).ToNot(HaveOccurred())
			Expect(infoPath).To(Equal("sample-prefix/sample-object.info"))
		})

		It("should return error when file path is empty", func() {
			_, err := xferfile.GenerateInfoPath("")
			Expect(err).To(HaveOccurred())
		})
	})
})
