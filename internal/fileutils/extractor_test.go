package fileutils_test

import (
	"runtime"

	"github.com/derektruong/fxfer/internal/fileutils"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Extractor", func() {
	Describe("ExtractFileParts", func() {
		When("file path in Unix-like format", func() {
			BeforeEach(func() {
				if runtime.GOOS == "windows" {
					Skip("skipping test on windows OS")
				}
			})

			It("should return prefix, file name, and extension", func() {
				prefix, fileName, fileExt, err := fileutils.ExtractFileParts("sample-prefix/sample-object.txt")
				Expect(err).ToNot(HaveOccurred())
				Expect(prefix).To(Equal("sample-prefix"))
				Expect(fileName).To(Equal("sample-object"))
				Expect(fileExt).To(Equal("txt"))
			})

			It("should return file name and extension when prefix is not present", func() {
				prefix, fileName, fileExt, err := fileutils.ExtractFileParts("sample-object.txt")
				Expect(err).ToNot(HaveOccurred())
				Expect(prefix).To(Equal(""))
				Expect(fileName).To(Equal("sample-object"))
				Expect(fileExt).To(Equal("txt"))
			})

			It("should return error when file path is empty", func() {
				_, _, _, err := fileutils.ExtractFileParts("")
				Expect(err.Error()).To(Equal("asset file parts are required"))
			})

			It("should return error when extension is not present", func() {
				_, _, _, err := fileutils.ExtractFileParts("sample-object")
				Expect(err.Error()).To(Equal("file extension is required"))
			})
		})

		When("file path in Windows-like format", func() {
			BeforeEach(func() {
				if runtime.GOOS != "windows" {
					Skip("skipping test on non-windows OS")
				}
			})
			It("should return prefix, file name, and extension", func() {
				prefix, fileName, fileExt, err := fileutils.ExtractFileParts("sample-prefix\\sample-object.txt")
				Expect(err).ToNot(HaveOccurred())
				Expect(prefix).To(Equal("sample-prefix"))
				Expect(fileName).To(Equal("sample-object"))
				Expect(fileExt).To(Equal("txt"))
			})

			It("should return file name and extension when prefix is not present", func() {
				prefix, fileName, fileExt, err := fileutils.ExtractFileParts("sample-object.txt")
				Expect(err).ToNot(HaveOccurred())
				Expect(prefix).To(Equal(""))
				Expect(fileName).To(Equal("sample-object"))
				Expect(fileExt).To(Equal("txt"))
			})

			It("should return error when file path is empty", func() {
				_, _, _, err := fileutils.ExtractFileParts("")
				Expect(err.Error()).To(Equal("asset file parts are required"))
			})

			It("should return error when extension is not present", func() {
				_, _, _, err := fileutils.ExtractFileParts("sample-object")
				Expect(err.Error()).To(Equal("file extension is required"))
			})
		})
	})
})
