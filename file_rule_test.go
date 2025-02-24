package fxfer

import (
	"regexp"
	"time"

	"github.com/derektruong/fxfer/internal/xferfile"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("FileRule", func() {
	var (
		rule     *fileRule
		fileInfo xferfile.Info
	)

	BeforeEach(func() {
		rule = &fileRule{}
		fileInfo = xferfile.Info{
			Path:      "/Path1/Đường dẫn 2/path-3/Tên file.mov",
			Size:      2 << 30, // 2 GB
			Name:      "Tên file",
			Extension: "mov",
			ModTime:   time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		}
	})

	It("should return error when file size exceeds the maximum allowed size", func() {
		rule.MaxFileSize = 1 << 30 // 1 GB
		err := rule.Check(fileInfo)
		Expect(err).To(MatchError(ErrMaxFileSizeExceeded(rule.MaxFileSize, fileInfo.Size)))
	})

	It("should return error when file size does not meet the minimum required size", func() {
		rule.MinFileSize = 4 << 30 // 4 GB
		err := rule.Check(fileInfo)
		Expect(err).To(MatchError(ErrMinFileSizeNotMet(rule.MinFileSize, fileInfo.Size)))
	})

	It("should return error when file extension is not allowed", func() {
		rule.ExtensionWhitelist = []string{"mp4"}
		err := rule.Check(fileInfo)
		Expect(err).To(MatchError(ErrExtensionNotAllowed(fileInfo.Extension)))
	})

	It("should return error when file extension is blocked", func() {
		rule.ExtensionBlacklist = []string{"mov", "mp4"}
		err := rule.Check(fileInfo)
		Expect(err).To(MatchError(ErrExtensionBlocked(fileInfo.Extension)))
	})

	It("should return error when file was modified before the required time", func() {
		rule.ModifiedAfter = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		err := rule.Check(fileInfo)
		Expect(err).To(MatchError(ErrModifiedAfter(rule.ModifiedAfter)))
	})

	It("should return error when file was modified after the required time", func() {
		rule.ModifiedBefore = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		err := rule.Check(fileInfo)
		Expect(err).To(MatchError(ErrModifiedBefore(rule.ModifiedBefore)))
	})

	It("should return error when file name does not match the required pattern", func() {
		rule.FileNamePattern = regexp.MustCompile(`^abc$`)
		err := rule.Check(fileInfo)
		Expect(err).To(MatchError(ErrFileNamePatternMismatch(rule.FileNamePattern.String())))
	})

	It("should return nil when all checks pass", func() {
		err := rule.Check(fileInfo)
		Expect(err).To(BeNil())
	})
})
