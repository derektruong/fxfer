package xferfiletest_test

import (
	"github.com/derektruong/fxfer/internal/xferfile"
	"github.com/derektruong/fxfer/internal/xferfile/xferfiletest"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"
)

var _ = Describe("InfoFactory", func() {
	DescribeTable("should create a new Info correctly",
		func(editFn func(info *xferfile.Info), matcher types.GomegaMatcher) {
			info := xferfiletest.InfoFactory(editFn)
			Expect(info).To(matcher)
		},
		Entry("should create a new Info correctly", nil, gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
			"Path":       Not(BeEmpty()),
			"Size":       BeNumerically("~", 0, 1000000),
			"Name":       Not(BeEmpty()),
			"Extension":  Not(BeEmpty()),
			"ModTime":    Not(BeZero()),
			"StartTime":  Not(BeZero()),
			"FinishTime": Not(BeZero()),
			"Offset":     BeNumerically("~", 0, 1000000),
			"Checksum":   Not(BeEmpty()),
			"Metadata":   Not(BeEmpty()),
		})),
		Entry("should create a new Info correctly with editFn", func(info *xferfile.Info) {
			info.Path = "test"
		}, gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
			"Path": Equal("test"),
		})),
		Entry("should create a new Info correctly with editFn", func(info *xferfile.Info) {
			info.Size = 100
		}, gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
			"Size": Equal(int64(100)),
		})),
	)
})
