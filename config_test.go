package fxfer_test

import (
	"context"
	"fmt"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	fxfer "github.com/derektruong/fxfer"
	s3protoc "github.com/derektruong/fxfer/protoc/s3"
	"github.com/derektruong/fxfer/storage/s3"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Config", func() {
	It("should validate source config correctly", func(ctx context.Context) {
		cmd := sourceConfigFactory(nil)
		Expect(cmd.Validate(ctx)).To(Succeed())
	}, NodeTimeout(10*time.Second))

	It("should validate destination config correctly", func(ctx context.Context) {
		cmd := destinationConfigFactory(nil)
		Expect(cmd.Validate(ctx)).To(Succeed())
	}, NodeTimeout(10*time.Second))

	DescribeTable(
		"Validate source config matches with validation",
		func(ctx context.Context, sourceConfig fxfer.SourceConfig, expectedMsg string) {
			Expect(sourceConfig.Validate(ctx)).To(MatchError(expectedMsg))
		},
		Entry(
			"should return error if file path is empty",
			sourceConfigFactory(func(cmd *fxfer.SourceConfig) {
				cmd.FilePath = ""
			}),
			"Key: 'SourceConfig.FilePath' Error:Field validation for 'FilePath' failed on the 'required' tag",
			NodeTimeout(10*time.Second),
		),
		Entry(
			"should return error if storage is nil",
			sourceConfigFactory(func(cmd *fxfer.SourceConfig) {
				cmd.Storage = nil
			}),
			"Key: 'SourceConfig.Storage' Error:Field validation for 'Storage' failed on the 'required' tag",
			NodeTimeout(10*time.Second),
		),
		Entry(
			"should return error if client is nil",
			sourceConfigFactory(func(cmd *fxfer.SourceConfig) {
				cmd.Client = nil
			}),
			"Key: 'SourceConfig.Client' Error:Field validation for 'Client' failed on the 'required' tag",
			NodeTimeout(10*time.Second),
		),
	)

	DescribeTable(
		"Validate destination config matches with validation",
		func(ctx context.Context, destConfig fxfer.DestinationConfig, expectedMsg string) {
			err := destConfig.Validate(ctx)
			Expect(err).To(MatchError(expectedMsg))
		},
		Entry(
			"should return error if file path is empty",
			destinationConfigFactory(func(cmd *fxfer.DestinationConfig) {
				cmd.FilePath = ""
			}),
			"Key: 'DestinationConfig.FilePath' Error:Field validation for 'FilePath' failed on the 'required' tag",
			NodeTimeout(10*time.Second),
		),
		Entry(
			"should return error if storage is nil",
			destinationConfigFactory(func(cmd *fxfer.DestinationConfig) {
				cmd.Storage = nil
			}),
			"Key: 'DestinationConfig.Storage' Error:Field validation for 'Storage' failed on the 'required' tag",
			NodeTimeout(10*time.Second),
		),
		Entry(
			"should return error if client is nil",
			destinationConfigFactory(func(cmd *fxfer.DestinationConfig) {
				cmd.Client = nil
			}),
			"Key: 'DestinationConfig.Client' Error:Field validation for 'Client' failed on the 'required' tag",
			NodeTimeout(10*time.Second),
		),
	)
})

func sourceConfigFactory(editFn func(*fxfer.SourceConfig)) fxfer.SourceConfig {
	cmd := &fxfer.SourceConfig{
		FilePath: fmt.Sprintf("%s/%s.%s", gofakeit.Word(), gofakeit.Word(), gofakeit.FileExtension()),
		Storage:  s3.NewSource(GinkgoLogr),
		Client:   s3protoc.NewClient("http://localhost:9000", "bucket", "us-east-1", "minioadmin", "minioadmin"),
	}
	if editFn != nil {
		editFn(cmd)
	}
	return *cmd
}

func destinationConfigFactory(editFn func(*fxfer.DestinationConfig)) fxfer.DestinationConfig {
	cmd := &fxfer.DestinationConfig{
		FilePath: fmt.Sprintf("%s/%s.%s", gofakeit.Word(), gofakeit.Word(), gofakeit.FileExtension()),
		Storage:  s3.NewDestination(GinkgoLogr),
		Client:   s3protoc.NewClient("http://localhost:9000", "bucket", "us-east-1", "minioadmin", "minioadmin"),
	}
	if editFn != nil {
		editFn(cmd)
	}
	return *cmd
}
