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

var _ = Describe("Command", func() {
	It("should validate source command correctly", func(ctx context.Context) {
		cmd := sourceCommandFactory(nil)
		Expect(cmd.Validate(ctx)).To(Succeed())
	}, NodeTimeout(10*time.Second))

	It("should validate destination command correctly", func(ctx context.Context) {
		cmd := destinationCommandFactory(nil)
		Expect(cmd.Validate(ctx)).To(Succeed())
	}, NodeTimeout(10*time.Second))

	DescribeTable(
		"Validate source command matches with validation",
		func(ctx context.Context, sourceCommand fxfer.SourceCommand, expectedMsg string) {
			Expect(sourceCommand.Validate(ctx)).To(MatchError(expectedMsg))
		},
		Entry(
			"should return error if file path is empty",
			sourceCommandFactory(func(cmd *fxfer.SourceCommand) {
				cmd.FilePath = ""
			}),
			"Key: 'SourceCommand.FilePath' Error:Field validation for 'FilePath' failed on the 'required' tag",
			NodeTimeout(10*time.Second),
		),
		Entry(
			"should return error if storage is nil",
			sourceCommandFactory(func(cmd *fxfer.SourceCommand) {
				cmd.Storage = nil
			}),
			"Key: 'SourceCommand.Storage' Error:Field validation for 'Storage' failed on the 'required' tag",
			NodeTimeout(10*time.Second),
		),
		Entry(
			"should return error if client is nil",
			sourceCommandFactory(func(cmd *fxfer.SourceCommand) {
				cmd.Client = nil
			}),
			"Key: 'SourceCommand.Client' Error:Field validation for 'Client' failed on the 'required' tag",
			NodeTimeout(10*time.Second),
		),
	)

	DescribeTable(
		"Validate destination command matches with validation",
		func(ctx context.Context, destCommand fxfer.DestinationCommand, expectedMsg string) {
			err := destCommand.Validate(ctx)
			Expect(err).To(MatchError(expectedMsg))
		},
		Entry(
			"should return error if file path is empty",
			destinationCommandFactory(func(cmd *fxfer.DestinationCommand) {
				cmd.FilePath = ""
			}),
			"Key: 'DestinationCommand.FilePath' Error:Field validation for 'FilePath' failed on the 'required' tag",
			NodeTimeout(10*time.Second),
		),
		Entry(
			"should return error if storage is nil",
			destinationCommandFactory(func(cmd *fxfer.DestinationCommand) {
				cmd.Storage = nil
			}),
			"Key: 'DestinationCommand.Storage' Error:Field validation for 'Storage' failed on the 'required' tag",
			NodeTimeout(10*time.Second),
		),
		Entry(
			"should return error if client is nil",
			destinationCommandFactory(func(cmd *fxfer.DestinationCommand) {
				cmd.Client = nil
			}),
			"Key: 'DestinationCommand.Client' Error:Field validation for 'Client' failed on the 'required' tag",
			NodeTimeout(10*time.Second),
		),
	)
})

func sourceCommandFactory(editFn func(*fxfer.SourceCommand)) fxfer.SourceCommand {
	cmd := &fxfer.SourceCommand{
		FilePath: fmt.Sprintf("%s/%s.%s", gofakeit.Word(), gofakeit.Word(), gofakeit.FileExtension()),
		Storage:  s3.NewSource(GinkgoLogr),
		Client:   s3protoc.NewClient("http://localhost:9000", "bucket", "us-east-1", "minioadmin", "minioadmin"),
	}
	if editFn != nil {
		editFn(cmd)
	}
	return *cmd
}

func destinationCommandFactory(editFn func(*fxfer.DestinationCommand)) fxfer.DestinationCommand {
	cmd := &fxfer.DestinationCommand{
		FilePath: fmt.Sprintf("%s/%s.%s", gofakeit.Word(), gofakeit.Word(), gofakeit.FileExtension()),
		Storage:  s3.NewDestination(GinkgoLogr),
		Client:   s3protoc.NewClient("http://localhost:9000", "bucket", "us-east-1", "minioadmin", "minioadmin"),
	}
	if editFn != nil {
		editFn(cmd)
	}
	return *cmd
}
