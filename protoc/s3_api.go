package protoc

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3API interface {
	PutObject(ctx context.Context, input *s3.PutObjectInput, opt ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	ListParts(ctx context.Context, input *s3.ListPartsInput, opt ...func(*s3.Options)) (*s3.ListPartsOutput, error)
	UploadPart(ctx context.Context, input *s3.UploadPartInput, opt ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	GetObject(ctx context.Context, input *s3.GetObjectInput, opt ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, input *s3.HeadObjectInput, opt ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	CreateMultipartUpload(ctx context.Context, input *s3.CreateMultipartUploadInput, opt ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	AbortMultipartUpload(ctx context.Context, input *s3.AbortMultipartUploadInput, opt ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
	DeleteObject(ctx context.Context, input *s3.DeleteObjectInput, opt ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	DeleteObjects(ctx context.Context, input *s3.DeleteObjectsInput, opt ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput, opt ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	UploadPartCopy(ctx context.Context, input *s3.UploadPartCopyInput, opt ...func(*s3.Options)) (*s3.UploadPartCopyOutput, error)
}
