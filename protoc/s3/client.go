package s3

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/metrics/smithyotelmetrics"
	"github.com/derektruong/fxfer/protoc"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
)

var connectionIDNamespace = uuid.MustParse("8676c88d-b3f7-44b2-b645-11c28d6bb4c8")

// Client represents the S3 storage client.
type Client struct {
	Endpoint   string `json:"endpoint"`
	BucketName string `json:"bucketName"`
	Region     string `json:"region"`
	AccessKey  string `json:"accessKey"`
	SecretKey  string `json:"secretKey"`
}

// NewClient creates a new S3 client.
func NewClient(
	endpoint, bucketName,
	Region, AccessKey, SecretKey string,
) (c *Client) {
	c = &Client{
		Endpoint:   endpoint,
		BucketName: bucketName,
		Region:     Region,
		AccessKey:  AccessKey,
		SecretKey:  SecretKey,
	}
	return
}

func (c Client) GetConnectionPool(logr.Logger) protoc.ConnectionPool {
	panic(errors.ErrUnsupported)
}

func (c Client) GetS3API() protoc.S3API {
	s3Options := awss3.Options{
		Region:       c.Region,
		BaseEndpoint: aws.String(c.Endpoint),
		Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     c.AccessKey,
				SecretAccessKey: c.SecretKey,
			}, nil
		}),
		MeterProvider: smithyotelmetrics.Adapt(otel.GetMeterProvider()),
	}
	return awss3.New(s3Options)
}

func (c Client) GetCredential() any {
	return c
}

func (c Client) GetConnectionID() string {
	return uuid.NewSHA1(
		connectionIDNamespace,
		[]byte(fmt.Sprintf(
			"%s:%s:%s:%s:%s",
			c.Endpoint, c.BucketName, c.Region, c.AccessKey, c.SecretKey),
		),
	).String()
}

func (c Client) GetURI() string {
	endpoint := c.Endpoint
	for _, scheme := range []string{"https", "http"} {
		endpoint = strings.TrimPrefix(endpoint, scheme+"://")
	}
	return fmt.Sprintf("%s/%s", endpoint, c.BucketName)
}
