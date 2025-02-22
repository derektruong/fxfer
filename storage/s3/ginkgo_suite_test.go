package s3

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/derektruong/fxfer/protoc"
	"github.com/derektruong/fxfer/protoc/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	minioRootUser     = "minioadmin"
	minioRootPassword = "minioadmin"
	minioImage        = "minio/minio:RELEASE.2025-02-07T23-21-09Z"
	minioPort         = "9000"
	minioConsolePort  = "9001"
	originalBucket    = "original"
)

var (
	bucketName                     = "test-bucket"
	region                         = "us-east-1"
	awsS3Client                    *awss3.Client
	endpoint, accessKey, secretKey string
	protocS3Client                 protoc.Client
)

func TestGinkgoSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "S3 Storage tests suite")
}

var _ = BeforeSuite(func() {
	By("setup docker network")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	DeferCleanup(cancel)

	network, err := network.New(ctx)
	Expect(err).ToNot(HaveOccurred())
	DeferCleanup(network.Remove, context.Background())

	By("setup minio cluster")
	minioMetadata, err := setupMinIOContainer(ctx, network.Name)
	Expect(err).ToNot(HaveOccurred())
	endpoint = minioMetadata.Endpoint
	accessKey = minioMetadata.AccessKey
	secretKey = minioMetadata.SecretKey

	By("setup s3 client")
	endpoint = "http://" + strings.Replace(endpoint, "localhost", "127.0.0.1", 1)
	awsS3Client = awss3.New(awss3.Options{
		Region:       region,
		BaseEndpoint: aws.String(endpoint),
		Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     accessKey,
				SecretAccessKey: secretKey,
			}, nil
		}),
	})
	_, err = awsS3Client.CreateBucket(context.Background(), &awss3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	Expect(err).ToNot(HaveOccurred())

	protocS3Client = s3.NewClient(endpoint, bucketName, region, accessKey, secretKey)
})

type minioMetadata struct {
	Endpoint  string
	AccessKey string
	SecretKey string
}

func setupMinIOContainer(ctx context.Context, network string) (*minioMetadata, error) {
	By("setup minio cluster")
	prefix := gofakeit.Letter() + gofakeit.Password(true, false, true, false, false, 5)
	nameAlias := prefix + "-minio"
	minioContainer, err := minio.Run(
		ctx,
		minioImage,
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        minioImage,
				ExposedPorts: []string{minioPort, minioConsolePort},
				Env: map[string]string{
					"MINIO_ROOT_USER":     minioRootUser,
					"MINIO_ROOT_PASSWORD": minioRootPassword,
				},
				Cmd:            []string{"server", "--console-address", ":" + minioConsolePort, "/data"},
				Name:           nameAlias,
				Networks:       []string{network},
				NetworkAliases: map[string][]string{network: {nameAlias}},
				WaitingFor:     wait.ForListeningPort(minioPort + "/tcp"),
			},
		}),
	)
	if err != nil {
		return nil, err
	}

	endpoint, err := minioContainer.Host(ctx)
	if err != nil {
		return nil, err
	}

	// create user
	accessKey = gofakeit.HexUint(128)[2:]
	secretKey = gofakeit.HexUint(128)[2:]
	_, _, err = minioContainer.Exec(ctx, []string{"mc", "admin", "user", "add", nameAlias, accessKey, secretKey, "--no-color"})
	if err != nil {
		return nil, err
	}
	_, _, err = minioContainer.Exec(ctx, []string{"mc", "admin", "policy", "attach", nameAlias, "readwrite", "--user=" + accessKey, "--no-color"})
	if err != nil {
		return nil, err
	}

	return &minioMetadata{
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
	}, nil
}
