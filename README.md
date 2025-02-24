# fxfer

## Table of Contents

- [Motivation](#motivation)
- [Quick Start](#quick-start)
- [Concepts](#concepts)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Roadmap](#roadmap)
- [Acknowledgements](#acknowledgements)

## Motivation

Imagine managing files and objects stored across various systems—whether it's a NAS, your local machine, or a cloud
storage service like S3, GCS, or Azure Blob Storage. We refer to these as "source storage." Now, suppose you need to
seamlessly transfer these files to another storage system, which we call "destination storage", regardless of the
unreliability of the network or the large size of the files. How would you go about doing this?

This library is designed to make such transfers effortless, resumable, and reliable. Built with extensibility in
mind, it allows you to easily add support for new source or destination storage, empowering you to handle diverse
storage solutions with ease.

## Installation

To install `fxfer`, use `go get`:

```bash
go get github.com/derektruong/fxfer
```

Next include `fxfer` in your application.

```bash
import "github.com/derektruong/fxfer"
```

## Concepts

### Storage

Place where files are processed and transferred. There are two types of storage:

- **Source Storage**: The storage where your files are stored, so you SHOULD have `read` access to this storage.
- **Destination Storage**: The storage where you want to transfer your files to, so you SHOULD have both `read` and
    `write` access to this storage.

### Client

Between the source storage and the destination storage, the protocol is used to transfer the files. The protocol
could be `FTP`, `SFTP`, `S3`, ...

### Transfer file

The file that persists the information about the file you want to transfer. It contains the file's name, size, and other
metadata, additional information. This file SHOULD be put on _Destination Storage_ when starting a transfer process.

## Features

- 📂 Transfer files seamlessly between source storage and destination storage.
- 🌐 Supports a variety of storages: local filesystem, (S)FTP server, and cloud platforms (e.g., S3, ...).
- 🛠️ Easily extensible to include new storage types.
- 🚀 Optimized for performance and reliability.
- 📶 All connections are employed connection pooling to reduce latency.
- ⏭️ Supports resuming interrupted transfers (ideal for large files or unreliable connections).
- 🖲️ Support tracking the transfer progress.

## Roadmap

### Support storages

- [ ] Support local filesystem storage.
- [x] Support S3 storage, S3-compatible storage (e.g., MinIO, Ceph, Storj, ...).
- [ ] Support FTP, SFTP server storage.
- [ ] Support Azure Blob Storage.
- [ ] Support Google Cloud Storage.

### Resilience and performance

- [x] Support resuming interrupted transfers.
- [x] Support transfer progress tracking.
- [x] Support exponential backoff for retrying failed transfers automatically.
- [ ] Support checksum verification.
- [ ] Support compression during transfer.

## Usage

### Real-world examples

Please refer to the [examples](./examples) directory for detailed usage instructions.

### Quick Start

```go
func main() {
	// setup clients
	srcClient := s3protoc.NewClient(s3Endpoint, s3Bucket, s3Region, s3AccessKey, s3SecretKey)
	destClient := ftpprotoc.NewClient(ftpHost, ftpPort, ftpUser, ftpPassword)

	// setup storages
	srcStorage := s3.NewSource(logger)
	destStorage := ftp.NewDestination(logger)

	transferer := fxfer.NewTransferer(logger, fxfer.WithMaxFileSize(5<<40)) // 5TB
	err = transferer.Transfer(
		ctx,
		fxfer.SourceCommand{
			FilePath: "path/to/source/file",
			Storage:  srcStorage,
			Client:   srcClient,
		},
		fxfer.DestinationCommand{
			FilePath: "path/to/destination/file",
			Storage:  destStorage,
			Client:   destClient,
		},
		examples.HandleProgressUpdated(logger),
	)
	if err != nil {
		log.Fatal(err)
	}
}
```

### Taskfile

**Note**: Put `.env` file in the `examples` with corresponding credentials for each
storage type.

If you have [Taskfile](https://taskfile.dev) installed, you can run the examples very easily.
See [./Taskfile.yaml](./Taskfile.yaml) for more details.

## Acknowledgements

This project builds upon several excellent open-source libraries:

- [logr](https://github.com/go-logr/logr) - Provides the logging interface that enables easy integration with various logging backends (slog, zap, etc.)
- [ftp](https://github.com/jlaffaye/ftp) and [sftp](github.com/pkg/sftp) - Power our FTP and SFTP protocol support
- [aws-sdk-go-v2](github.com/aws/aws-sdk-go-v2) - Handles S3 operations with AWS's official SDK
- [tusd](github.com/tus/tusd) - Inspired our resumable transfer implementation for S3 storage.
