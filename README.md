# fxfer

## Motivation

Imagine managing files and objects stored across various systems—whether it's a NAS, your local machine, or a cloud
storage service like S3, GCS, or Azure Blob Storage. We refer to these as "source storage." Now, suppose you need to
seamlessly transfer these files to another storage system, which we call "destination storage", regardless of the
unreliability of the network or the large size of the files. How would you go about doing this?

This library is designed to make such transfers effortless, resumable, and reliable. Built with extensibility in
mind, it allows you to easily add support for new source or destination storage, empowering you to handle diverse
storage solutions with ease.

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
metadata, additional information. This file SHOULD be put on *Destination Storage* when starting a transfer process.

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
