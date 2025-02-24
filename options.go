package fxfer

import (
	"regexp"
	"time"
)

const (
	defaultMaxFileSize      = 5 << 40 // 5 TB
	defaultMinFileSize      = 0
	defaultRefreshInterval  = 1 * time.Second
	defaultMaxRetryAttempts = 5
	defaultInitialDelay     = 1 * time.Second
	defaultMaxDelay         = 30 * time.Second
)

type TransferOption func(*transferer)

// WithMaxFileSize sets the maximum file size allowed for transfer.
// Default is 0 (no limit).
func WithMaxFileSize(size int64) TransferOption {
	if size <= 0 {
		size = defaultMaxFileSize
	}
	return func(t *transferer) {
		t.fileRule.MaxFileSize = size
	}
}

// WithMinFileSize sets the minimum file size required for transfer.
// Default is 0 (no limit).
func WithMinFileSize(size int64) TransferOption {
	if size <= 0 {
		size = defaultMinFileSize
	}
	return func(t *transferer) {
		t.fileRule.MinFileSize = size
	}
}

// WithExtensionWhitelist sets the list of allowed file extensions for transfer.
// Default is empty (no restriction).
func WithExtensionWhitelist(extensions ...string) TransferOption {
	return func(t *transferer) {
		t.fileRule.ExtensionWhitelist = extensions
	}
}

// WithExtensionBlacklist sets the list of blocked file extensions for transfer.
// Default is empty (no restriction).
func WithExtensionBlacklist(extensions ...string) TransferOption {
	return func(t *transferer) {
		t.fileRule.ExtensionBlacklist = extensions
	}
}

// WithModifiedAfter sets the minimum modified time required for transfer.
// Default is zero (no restriction).
func WithModifiedAfter(modTime time.Time) TransferOption {
	return func(t *transferer) {
		t.fileRule.ModifiedAfter = modTime
	}
}

// WithModifiedBefore sets the maximum modified time required for transfer.
// Default is zero (no restriction).
func WithModifiedBefore(modTime time.Time) TransferOption {
	return func(t *transferer) {
		t.fileRule.ModifiedBefore = modTime
	}
}

// WithFileNamePattern sets the regular expression pattern for file names.
// Default is nil (no restriction).
func WithFileNamePattern(pattern *regexp.Regexp) TransferOption {
	return func(t *transferer) {
		t.fileRule.FileNamePattern = pattern
	}
}

// WithProgressRefreshInterval sets the interval for refreshing the progress update.
// Default is 1 second.
func WithProgressRefreshInterval(interval time.Duration) TransferOption {
	if interval <= 0 {
		interval = defaultRefreshInterval
	}
	return func(t *transferer) {
		t.refreshProgressInterval = interval
	}
}

// ChecksumAlgorithm defines the supported checksum algorithms for file transfer validation.
type ChecksumAlgorithm int

const (
	// NoneChecksumAlgorithm is the default checksum algorithm.
	NoneChecksumAlgorithm ChecksumAlgorithm = iota
	// ChecksumAlgorithmCRC32 is the cyclic redundancy check (CRC32) checksum algorithm.
	// It is the default checksum algorithm, suitable for large files (> 1GB, fast, less secure).
	ChecksumAlgorithmCRC32
	// ChecksumAlgorithmMD5 is the message-digest algorithm 5 (MD5) checksum algorithm.
	// It is suitable for general files (< 1GB, moderate speed, more secure)
	ChecksumAlgorithmMD5
	// ChecksumAlgorithmSHA256 is the secure hash algorithm 256 (SHA-256) checksum algorithm.
	// It is suitable for sensitive files (slow, most secure)
	ChecksumAlgorithmSHA256
)

// WithChecksumAlgorithm sets the checksum algorithm for the transferer.
// It is recommended to use the default checksum algorithm (CRC32) unless
// there is a specific requirement for a different algorithm.
func WithChecksumAlgorithm(algorithm ChecksumAlgorithm) TransferOption {
	// TODO: implement checksum algorithm
	return func(t *transferer) {
		t.checksumAlgorithm = algorithm
	}
}

// WithDisabledRetry disables the retry mechanism for the transferer.
// Default is false (enabled). If disabled, the transferer will not
// retry failed transfers, regardless of setting WithRetryConfig option.
func WithDisabledRetry() TransferOption {
	return func(t *transferer) {
		t.disabledRetry = true
	}
}

// RetryConfig defines the retry configuration for the transferer.
type RetryConfig struct {
	// MaxRetryAttempts is the maximum number of retry attempts, default = 5.
	MaxRetryAttempts int
	// InitialDelay is the initial delay before the first retry, default = 1 second.
	InitialDelay time.Duration
	// MaxDelay is the maximum delay between retries, default = 30 seconds.
	MaxDelay time.Duration
}

// WithRetryConfig sets the retry configuration for the transferer.
// Support partial configuration, default values will be used if not set.
func WithRetryConfig(config RetryConfig) TransferOption {
	if config.MaxRetryAttempts <= 0 {
		config.MaxRetryAttempts = defaultMaxRetryAttempts
	}
	if config.InitialDelay <= 0 {
		config.InitialDelay = defaultInitialDelay
	}
	if config.MaxDelay <= 0 {
		config.MaxDelay = defaultMaxDelay
	}
	return func(t *transferer) {
		t.retryConfig = config
	}
}
