package protocutils

import (
	"fmt"
	"strings"
)

// BuildAddress builds the address based on the provided host and port.
// If port is greater than 0, it overrides any port specified in the host.
// If host contains a port and port parameter is 0, the host's port is used.
// Examples:
//   - BuildAddress("example.com", 22) -> "example.com:22"
//   - BuildAddress("example.com:2222", 0) -> "example.com:2222"
//   - BuildAddress("example.com:2222", 22) -> "example.com:22"
func BuildAddress(host string, port int) (addr string) {
	if host == "" {
		return
	}

	hostParts := strings.Split(host, ":")
	if port > 0 {
		addr = fmt.Sprintf("%s:%d", hostParts[0], port)
		return
	}

	addr = host
	return
}
