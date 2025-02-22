package protocutils

import (
	"fmt"
	"strings"
)

// BuildAddress builds the address based on the provided host and port.
// If the port is not provided, the host is returned.
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
