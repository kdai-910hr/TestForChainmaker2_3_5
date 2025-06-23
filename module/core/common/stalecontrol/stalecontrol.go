package stalecontrol

import (
	"os"
	"strings"
	"sync"
)

var (
	once    sync.Once
	enabled bool
)

func IsEnabled() bool {
	once.Do(func() {
		enabled = strings.ToLower(os.Getenv("STALE_READ_PROTECTION_ENABLED")) == "true"
	})
	return enabled
}
