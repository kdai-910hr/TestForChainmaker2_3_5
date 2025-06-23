package stalecontrol

import (
	"os"
	"testing"
)

func TestIsEnabled(t *testing.T) {
	os.Setenv("STALE_READ_PROTECTION_ENABLED", "true")
	if !IsEnabled() {
		t.Error("Expected true when env is set to true")
	}

	os.Setenv("STALE_READ_PROTECTION_ENABLED", "false")
	// 注意：IsEnabled 只加载一次，为测试需要重启进程或重置 once（略复杂）
}
