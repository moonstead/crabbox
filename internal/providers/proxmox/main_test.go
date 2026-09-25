package proxmox

import (
	"os"
	"strings"
	"testing"

	"github.com/openclaw/crabbox/internal/testutil"
)

func TestMain(m *testing.M) {
	if path := os.Getenv(adapterChildStateEnv); path != "" && len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-test.") {
		os.Exit(runAdapterChild(path, os.Args[1:]))
	}
	os.Exit(testutil.RunWithIsolatedUserDirs(m))
}
