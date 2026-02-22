package framework

import (
	"crypto/rand"
	"fmt"
	"os"
	"plugin-sdk"
)

func generateUUID() sdk.UUID {
	b := make([]byte, 16)
	rand.Read(b)
	return sdk.UUID(fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]))
}

func SafeRun(bundleID sdk.UUID, label string, fn func()) {
	defer func() {
		if r := recover(); r != nil {
			bundlesMu.RLock()
			b := bundles[bundleID]
			bundlesMu.RUnlock()
			errMsg := fmt.Sprintf("PANIC RECOVERED in [%s]: %v", label, r)
			if b != nil {
				b.Log().Error(errMsg)
			} else {
				fmt.Fprintf(os.Stderr, "[%s] %s\n", bundleID, errMsg)
			}
		}
	}()
	fn()
}
