package framework

import (
	"fmt"
	"os"
	"path/filepath"
	"github.com/slidebolt/plugin-sdk"
	"time"
)

type loggerImpl struct {
	bundleID sdk.UUID
	logPath  string
}

func (l *loggerImpl) log(level, format string, args ...interface{}) {
	timestamp := time.Now().Format(time.RFC3339)
	msg := fmt.Sprintf(format, args...)
	line := fmt.Sprintf("[%s] [%s] [%s] %s\n", timestamp, level, l.bundleID, msg)
	fmt.Print(line)
	f, err := os.OpenFile(filepath.Join(l.logPath, "current.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err == nil {
		defer f.Close()
		f.WriteString(line)
	}
}

func (l *loggerImpl) Info(f string, a ...interface{})  { l.log("INFO", f, a...) }
func (l *loggerImpl) Error(f string, a ...interface{}) { l.log("ERROR", f, a...) }
func (l *loggerImpl) Debug(f string, a ...interface{}) { l.log("DEBUG", f, a...) }
