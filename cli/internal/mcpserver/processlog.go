// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"log"
	"log/slog"
	"strings"
)

// processErrorLog is the http.Server error log. net/http uses it for panics
// it recovers and for failures while accepting connections. A nil ErrorLog
// uses the standard log package, which is not the relayer logger.
func processErrorLog(logger *slog.Logger) *log.Logger {
	if logger == nil {
		logger = slog.Default()
	}
	return log.New(processLogWriter{logger: logger}, "", 0)
}

// processLogWriter turns one standard-log write into relayer log records,
// one per line, so a recovered panic stack is kept.
type processLogWriter struct {
	logger *slog.Logger
}

func (w processLogWriter) Write(p []byte) (int, error) {
	text := strings.TrimRight(string(p), "\r\n")
	if text == "" {
		if len(p) > 0 {
			w.logger.Error("")
		}
		return len(p), nil
	}
	for _, line := range strings.Split(text, "\n") {
		w.logger.Error(strings.TrimRight(line, "\r"))
	}
	return len(p), nil
}
