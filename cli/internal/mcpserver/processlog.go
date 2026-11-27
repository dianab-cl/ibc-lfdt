// SPDX-License-Identifier: Apache-2.0

package mcpserver

import (
	"log"
	"log/slog"
	"strings"
)

// processLogWriter turns one standard-log write into relayer log records,
// one per line, so a recovered panic stack is kept.
type processLogWriter struct {
	logger *slog.Logger
}

// processErrorLog routes net/http's recovered panics and accept errors to logger
// instead of the standard log package.
func processErrorLog(logger *slog.Logger) *log.Logger {
	if logger == nil {
		logger = slog.Default()
	}
	return log.New(processLogWriter{logger: logger}, "", 0)
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
