// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"
)

// redirectMCPStdout copies every byte written to os.Stdout onto logger until
// restore runs. The MCP Go SDK stdio transport writes protocol frames to
// os.Stdout, and fmt.Print does too. logger is the relayer logger: its handler
// writes the process stderr a container collects. Lines are handed to that
// handler directly so logging.level cannot drop them. Restore flushes a
// trailing partial line and puts the previous stdout back.
func redirectMCPStdout(logger *slog.Logger) (restore func(), err error) {
	if logger == nil {
		logger = slog.Default()
	}
	prev := os.Stdout
	read, write, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("redirect MCP stdout: %w", err)
	}
	os.Stdout = write

	done := make(chan struct{})
	go func() {
		defer close(done)
		forwardStdout(read, logger)
	}()

	var once sync.Once
	return func() {
		once.Do(func() {
			_ = write.Close()
			<-done
			os.Stdout = prev
		})
	}, nil
}

// forwardStdout emits each line, including a final line with no newline.
func forwardStdout(read *os.File, logger *slog.Logger) {
	defer read.Close()
	reader := bufio.NewReader(read)
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			writeStdoutLine(logger, strings.TrimRight(line, "\r\n"))
		}
		if err != nil {
			return
		}
	}
}

// writeStdoutLine records one stdout line on the relayer handler. Handler.Handle
// does not consult logging.level, so the line is not filtered. A handler that
// refuses the record is written to stderr as itself, which is the same stream.
func writeStdoutLine(logger *slog.Logger, line string) {
	handler := logger.Handler()
	if handler == nil {
		_, _ = io.WriteString(os.Stderr, line+"\n")
		return
	}
	rec := slog.NewRecord(time.Now(), slog.LevelInfo, line, 0)
	if err := handler.Handle(context.Background(), rec); err != nil {
		_, _ = io.WriteString(os.Stderr, line+"\n")
	}
}
