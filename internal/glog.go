package internal

import (
	"log/slog"
)

// GoroutineLogger returns a child logger tagged with the current goroutine ID.
func GoroutineLogger(id int) *slog.Logger {
	return slog.Default().With("goroutine", id)
}
