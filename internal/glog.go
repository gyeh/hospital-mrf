package internal

import (
	"fmt"
	"log/slog"
)

// EntryLogger returns a child logger tagged with the entry count (1-based index and total).
func EntryLogger(index, total int) *slog.Logger {
	return slog.Default().With("entry", fmt.Sprintf("%d/%d", index, total))
}
