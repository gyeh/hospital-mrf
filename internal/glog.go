package internal

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
)

// goid returns the current goroutine's ID.
func goid() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// Format: "goroutine 123 [running]:\n..."
	s := string(buf[:n])
	s = strings.TrimPrefix(s, "goroutine ")
	s = s[:strings.IndexByte(s, ' ')]
	id, _ := strconv.ParseUint(s, 10, 64)
	return id
}

// GoPrefix returns a "[G<id>] " prefix for the current goroutine.
func GoPrefix() string {
	return fmt.Sprintf("[G%d] ", goid())
}

// Pprintf prints to stdout with the given prefix.
func Pprintf(prefix, format string, args ...any) {
	fmt.Printf("%s%s", prefix, fmt.Sprintf(format, args...))
}

// pprintln prints a blank line to stdout with the given prefix.
func pprintln(prefix string) {
	fmt.Printf("%s\n", prefix)
}
