package io_examples

import (
	"fmt"

	streams "github.com/ilxqx/go-streams"
)

// Example: FromStringLines.
func Example_lines() {
	lines := streams.FromStringLines("a\nb\n").Collect()
	fmt.Println(lines)
	// Output:
	// [a b]
}

