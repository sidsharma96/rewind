// Command rewind is the entry point for the Rewind trace-store project.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "rewind:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	if len(args) == 0 || (len(args) == 1 && (args[0] == "help" || args[0] == "--help" || args[0] == "-h")) {
		_, err := fmt.Fprintln(out, "Usage: rewind version\n\nRewind is a trace-store project. Storage and replay are not implemented yet.")
		return err
	}
	if len(args) == 1 && args[0] == "version" {
		_, err := fmt.Fprintln(out, "rewind dev")
		return err
	}
	return errors.New("unknown command or arguments; use rewind --help")
}
