// Command rewind serves authenticated shared traces and recorded results.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := runContext(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "rewind:", err)
		os.Exit(1)
	}
}
func run(args []string, out io.Writer) error { return runContext(context.Background(), args, out) }
func runContext(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 || len(args) == 1 && (args[0] == "help" || args[0] == "--help" || args[0] == "-h") {
		_, err := fmt.Fprintln(out, `Usage: rewind COMMAND [flags]

serve --config FILE                         Start the TLS service and browser
install --config FILE                       Install a new catalog schema (operator only)
token --out FILE                            Generate a private credential; print its digest
register --endpoint URL --token-file FILE --project PROJECT --trace ID --span ID --run RUN --instance INSTANCE
save-span|save-capture ... --file FILE       Save one ended span or independent capture
trace ... --trace ID [--cursor POSITION]     Inspect one current trace page
capture|playback ... --trace ID --span ID    Inspect capture or retrieve saved response
diagnose ... --trace ID [--max-spans N]     Print every readable span and capture as evidence text
diagnose --rules                           Print the root-cause rules for that evidence
playback ... --out FILE                     Save exact response to a new private file
version                                    Print the development version

Clients use HTTPS and separate producer/reader credentials. Use --ca-file for a private CA.
Accepted records require remote catalog/object storage; a trace page is not a complete run.`)
		return err
	}
	if len(args) == 1 && args[0] == "version" {
		_, err := fmt.Fprintln(out, "rewind dev")
		return err
	}
	if len(args) > 0 {
		switch args[0] {
		case "serve", "install":
			return serverCommand(ctx, args[0], args[1:], out)
		case "token":
			return tokenCommand(args[1:], out)
		case "diagnose":
			return diagnoseCommand(ctx, args[1:], out)
		case "register", "save-span", "save-capture", "trace", "capture", "playback":
			return clientCommand(ctx, args[0], args[1:], out)
		}
	}
	return errors.New("unknown command or arguments; use rewind help")
}
