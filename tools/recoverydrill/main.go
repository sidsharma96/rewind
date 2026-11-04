// recoverydrill is an operator-only synthetic write/read/host-replacement driver.
// It exposes no receiver and never creates or deletes cloud infrastructure.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out, diagnostics io.Writer) error {
	flags := flag.NewFlagSet("recoverydrill", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	phase := flags.String("phase", "", "prepare, setup, write, read or retry")
	path := flags.String("config", "", "protected private configuration file")
	dir := flags.String("out-dir", "", "new protected prepare output directory")
	withhold := flags.Bool("withhold-final-ack", false, "withhold final application acknowledgment")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *path == "" || *phase != "prepare" && *dir != "" || *withhold && *phase != "write" {
		return errors.New("recoverydrill: invalid command arguments")
	}
	ctx, cancel := context.WithTimeout(ctx, phaseTimeout)
	defer cancel()
	switch *phase {
	case "prepare":
		var cfg baseConfig
		if err := loadConfig(*path, &cfg); err != nil {
			return err
		}
		if err := prepare(ctx, cfg, *dir); err != nil {
			return err
		}
		_, err := fmt.Fprintln(out, `{"type":"prepared"}`)
		return err
	case "setup":
		var cfg setupConfig
		if loadConfig(*path, &cfg) != nil || !cfg.valid() {
			return errConfig
		}
		pool, _, err := openCatalog(ctx, cfg.deployment)
		if err != nil {
			return err
		}
		defer pool.Close()
		if err := setup(ctx, cfg, pool); err != nil {
			return err
		}
		_, err = fmt.Fprintln(out, `{"type":"setup"}`)
		return err
	case "write", "retry":
		var cfg writeConfig
		if loadConfig(*path, &cfg) != nil || !cfg.valid() {
			return errConfig
		}
		pool, c, err := openCatalog(ctx, cfg.deployment)
		if err != nil {
			return err
		}
		defer pool.Close()
		threshold := 32768
		if *phase == "retry" {
			threshold = 1
		}
		s, closeObjects, err := liveSession(c, cfg.deployment, cfg.Credentials, threshold)
		if err != nil {
			return err
		}
		defer closeObjects()
		return write(ctx, cfg, s, out, diagnostics, *withhold, *phase == "retry")
	case "read":
		var cfg readConfig
		if loadConfig(*path, &cfg) != nil || !cfg.valid() {
			return errConfig
		}
		pool, c, err := openCatalog(ctx, cfg.deployment)
		if err != nil {
			return err
		}
		defer pool.Close()
		s, closeObjects, err := liveSession(c, cfg.deployment, cfg.Credentials, 32768)
		if err != nil {
			return err
		}
		defer closeObjects()
		return read(ctx, cfg, s, out)
	default:
		return errors.New("recoverydrill: unknown phase")
	}
}
