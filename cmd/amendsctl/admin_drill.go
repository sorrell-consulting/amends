package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/sorrell-consulting/amends/internal/showcase"
)

func runAdminDrill(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("drill", flag.ContinueOnError)
	flags.SetOutput(stderr)
	name := flags.String("name", "", "replay-intact, full-rebuild, poison-event, source-conflict, relay-stall, disappearing-day, materializer-crash, duplicate-flood, stale-amendment-storm, history-missing, zombie-worker, ambiguous-commit, or rebalance-under-load (required; history-missing removes a prefix only from its fresh owned topic)")
	brokers := flags.String("brokers", "127.0.0.1:19092", "comma-separated broker endpoints")
	artifacts := flags.String("artifacts", "artifacts/operations", "parent directory for retained drill evidence")
	timeout := flags.Duration("timeout", 3*time.Minute, "overall drill deadline")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	supported := false
	switch *name {
	case "replay-intact", "full-rebuild", "poison-event", "source-conflict", "relay-stall", "disappearing-day", "materializer-crash", "duplicate-flood", "stale-amendment-storm", "history-missing", "zombie-worker", "ambiguous-commit", "rebalance-under-load":
		supported = true
	}
	if flags.NArg() != 0 || !supported || *artifacts == "" || *timeout <= 0 || os.Getenv("AMENDS_DATABASE_URL") == "" {
		fmt.Fprintln(stderr, "drill requires a supported -name, AMENDS_DATABASE_URL, and valid options")
		return 2
	}
	parent, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(parent, *timeout)
	defer cancel()
	err := func() error {
		binary, err := os.Executable()
		if err != nil {
			return err
		}
		seeds := strings.Split(*brokers, ",")
		options := showcase.Options{DatabaseURL: os.Getenv("AMENDS_DATABASE_URL"), Brokers: seeds, ControlBinary: binary, WorkerBinary: filepath.Join(filepath.Dir(binary), "amends"), ArtifactParent: *artifacts, Output: stdout}
		switch *name {
		case "ambiguous-commit":
			for _, scenario := range []showcase.Scenario{showcase.AmbiguousRequest, showcase.AmbiguousReply} {
				options.Scenario = scenario
				result, err := showcase.Run(ctx, options)
				if err != nil {
					return err
				}
				loss := strings.TrimPrefix(string(scenario), "ambiguous-commit-")
				if _, err = fmt.Fprintf(stdout, "ambiguous-commit %s-loss assertions PASS; final verifier %s.\nArtifacts: %s\n", loss, result.FinalVerification, result.Directory); err != nil {
					return err
				}
			}
			return nil
		case "replay-intact", "full-rebuild":
			_, err := showcase.RunAdministrative(ctx, options, *name)
			return err
		default:
			options.Scenario = showcase.Scenario(*name)
			result, err := showcase.Run(ctx, options)
			if err != nil {
				return err
			}
			qualification := ""
			if *name == "poison-event" || *name == "source-conflict" || *name == "history-missing" {
				qualification = ", not strict PASS"
			}
			_, err = fmt.Fprintf(stdout, "%s assertions PASS; final verifier %s%s.\nArtifacts: %s\n", *name, result.FinalVerification, qualification, result.Directory)
			return err
		}

	}()
	if err != nil {
		fmt.Fprintf(stderr, "drill failed: %v\n", err)
		return 1
	}
	return 0
}
