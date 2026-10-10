package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sorrell-consulting/amends/internal/admin"
	"github.com/sorrell-consulting/amends/internal/broker"
	"github.com/sorrell-consulting/amends/internal/verify"
)

func runAdmin(command string, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("manifest", "", "existing source/pipeline manifest (required)")
	brokers := flags.String("brokers", "127.0.0.1:19092", "comma-separated broker endpoints")
	timeout := flags.Duration("timeout", 2*time.Minute, "bounded operation time")
	execute := flags.Bool("execute", false, "execute the checked operation; omitted means dry run")
	quiesce := flags.Bool("quiesce", false, "confirm source loaders and ordinary workers are stopped and joined")
	var partition *int
	var from, until *int64
	var namespace, outputTopic, artifacts, expected *string
	if command == "replay" {
		partition = flags.Int("partition", -1, "source partition (required)")
		from = flags.Int64("from-offset", -1, "inclusive retained, already-consumed offset (required)")
		until = flags.Int64("until-offset", -1, "exclusive already-consumed offset (required)")
	} else {
		namespace = flags.String("new-namespace", "", "unused destination namespace (required)")
		outputTopic = flags.String("output-topic", "", "new output topic (required)")
		artifacts = flags.String("artifacts", "artifacts/rebuild", "parent directory for retained manifest and report")
		expected = flags.String("expected-diagnostics", "", "exact named diagnostics fixture for a qualified verification result")
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	invalid := flags.NArg() != 0 || *path == "" || *timeout <= 0 || os.Getenv("AMENDS_DATABASE_URL") == "" || (*execute && !*quiesce)
	if command == "replay" {
		invalid = invalid || *partition < 0 || *from < 0 || *until <= *from
	} else {
		invalid = invalid || *namespace == "" || *outputTopic == "" || *artifacts == ""
	}
	if invalid {
		fmt.Fprintln(stderr, "requires -manifest, AMENDS_DATABASE_URL, valid range/destination options, and --quiesce with --execute")
		return 2
	}
	code := 1
	err := func() error {
		manifest, err := readAdminManifest(*path)
		if err != nil {
			return err
		}
		var fixture *verify.ExpectedDiagnostics
		if expected != nil && *expected != "" {
			file, err := os.Open(*expected)
			if err != nil {
				return err
			}
			fixture, err = verify.ReadExpected(file)
			err = errors.Join(err, file.Close())
			if err != nil {
				return err
			}
		}
		parent, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		ctx, cancel := context.WithTimeout(parent, *timeout)
		defer cancel()
		pool, err := pgxpool.New(ctx, os.Getenv("AMENDS_DATABASE_URL"))
		if err != nil {
			return err
		}
		defer pool.Close()
		options := admin.Options{Brokers: strings.Split(*brokers, ","), Manifest: manifest, Pool: pool, Execute: *execute, Quiesce: *quiesce}
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		if command == "replay" {
			report, operationErr := admin.Replay(ctx, options, *partition, *from, *until)
			if err = encoder.Encode(report); err != nil {
				return errors.Join(operationErr, err)
			}
			if operationErr == nil {
				code = 0
			}
			return operationErr
		}
		var directory string
		if *execute {
			if err = os.MkdirAll(*artifacts, 0755); err != nil {
				return err
			}
			directory, err = os.MkdirTemp(*artifacts, "run-")
			if err != nil {
				return err
			}
			if _, err = fmt.Fprintf(stderr, "Rebuild artifacts: %s\n", directory); err != nil {
				return err
			}
		}
		report, operationErr := admin.Rebuild(ctx, options, *namespace, *outputTopic, func(manifest broker.Manifest) error {
			return saveAdminJSON(filepath.Join(directory, "manifest.json"), manifest)
		})
		if report.Verification != nil {
			executable, hashErr := os.Executable()
			if hashErr == nil {
				var raw []byte
				raw, hashErr = os.ReadFile(executable)
				if hashErr == nil {
					sum := sha256.Sum256(raw)
					report.Verification.BinarySHA256 = hex.EncodeToString(sum[:])
				}
			}
			operationErr = errors.Join(operationErr, hashErr)
		}
		accepted := report.Status == "DRY_RUN" || (report.Verification != nil && report.Verification.Accepts(fixture))
		if directory != "" {
			operationErr = errors.Join(operationErr, saveAdminJSON(filepath.Join(directory, "report.json"), report))
		}
		if err = encoder.Encode(report); err != nil {
			return errors.Join(operationErr, err)
		}
		if operationErr == nil && accepted {
			code = 0
		}
		return operationErr
	}()
	if err != nil {
		fmt.Fprintf(stderr, "amendsctl %s: %v\n", command, err)
		return 1
	}
	return code
}

func readAdminManifest(path string) (broker.Manifest, error) {
	file, err := os.Open(path)
	if err != nil {
		return broker.Manifest{}, err
	}
	manifest, err := broker.ReadManifest(file)
	return manifest, errors.Join(err, file.Close())
}

func saveAdminJSON(path string, value any) error {
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if err = os.WriteFile(path+".tmp", append(raw, '\n'), 0600); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
}
