// Copyright 2026 Sorrell Consulting, LLC
// SPDX-License-Identifier: Apache-2.0

package showcase

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Every signal targets a process started and joined by this runner. Completed
// commands remain in the list so cleanup never has to discover processes.
type process struct {
	name        string
	cmd         *exec.Cmd
	done        chan struct{}
	err         error // read only after done closes
	logCloseErr error // read only after done closes
	stopped     bool
	finite      bool
}

func (r *runner) start(name, binary string, finite bool, args ...string) (*process, error) {
	log, err := os.OpenFile(filepath.Join(r.report.Directory, name+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	// The unique application name connects an observed PostgreSQL backend to
	// this exact child. Do not write connection strings into artifacts.
	app := r.report.Namespace + "-" + name
	dsn := r.options.DatabaseURL
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			log.Close()
			return nil, err
		}
		q := u.Query()
		q.Set("application_name", app)
		u.RawQuery = q.Encode()
		dsn = u.String()
	} else {
		dsn += " application_name='" + app + "'"
	}
	p := &process{name: name, cmd: exec.Command(binary, args...), done: make(chan struct{}), finite: finite}
	p.cmd.Env = append(os.Environ(), "AMENDS_DATABASE_URL="+dsn)
	p.cmd.Stdout, p.cmd.Stderr = log, log
	if err = p.cmd.Start(); err != nil {
		log.Close()
		return nil, err
	}
	r.children = append(r.children, p)
	r.report.Processes = append(r.report.Processes, Process{Name: name, PID: p.cmd.Process.Pid, ApplicationName: app})
	go func() {
		waitErr := p.cmd.Wait()
		p.logCloseErr = log.Close()
		p.err = errors.Join(waitErr, p.logCloseErr)
		close(p.done)
	}()
	return p, nil
}

func (p *process) wait(ctx context.Context) error {
	select {
	case <-p.done:
		return p.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *process) stop() error {
	if p.stopped {
		return nil
	}
	p.stopped = true
	select {
	case <-p.done:
		return fmt.Errorf("%s exited before shutdown: %w", p.name, p.err)
	default:
	}
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.wait(ctx); err != nil {
		// Forced cleanup is an error, never a successful relay handoff.
		killErr := p.cmd.Process.Kill()
		<-p.done
		return fmt.Errorf("%s did not stop cleanly: %w", p.name, errors.Join(err, killErr))
	}
	return nil
}

func (p *process) kill() error {
	if err := p.cmd.Process.Kill(); err != nil {
		return err
	}
	p.stopped = true
	<-p.done
	status, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		return fmt.Errorf("%s did not exit through SIGKILL: %v", p.name, p.err)
	}
	return nil
}

func (r *runner) healthy() error {
	for _, p := range r.children {
		if p.stopped || p.finite {
			continue
		}
		select {
		case <-p.done:
			return fmt.Errorf("%s exited unexpectedly (see its log): %v", p.name, p.err)
		default:
		}
	}
	return nil
}

func (r *runner) cleanup() error {
	var result error
	for i := len(r.children) - 1; i >= 0; i-- {
		p := r.children[i]
		if p.finite {
			select {
			case <-p.done:
				continue
			default:
			}
		}
		result = errors.Join(result, p.stop())
	}
	return result
}

// expectExit permits this owned service to exit during trigger, then joins and
// checks its actual exit. A failed trigger or cancellation still leaves cleanup
// responsible for any live child.
func (p *process) expectExit(ctx context.Context, code int, trigger func() error) error {
	wasFinite := p.finite
	p.finite = true
	defer func() { p.finite = wasFinite }()
	if err := trigger(); err != nil {
		return err
	}
	err := p.wait(ctx)
	select {
	case <-p.done:
		p.stopped = true
	default:
		return err
	}
	var exitError *exec.ExitError
	if code == 0 && err == nil {
		return nil
	}
	if errors.As(err, &exitError) && exitError.ExitCode() == code {
		return p.logCloseErr
	}
	return fmt.Errorf("%s: expected exit %d, got %v", p.name, code, err)
}
