package showcase

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

func (runner *runner) inspectStage(ctx context.Context, name string) error {
	manifest := filepath.Join(runner.report.Directory, "manifest.json")
	if err := runner.command(ctx, "inspect-"+name, "inspect", "-manifest", manifest); err != nil {
		return err
	}
	if err := runner.command(ctx, "dashboard-"+name, "inspect", "-manifest", manifest, "-format", "html"); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(runner.report.Directory, "dashboard-"+name+".log"), filepath.Join(runner.report.Directory, "dashboard-"+name+".html")); err != nil {
		return err
	}
	for _, kind := range []string{"quarantine", "conflicts"} {
		if err := ctx.Err(); err != nil {
			return err
		}
		child, err := runner.start(kind+"-"+name, runner.options.ControlBinary, true, kind, "list", "-manifest", manifest)
		if err != nil {
			return err
		}
		if err = child.wait(ctx); err != nil {
			return fmt.Errorf("%s review failed (see %s-%s.log): %w", kind, kind, name, err)
		}
	}
	return nil
}
