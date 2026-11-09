// Command vmrebalance moves a VictoriaMetrics cluster's data to a new cluster with more vmstorage nodes.
package main

import (
	"context"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/cli"
)

// version is the release version, set at build time with -ldflags "-X main.version=...".
var version string

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := cli.Execute(ctx, versionString())
	stop()
	os.Exit(code)
}

// versionString is the release version, else the module version Go records for `go install`, else "dev".
func versionString() string {
	if version != "" {
		return version
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return bi.Main.Version
	}
	return "dev"
}
