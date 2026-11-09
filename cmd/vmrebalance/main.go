// Command vmrebalance moves a VictoriaMetrics cluster's data to a new cluster with more vmstorage nodes.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/kokuwaio/victoriametrics-rebalance/internal/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := cli.Execute(ctx)
	stop()
	os.Exit(code)
}
