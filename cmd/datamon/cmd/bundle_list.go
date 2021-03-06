package cmd

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/oneconcern/datamon/pkg/core"
	"github.com/oneconcern/datamon/pkg/model"

	"github.com/spf13/cobra"
)

func applyBundleTemplate(bundle model.BundleDescriptor) error {
	var buf bytes.Buffer
	if err := bundleDescriptorTemplate(datamonFlags).Execute(&buf, bundle); err != nil {
		// NOTE(frederic): to be discussed - PR#267 introduced a change here
		// by stopping upon errors while it was previously non-blocking
		return fmt.Errorf("executing template: %w", err)
	}
	log.Println(buf.String())
	return nil
}

// BundleListCommand describes the CLI command for listing bundles
var BundleListCommand = &cobra.Command{
	Use:   "list",
	Short: "List bundles",
	Long: `List the bundles in a repo, ordered by their bundle ID.

This is analogous to the "git log" command. The bundle ID works like a git commit hash.`,
	Example: `% datamon bundle list --repo ritesh-test-repo
1INzQ5TV4vAAfU2PbRFgPfnzEwR , 2019-03-12 22:10:24.159704 -0700 PDT , Updating test bundle`,
	Run: func(cmd *cobra.Command, args []string) {
		var err error

		defer func(t0 time.Time) {
			cliUsage(t0, "bundle list", err)
		}(time.Now())

		ctx := context.Background()
		datamonFlagsPtr := &datamonFlags
		optionInputs := newCliOptionInputs(config, datamonFlagsPtr)
		remoteStores, err := optionInputs.datamonContext(ctx)
		if err != nil {
			wrapFatalln("create remote stores", err)
			return
		}
		err = core.ListBundlesApply(datamonFlags.repo.RepoName, remoteStores, applyBundleTemplate,
			core.ConcurrentList(datamonFlags.core.ConcurrencyFactor),
			core.BatchSize(datamonFlags.core.BatchSize),
			core.WithMetrics(datamonFlags.root.metrics.IsEnabled()),
		)
		if err != nil {
			wrapFatalln("concurrent list bundles", err)
			return
		}
	},
	PreRun: func(cmd *cobra.Command, args []string) {
		if err := newCliOptionInputs(config, &datamonFlags).populateRemoteConfig(); err != nil {
			wrapFatalln("populate remote config", err)
		}
	},
}

func init() {
	requireFlags(BundleListCommand,
		addRepoNameOptionFlag(BundleListCommand),
	)

	addCoreConcurrencyFactorFlag(BundleListCommand, 500)
	addBatchSizeFlag(BundleListCommand)

	bundleCmd.AddCommand(BundleListCommand)
}
