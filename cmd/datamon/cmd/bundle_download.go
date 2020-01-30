// Copyright © 2018 One Concern

package cmd

import (
	"context"
	"fmt"
	"regexp"

	"github.com/oneconcern/datamon/pkg/core"
	"github.com/spf13/cobra"
)

const (
	fileDownloadsByConcurrencyFactor     = 10
	filelistDownloadsByConcurrencyFactor = 10
)

// BundleDownloadCmd is a command to download a read-only view of the bundle data
var BundleDownloadCmd = &cobra.Command{
	Use:   "download",
	Short: "Download a bundle",
	Long: `Download a read-only, non-interactive view of the entire data
that is part of a bundle.

If --bundle is not specified, the latest bundle (aka "commit") will be downloaded.

This is analogous to the git command "git checkout {commit-ish}".`,
	Example: `# Download a bundle by hash
% datamon bundle download --repo ritesh-test-repo --destination /path/to/folder/to/download --bundle 1INzQ5TV4vAAfU2PbRFgPfnzEwR

# Download a bundle by label
% datamon bundle download --repo ritesh-test-repo --destination /path/to/folder/to/download --label init
Using bundle: 1UZ6kpHe3EBoZUTkKPHSf8s2beh
...
`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()

		datamonFlagsPtr := &datamonFlags
		optionInputs := newCliOptionInputs(config, datamonFlagsPtr)
		remoteStores, err := optionInputs.datamonContext(ctx)
		if err != nil {
			wrapFatalln("create remote stores", err)
			return
		}
		destinationStore, err := datamonFlagsPtr.destStore(destTEmpty, "")
		if err != nil {
			wrapFatalln("create destination store", err)
			return
		}
		err = setLatestOrLabelledBundle(ctx, remoteStores)
		if err != nil {
			wrapFatalln("determine bundle id", err)
			return
		}

		bundleOpts, err := optionInputs.bundleOpts(ctx)
		if err != nil {
			wrapFatalln("failed to initialize bundle options", err)
		}
		bundleOpts = append(bundleOpts, core.Repo(datamonFlags.repo.RepoName))
		bundleOpts = append(bundleOpts, core.ConsumableStore(destinationStore))
		bundleOpts = append(bundleOpts, core.BundleID(datamonFlags.bundle.ID))
		bundleOpts = append(bundleOpts, core.ConcurrentFileDownloads(
			datamonFlags.bundle.ConcurrencyFactor/fileDownloadsByConcurrencyFactor))
		bundleOpts = append(bundleOpts, core.ConcurrentFilelistDownloads(
			datamonFlags.bundle.ConcurrencyFactor/filelistDownloadsByConcurrencyFactor))
		bundleOpts = append(bundleOpts, core.Logger(optionInputs.mustGetLogger()))

		bundle := core.NewBundle(core.NewBDescriptor(),
			bundleOpts...,
		)

		if datamonFlags.bundle.NameFilter != "" {
			var nameFilterRe *regexp.Regexp
			nameFilterRe, err = regexp.Compile(datamonFlags.bundle.NameFilter)
			if err != nil {
				wrapFatalln(fmt.Sprintf("name filter regexp %s didn't build", datamonFlags.bundle.NameFilter), err)
				return
			}
			err = core.PublishSelectBundleEntries(ctx, bundle, func(name string) (bool, error) {
				return nameFilterRe.MatchString(name), nil
			})
			if err != nil {
				wrapFatalln("publish bundle entries selected by name filter", err)
				return
			}
		} else {
			err = core.Publish(ctx, bundle)
			if err != nil {
				wrapFatalln("publish bundle", err)
				return
			}
		}
	},
	PreRun: func(cmd *cobra.Command, args []string) {
		optionInputs := newCliOptionInputs(config, &datamonFlags)
		optionInputs.populateRemoteConfig()
	},
}

func init() {
	requireFlags(BundleDownloadCmd,
		// Source
		addRepoNameOptionFlag(BundleDownloadCmd),
		// Destination
		addDataPathFlag(BundleDownloadCmd),
	)

	// Bundle to download
	addBundleFlag(BundleDownloadCmd)

	addLabelNameFlag(BundleDownloadCmd)

	addConcurrencyFactorFlag(BundleDownloadCmd, 100)

	addNameFilterFlag(BundleDownloadCmd)

	bundleCmd.AddCommand(BundleDownloadCmd)
}
