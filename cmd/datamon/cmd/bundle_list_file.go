package cmd

import (
	"context"

	"github.com/oneconcern/datamon/pkg/core"

	"github.com/spf13/cobra"
)

var bundleFileList = &cobra.Command{
	Use:   "files",
	Short: "List files in a bundle",
	Long: `List all the files in a bundle.

You may use the "--label" flag as an alternate way to specify the bundle to search for.

This is analogous to the git command "git show --pretty="" --name-only {commit-ish}".
`,
	Example: `% datamon bundle list files --repo ritesh-test-repo --bundle 1ISwIzeAR6m3aOVltAsj1kfQaml
Using bundle: 1UZ6kpHe3EBoZUTkKPHSf8s2beh
name:bundle_upload.go, size:4021, hash:b9258e91eb29fe42c70262dd2da46dd71385995dbb989e6091328e6be3d9e3161ad22d9ad0fbfb71410f9e4730f6ac4482cc592c0bc6011585bd9b0f00b11463
...`,
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		datamonFlagsPtr := &datamonFlags
		optionInputs := newCliOptionInputs(config, datamonFlagsPtr)
		remoteStores, err := optionInputs.datamonContext(ctx)
		if err != nil {
			wrapFatalln("create remote stores", err)
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
		bundleOpts = append(bundleOpts, core.BundleID(datamonFlags.bundle.ID))
		bundle := core.NewBundle(core.NewBDescriptor(),
			bundleOpts...,
		)
		err = core.PopulateFiles(context.Background(), bundle)
		if err != nil {
			wrapFatalln("download filelist", err)
			return
		}
		for _, e := range bundle.BundleEntries {
			_, _ = logStdOut("name:%s, size:%d, hash:%s\n", e.NameWithPath, e.Size, e.Hash)
		}
	},
	PreRun: func(cmd *cobra.Command, args []string) {
		config.populateRemoteConfig(&datamonFlags)
	},
}

func init() {
	requireFlags(bundleFileList,
		// Source
		addRepoNameOptionFlag(bundleFileList),
	)

	// Bundle to download
	addBundleFlag(bundleFileList)

	addLabelNameFlag(bundleFileList)

	BundleListCommand.AddCommand(bundleFileList)
}
