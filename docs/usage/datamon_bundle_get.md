**Version: dev**

## datamon bundle get

Get bundle info

### Synopsis

Performs a direct lookup of a bundle.

Prints corresponding bundle metadata if the bundle exists,
exits with ENOENT status otherwise.

```
datamon bundle get [flags]
```

### Options

```
      --bundle string   The hash id for the bundle, if not specified the latest bundle will be used
  -h, --help            help for get
      --label string    The human-readable name of a label
      --repo string     The name of this repository
```

### Options inherited from parent commands

```
      --upgrade   Upgrades the current version then carries on with the specified command
```

### SEE ALSO

* [datamon bundle](datamon_bundle.md)	 - Commands to manage bundles for a repo
