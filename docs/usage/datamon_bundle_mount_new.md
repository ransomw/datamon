**Version: dev**

## datamon bundle mount new

Create a bundle incrementally with filesystem operations

### Synopsis

Write directories and files to the mountpoint.  Unmount or send SIGINT to this process to save.

```
datamon bundle mount new [flags]
```

### Options

```
      --daemonize            Whether to run the command as a daemonized process
      --destination string   The path to the download dir
  -h, --help                 help for new
      --message string       The message describing the new bundle
      --mount string         The path to the mount dir
      --repo string          The name of this repository
```

### Options inherited from parent commands

```
      --upgrade   Upgrades the current version then carries on with the specified command
```

### SEE ALSO

* [datamon bundle mount](datamon_bundle_mount.md)	 - Mount a bundle
