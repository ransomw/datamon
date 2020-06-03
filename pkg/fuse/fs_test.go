package fuse

import (
	"os"
	"sync"
	"testing"
	"time"

	iradix "github.com/hashicorp/go-immutable-radix"
	"github.com/spf13/afero"

	"github.com/stretchr/testify/assert"

	// "github.com/jacobsa/fuse/fuseops"

	// "github.com/jacobsa/fuse/fuseutil"

	"github.com/stretchr/testify/require"
)

type LookupKeys struct {
	iNode    myINodeID
	name     string
	expected []byte
}

var lookupKeys = []LookupKeys{
	// Already clean
	{0, "0", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30}},
	{1, "0", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x30}},
	{16, "2", []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x32}},
	{18446744073709551615, "key", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x6b, 0x65, 0x79}},
}

func TestFormLookupKey(t *testing.T) {
	for _, test := range lookupKeys {
		keyGenerated := formLookupKey(test.name)
		require.Equal(t, test.expected, keyGenerated)
	}
}

func TestCreate(t *testing.T) {
	const testRoot = "../../testdata/core"
	t.SkipNow()
	child := "child"
	fs := fsMutable{
		fsCommon: fsCommon{
			bundle:     nil,
			lookupTree: iradix.New(),
		},
		iNodeStore: iradix.New(),
		// readDirMap: make(map[myINodeID]map[myINodeID]*fuseutil.Dirent),
		lock:       sync.Mutex{},
		iNodeGenerator: iNodeGenerator{
			lock:         sync.Mutex{},
			highestInode: firstINode,
			freeInodes:   make([]myINodeID, 0, 1024), // equates to 1024 files deleted
		},
		localCache:   afero.NewBasePathFs(afero.NewOsFs(), testRoot+"/fs"),
		backingFiles: make(map[myINodeID]*afero.File),
	}
	fs.iNodeStore, _, _ = fs.iNodeStore.Insert(formKey(), &nodeEntry{
		/*
		attr: fuseops.InodeAttributes{
			Size:   64,
			Nlink:  dirLinkCount,
			Mode:   dirDefaultMode,
			Atime:  time.Time{},
			Mtime:  time.Time{},
			Ctime:  time.Time{},
			Crtime: time.Time{},
			Uid:    defaultUID,
			Gid:    defaultGID,
		},
    */
	})

}
