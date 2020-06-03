package fuse

import (
	"context"
	// "os"
	"path"
	"time"

	"github.com/oneconcern/datamon/pkg/cafs"
	"github.com/oneconcern/datamon/pkg/core"
	"github.com/oneconcern/datamon/pkg/core/status"
	"github.com/oneconcern/datamon/pkg/dlogger"
	"github.com/oneconcern/datamon/pkg/errors"

	iradix "github.com/hashicorp/go-immutable-radix"
	"go.uber.org/zap"

	"github.com/oneconcern/datamon/pkg/model"

	// jfuse "github.com/jacobsa/fuse"
	// "github.com/jacobsa/fuse/fuseops"
	// "github.com/jacobsa/fuse/fuseutil"
)

// var _ fuseutil.FileSystem = &readOnlyFsInternal{}

var (
	myENOENT = errors.New("my fuse replacement NO ENTry error")
)

type myINodeID uint64

const (
	myRootInodeID = 2
)

type myDirent struct{}

type readOnlyFsInternal struct {
	fsCommon

	// Get iNode for path. This is needed to generate directory entries without imposing a strict order of traversal.
	// This tree is relinquished to gc after the fs is generated.
	fsDirStore *iradix.Tree

	// Get FsEntry for an iNode. Speed up stat and other calls keyed by iNode
	fsEntryStore *iradix.Tree

	// List of children for a given iNode. Maps inode id to list of children. This stitches the fuse FS together.
	// NOTE: since populateFS is not parallel and is computed before the FS is available,
	// this map does not need being protected from concurrent access.
	// readDirMap map[fuseops.InodeID][]fuseutil.Dirent
	readDirMap map[myINodeID][]myDirent

	// readonly
	isReadOnly bool

	cafs     cafs.Fs
	streamed bool

	// Streamed mode options
	withVerifyHash bool
	lruSize        int
	prefetch       int
}

func defaultReadOnlyFS(bundle *core.Bundle) *readOnlyFsInternal {
	return &readOnlyFsInternal{
		fsCommon: fsCommon{
			bundle:     bundle,
			lookupTree: iradix.New(),
			l:          dlogger.MustGetLogger("info"),
		},
		readDirMap:   make(map[myINodeID][]myDirent),
		fsEntryStore: iradix.New(),
		fsDirStore:   iradix.New(),
	}
}

func asFsEntry(p interface{}) *FsEntry {
	fe := p.(FsEntry)
	return &fe
}

func (fs *readOnlyFsInternal) LookUpInode(ctx context.Context,
	op interface{}) (err error) {

	t0 := fs.opStart(op)
	defer fs.opEnd(t0, op, err)

	return status.ErrNoFuse
}

func (fs *readOnlyFsInternal) GetInodeAttributes(
	ctx context.Context,
	op interface{}) (err error) {

	t0 := fs.opStart(op)
	defer fs.opEnd(t0, op, err)

	return status.ErrNoFuse

	// key := formKey()
	// e, found := fs.fsEntryStore.Get(key)
	// if !found {
	// 	err = myENOENT
	// 	return
	// }
	// fe := asFsEntry(e)
	// op.AttributesExpiration = time.Now().Add(cacheYearLong)
	// op.Attributes = fe.attributes
	// return nil
}

func (fs *readOnlyFsInternal) ForgetInode(
	ctx context.Context,
	op interface{}) (err error) {
	t0 := fs.opStart(op)
	defer fs.opEnd(t0, op, err)
	return status.ErrNoFuse
}

func (fs *readOnlyFsInternal) OpenDir(ctx context.Context,
	op interface{}) (err error) {
	t0 := fs.opStart(op)
	defer fs.opEnd(t0, op, err)

	return status.ErrNoFuse

	p, found := fs.fsEntryStore.Get(formKey())
	if !found {
		err = myENOENT
		return
	}
	fe := asFsEntry(p)
	if !fe.isDir() {
		err = myENOENT
		return
	}
	return nil
}

func (fs *readOnlyFsInternal) ReadDir(ctx context.Context,
	op interface{}) (err error) {
	t0 := fs.opStart(op)
	defer fs.opEnd(t0, op, err)

	return status.ErrNoFuse

	// offset := int(op.Offset)
	// iNode := op.Inode

	// children, found := fs.readDirMap[iNode]

	// if !found {
	// 	err = myENOENT
	// 	return
	// }

	// if offset > len(children) {
	// 	err = myENOENT
	// 	return
	// }

	/*
	for i := offset; i < len(children); i++ {
		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], children[i])
		if n == 0 {
			break
		}
		op.BytesRead += n
	}
  */

	// return nil
}

func (fs *readOnlyFsInternal) ReleaseDirHandle(
	ctx context.Context,
	op interface{}) (err error) {
	t0 := fs.opStart(op)
	defer fs.opEnd(t0, op, err)
	return status.ErrNoFuse
}

func (fs *readOnlyFsInternal) OpenFile(
	ctx context.Context,
	op interface{}) (err error) {
	t0 := fs.opStart(op)
	defer fs.opEnd(t0, op, err)
	return status.ErrNoFuse
}

func (fs *readOnlyFsInternal) ReadFile(
	ctx context.Context,
	op interface{}) (err error) {

	var n int

	t0 := fs.opStart(op)
	defer func() {
		fs.opEnd(t0, op, err)
		if fs.MetricsEnabled() {
			fs.m.Volume.Files.Inc("read")
			fs.m.Volume.Files.Size(int64(n), "read")
			fs.m.Volume.IO.IORecord(t0, "read")(int64(n), err)
		}
	}()


	return status.ErrNoFuse

	// If file has not been mutated.
	// p, found := fs.fsEntryStore.Get(formKey())
	// if !found {
	// 	err = myENOENT
	// 	return
	// }
	// fe := asFsEntry(p)
	// fs.l.Debug("reading file", zap.String("file", fe.fullPath), zap.Uint64("inode", uint64(fe.iNode)))

	// now consumes the file from the bundle
	// n, err = fs.readAtBundle(fe, op.Dst, op.Offset)
	// op.BytesRead = n
	// return err
}

func (fs *readOnlyFsInternal) ReleaseFileHandle(
	ctx context.Context,
	op interface{}) (err error) {
	t0 := fs.opStart(op)
	defer fs.opEnd(t0, op, err)
	return status.ErrNoFuse
}

func (fs *readOnlyFsInternal) FlushFile(
	ctx context.Context,
	op interface{}) (err error) {
	// noop
	return status.ErrNoFuse
}

type fsNodeToAdd struct {
	// parentINode fuseops.InodeID
	parentINode myINodeID
	FsEntry     FsEntry
}

// FsEntry is a node in the filesystem
type FsEntry struct {
	hash string // Set for files, empty for directories

	// iNode ID is generated on the fly for a bundle that is committed. Since the file list
	// for a bundle is static and the list of files is frozen, multiple mounts of the same
	// bundle will preserve a fixed iNode for a file provided the order of reading the files
	// remains fixed.
	// iNode      fuseops.InodeID         // Unique ID for Fuse
	iNode      myINodeID         // Unique ID for Fuse
	// attributes fuseops.InodeAttributes // Fuse Attributes
	attributes []interface{}
	fullPath   string
}

func (f FsEntry) isDir() bool {
	return f.hash == ""
}

func newFsEntry(bundleEntry *model.BundleEntry, t time.Time,
	id myINodeID,
	linkCount uint32) *FsEntry {
	// var mode os.FileMode = fileReadOnlyMode
	// if bundleEntry.Hash == "" {
	// 	mode = dirReadOnlyMode
	// }
	return &FsEntry{
		fullPath: bundleEntry.NameWithPath,
		hash:     bundleEntry.Hash,
		iNode:    id,
		attributes: make([]interface{}, 0),
	}
}

func newBundleEntry(nameWithPath string) *model.BundleEntry {
	return &model.BundleEntry{
		Hash:         "", // Directories do not have datamon backed hash
		NameWithPath: nameWithPath,
		FileMode:     dirReadOnlyMode,
		Size:         2048, // TODO: Increase size of directory with file count when mount is mutable.
	}
}

// populateFSTxns holds  all the radix trees used during initialization
type populateFSTxns struct {
	dirStore     *iradix.Txn
	lookupTree   *iradix.Txn
	fsEntryStore *iradix.Txn
}

func (txns *populateFSTxns) commitToFS(fs *readOnlyFsInternal) {
	fs.fsEntryStore = txns.fsEntryStore.Commit()
	fs.lookupTree = txns.lookupTree.Commit()
	fs.fsDirStore = txns.dirStore.Commit()
}

func newFSTxns(fs *readOnlyFsInternal) *populateFSTxns {
	return &populateFSTxns{
		dirStore:     fs.fsDirStore.Txn(),
		lookupTree:   fs.lookupTree.Txn(),
		fsEntryStore: fs.fsEntryStore.Txn(),
	}
}

type populate struct {
	fs          *readOnlyFsInternal
	bundle      *core.Bundle
	txns        *populateFSTxns
	nodesToAdd  []fsNodeToAdd
	// iNode       *fuseops.InodeID
	iNode       *myINodeID
	bundleEntry model.BundleEntry
}

func (p *populate) WithINode(i myINodeID) *populate {
	// p.iNode = i
	return p
}

func (p *populate) WithEntry(entry model.BundleEntry) *populate {
	p.bundleEntry = entry
	return p
}

/* unwound recursion to build a list of entries terminating at the first extant parent */
// consider winding up recursion for clarity.(?).
func (p *populate) WithNodesFromEntry() *populate {
	next := func(i *myINodeID) myINodeID {
		*i++
		return *i
	}

	be := p.bundleEntry
	// Generate the FsEntry
	FsEntry := newFsEntry(
		&be,
		p.bundle.BundleDescriptor.Timestamp,
		next(p.iNode),
		fileLinkCount,
	)

	// Add parents if first visit
	// If a parent has been visited, all the parent's parents in the path have been visited
	nameWithPath := be.NameWithPath
	for {
		parentPath := path.Dir(nameWithPath)
		logger := p.fs.l.With(zap.String("parentPath", parentPath))
		logger.Debug("Processing parent", zap.String("fullPath", be.NameWithPath))

		// entry under root
		if parentPath == "" || parentPath == "." || parentPath == "/" {
			p.nodesToAdd = append(p.nodesToAdd, fsNodeToAdd{
				parentINode: myRootInodeID,
				FsEntry:     *FsEntry,
			})
			if len(p.nodesToAdd) > 1 {
				// If more than one node is to be added populate the parent iNode.
				p.nodesToAdd[len(p.nodesToAdd)-2].parentINode = p.nodesToAdd[len(p.nodesToAdd)-1].FsEntry.iNode
			}
			break
		}

		// Copy into queue
		p.nodesToAdd = append(p.nodesToAdd, fsNodeToAdd{
			parentINode: 0, // undefined
			FsEntry:     *FsEntry,
		})

		if len(p.nodesToAdd) > 1 {
			// If more than one node is to be added populate the parent iNode.
			p.nodesToAdd[len(p.nodesToAdd)-2].parentINode = p.nodesToAdd[len(p.nodesToAdd)-1].FsEntry.iNode
		}

		parent, found := p.txns.dirStore.Get([]byte(parentPath))
		if !found {
			logger.Debug("parentPath not found")
			FsEntry = newFsEntry(
				newBundleEntry(parentPath),
				p.bundle.BundleDescriptor.Timestamp,
				next(p.iNode),
				dirLinkCount,
			)
			// Continue till we hit root or found
			nameWithPath = parentPath
			continue
		} else {
			logger.Debug("parentPath found")
			parentDirEntry := asFsEntry(parent)
			if len(p.nodesToAdd) >= 1 {
				p.nodesToAdd[len(p.nodesToAdd)-1].parentINode = parentDirEntry.iNode
			}
		}
		logger.Debug("last node",
			zap.String("path", p.nodesToAdd[len(p.nodesToAdd)-1].FsEntry.fullPath),
			zap.Uint64("childInode", uint64(p.nodesToAdd[len(p.nodesToAdd)-1].FsEntry.iNode)),
			zap.Uint64("parentInode", uint64(p.nodesToAdd[len(p.nodesToAdd)-1].parentINode)))
		break
	}
	p.fs.l.Debug("Nodes added", zap.Int("count", len(p.nodesToAdd)))
	return p
}

// populateFSAddNodes adds the resolved nodes to the file system,
// discriminating between leaf nodes and directory nodes
func populateFSAddNodes(p *populate) (err error) {
	for _, nodeToAdd := range p.nodesToAdd {
		if false /* nodeToAdd.FsEntry.attributes.Nlink == dirLinkCount */ {
			err = p.fs.insertDirEntry(
				p.txns,
				nodeToAdd.parentINode,
				nodeToAdd.FsEntry,
			)
		} else {
			err = p.fs.insertFsEntry(
				p.txns,
				nodeToAdd.parentINode,
				nodeToAdd.FsEntry,
			)
		}
		if err != nil {
			return
		}
	}
	return
}

// populateFSAddBundleEntries iterates bundle entries and create FS nodes
func populateFSAddBundleEntries(p *populate) error {
	// For a Bundle Entry there might be intermediate directories that need adding.
	p.nodesToAdd = []fsNodeToAdd{} // TODO(fred): preallocate
	// iNode for fs entries
	// inode := firstINode

	for _, bundleEntry := range p.fs.bundle.GetBundleEntries() {
		err := populateFSAddNodes(p.
			// WithINode(&inode).
			WithEntry(bundleEntry).
			WithNodesFromEntry())
		if err != nil {
			return err
		}
		p.nodesToAdd = p.nodesToAdd[:0]
	}
	return nil
}

// populateFS is the top-level file system initialization method.
// It populates the file system with inodes constructed from the bundle contents.
//
// Note that only bundle metadata is used at this stage.
func (fs *readOnlyFsInternal) populateFS(bundle *core.Bundle) (*ReadOnlyFS, error) {
	txns := newFSTxns(fs)

	// Add root
	dirFsEntry := newFsEntry(
		newBundleEntry(rootPath),
		bundle.BundleDescriptor.Timestamp,
		myRootInodeID,
		dirLinkCount,
	)

	// Root points to itself
	if err := fs.insertDirEntry(txns, myRootInodeID, *dirFsEntry); err != nil {
		return nil, err
	}

	fs.l.Info("Populating fs", zap.Int("entryCount", len(fs.bundle.BundleEntries)))
	if err := populateFSAddBundleEntries(&populate{fs: fs, bundle: bundle, txns: txns}); err != nil {
		return nil, err
	}

	txns.commitToFS(fs)

	fs.isReadOnly = true

	// free this resource: it used only during FS setup
	fs.fsDirStore = nil
	fs.l.Info("Populating fs done")

	return &ReadOnlyFS{
		fsInternal: fs,
		// server:     fuseutil.NewFileSystemServer(fs),
	}, nil
}

func (fs *readOnlyFsInternal) insertDirEntry(
	txns *populateFSTxns,
	parentInode myINodeID,
	dirFsEntry FsEntry) error {

	pth := dirFsEntry.fullPath
	logger := fs.l.With(zap.String("fullPath", pth))
	logger.Debug("Inserting FSDirEntry",
		zap.Uint64("parentInode", uint64(parentInode)))

	if _, update := txns.dirStore.Insert([]byte(pth), dirFsEntry); update {
		return status.ErrUnexpectedUpdate.
			WrapWithLog(logger, errors.New("dirStore updates are not expected: /"+pth))
	}

	key := formKey()

	if _, update := txns.fsEntryStore.Insert(key, dirFsEntry); update {
		return status.ErrUnexpectedUpdate.
			WrapWithLog(logger, errors.New("fsEntryStore updates are not expected: /"+pth))
	}

	if dirFsEntry.iNode != myRootInodeID {
		key = formLookupKey(path.Base(pth))

		if _, update := txns.lookupTree.Insert(key, dirFsEntry); update {
			return status.ErrUnexpectedUpdate.
				WrapWithLog(logger, errors.New("lookupTree updates are not expected: /"+pth))
		}

		childEntries := fs.readDirMap[parentInode]
		childEntries = append(childEntries, myDirent{})
		fs.readDirMap[parentInode] = childEntries
	}

	return nil
}

func (fs *readOnlyFsInternal) insertFsEntry(
	txns *populateFSTxns,
	parentInode myINodeID,
	fsEntry FsEntry) error {
	pth := fsEntry.fullPath
	base := path.Base(pth)
	logger := fs.l.With(zap.String("fullPath", pth))

	logger.Debug("adding",
		zap.Uint64("parent", uint64(parentInode)),
		zap.Uint64("childInode", uint64(fsEntry.iNode)),
		zap.String("base", base))

	key := formKey()
	if _, update := txns.fsEntryStore.Insert(key, fsEntry); update {
		return status.ErrUnexpectedUpdate.
			WrapWithLog(logger, errors.New("fsEntryStore updates are not expected: /"+pth))
	}

	key = formLookupKey(base)
	if _, update := txns.lookupTree.Insert(key, fsEntry); update {
		return status.ErrUnexpectedUpdate.
			WrapWithLog(logger, errors.New("fsEntryStore updates are not expected: /"+pth))
	}

	childEntries := fs.readDirMap[parentInode]
	childEntries = append(childEntries, myDirent{})
	fs.readDirMap[parentInode] = childEntries

	return nil
}
