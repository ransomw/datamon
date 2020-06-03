package fuse

import (
	"os"
	"sync"

	// "github.com/jacobsa/fuse/fuseops"
)

type iNodeGenerator struct {
	lock         sync.Mutex
	highestInode myINodeID
	// TODO: Replace with something more memory and cpu efficient
	freeInodes []myINodeID
}

type lookupEntry struct {
	iNode myINodeID
	mode  os.FileMode
}

type nodeEntry struct {
	lock              sync.Mutex // TODO: Replace with key based locking.
	refCount          int
	// attr              fuseops.InodeAttributes
	attr              []interface{}
	pathToBackingFile string // empty for directory
}

func (g *iNodeGenerator) allocINode() myINodeID {
	g.lock.Lock()
	var n myINodeID
	if len(g.freeInodes) == 0 {
		g.highestInode++
		n = g.highestInode
	} else {
		n = g.freeInodes[len(g.freeInodes)-1]
		g.freeInodes = g.freeInodes[:len(g.freeInodes)-1]
		if len(g.freeInodes) == 0 {
			g.highestInode = firstINode
		}
	}
	g.lock.Unlock()
	return n
}

func (g *iNodeGenerator) freeINode(iNode myINodeID) {
	i := iNode
	g.lock.Lock()
	if g.highestInode == i {
		g.highestInode--
	} else {
		g.freeInodes = append(g.freeInodes, i)
	}
	g.lock.Unlock()
}
