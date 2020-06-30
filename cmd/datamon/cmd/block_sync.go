package cmd

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	blake2b "github.com/minio/blake2b-simd"
	"github.com/spf13/cobra"
	"github.com/karrick/godirwalk"
)

/* transferring one file */

type xferFile interface {
	Next() int
	Hash(int) ([]byte, error)
	Set(io.Reader, int) error
	Get(int) (io.Reader, error)
}

func (f *xferFile_os) Next() int {
	f.curr += 1
	if f.size < f.blockSize * f.curr {
		return f.curr
	}
	return -1
}

func (f *xferFile_os) Hash(i int) ([]byte, error) {
	rdr, err := f.Get(i)
	if err != nil {
		return nil, err
	}
	var leafSize uint32
	leafSize = uint32(f.blockSize)
	hasher, err := blake2b.New(&blake2b.Config{
		Size: blake2b.Size,
		Tree: &blake2b.Tree{
			Fanout:        0,
			MaxDepth:      2,
			LeafSize:      leafSize,
			NodeOffset:    0,
			NodeDepth:     1,
			InnerHashSize: blake2b.Size,
			IsLastNode:    true,
		},
	})
	if err != nil {
		return nil, err
	}
	io.Copy(hasher, rdr)
	rv := hasher.Sum(nil)
	return rv, nil
}

/**
 * reader w/ fewer bytes than a block
 * always truncates.
 */
func (f *xferFile_os) Set(r io.Reader, i int) error {
	if f.blockSize * i > f.size {
		return fmt.Errorf("can't set holes in files")
	}
	f.lck.Lock()
	defer f.lck.Unlock()
	nRead, err := io.ReadFull(r, f.buf)
	if err != nil && err != io.ErrUnexpectedEOF { return err }
	readErr := err
	_, err = f.f.WriteAt(f.buf[:nRead], int64(f.blockSize * i))
	if err != nil { return err }
	if readErr == io.ErrUnexpectedEOF {
		if f.size > f.blockSize * i + nRead {
			err = f.f.Truncate(int64(f.blockSize * i + nRead))
			if err != nil { return err }
		}
		f.size = f.blockSize * i + nRead
		return nil
	}
	if f.blockSize * i >= f.size {
		f.size = f.blockSize * (i + 1)
	}
	return nil
}

func (f *xferFile_os) Get(i int) (io.Reader, error) {
	if i < 0 { i = f.curr }
	f.lck.Lock()
	defer f.lck.Unlock()
  n, err := f.f.ReadAt(f.buf, int64(f.blockSize * i))
	if err != nil && err != io.EOF { return nil, err }
	// expect copied to a new buffer in the reader
	res := bytes.NewBuffer(f.buf[:n])
  return res, nil
}

type xferFile_os struct{
	f *os.File
	size int
	pth string
	blockSize int
	curr int
	lck sync.Mutex
	buf []byte
}

func (f *xferFile_os) openFile(flags ...int) error {
	var err error
	flag := os.O_RDWR
	for _, fl := range flags {
		flag |= fl
	}
	f.lck.Lock()
	defer f.lck.Unlock()
	if f.f == nil {
		f.f, err = os.OpenFile(
			f.pth,
			flag,
			0755,
		)
		if err != nil { return err }
	}
	var fileInfo os.FileInfo
	fileInfo, err = f.f.Stat()
	if err != nil { return err }
	f.size = int(fileInfo.Size())
	return nil
}

func modifyTime(f *os.File) (int64, error) {
	var fileInfo os.FileInfo
	fileInfo, err := f.Stat()
	if err != nil { return -1, err }
	t := fileInfo.ModTime()
	return t.Unix(), nil
}

func fileExists(f *os.File) (bool, error) {
	_, err := f.Stat()
	if err == os.ErrNotExist {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func newXferFile(pth string) xferFile {
	var lck sync.Mutex
	return &xferFile_os{
		blockSize: 16,
		lck: lck,
		buf: make([]byte, 16),
		curr: 0,
		pth: pth,
		f: nil,
	}
}

func fileToXferFile(f *os.File) xferFile {
	var lck sync.Mutex
	return &xferFile_os{
		blockSize: 16,
		lck: lck,
		buf: make([]byte, 16),
		curr: 0,
		pth: f.Name(),
		f: f,
	}
}

/* quick util fn */
func hashesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for idx, x := range a {
		y := b[idx]
		if x != y {
			return false
		}
	}
	return true
}

type fileTransfer struct{
	Src xferFile
	Dest xferFile
	Win int
}

const (
	src = iota
	dest
)

const (
	ftTypeSrc = src
	ftTypeDest = dest
)

func (ft fileTransfer) xfer() error {
	var wg sync.WaitGroup
	var lck sync.Mutex
	copyErrorIndices := make([]int, 0)
	copyErrors := make([]error, 0)
	reportError := func(err error, i int) {
		defer lck.Unlock()
		if err != nil {
			lck.Lock()
			copyErrorIndices = append(copyErrorIndices, i)
			copyErrors = append(copyErrors, err)
		}
	}
	/* compare a block at a time */
	for i := 0; i != -1; i = ft.Src.Next() {
		srcHash, err := ft.Src.Hash(i)
		if err != nil { return err }
		destHash, err := ft.Dest.Hash(i)
		if err != nil { return err }
		if hashesEqual(srcHash, destHash) { continue }
		wg.Add(1)
		/* copy winning block on conflict */
		if ft.Win == src {
			go func() {
				defer wg.Done()
				rdr, err := ft.Src.Get(i)
				reportError(err, i)
				if err == nil {
					reportError(ft.Dest.Set(rdr, i), i)
				}
			}()
		} else if ft.Win == dest {
			go func() {
				defer wg.Done()
				rdr, err := ft.Dest.Get(i)
				reportError(err, i)
				if err == nil {
					reportError(ft.Src.Set(rdr, i), i)
				}
			}()
		} else {
			panic(fmt.Sprintf("unknown winner type %v <%T>",
				ft.Win, ft.Win))
		}
	}
	wg.Wait()
	if len(copyErrorIndices) == 0 { return nil }
	for _, copyError := range copyErrors {
		fmt.Printf("copy error: %v\n", copyError)
	}
	return fmt.Errorf("error copying some indices")
}

/* multiple file transfers */

func transferFiles(fts chan fileTransfer) error {
	var wg sync.WaitGroup
	errs := make([]error, 0)
	errFts := make([]fileTransfer, 0)
	var lck sync.Mutex
	reportErr := func(err error, ft fileTransfer) {
		if err != nil {
			lck.Lock()
			defer lck.Unlock()
			errFts = append(errFts, ft)
			errs = append(errs, err)
		}
	}
	//
	for ft := range fts {
		wg.Add(1)
		go func(ft fileTransfer) {
			defer wg.Done()
			srcOpenable := ft.Src.(*xferFile_os)
			destOpenable := ft.Dest.(*xferFile_os)
			reportErr(srcOpenable.openFile(), ft)
			reportErr(destOpenable.openFile(), ft)
			reportErr(ft.xfer(), ft)
		}(ft)
	}
	wg.Wait()
	if len(errs) == 0 { return nil }
	for idx, err := range errs {
		fmt.Printf("error transferring %v: %v",
			errFts[idx], err)
	}
	return fmt.Errorf("error transferring some files")
}

/* initializing fileTransfer channel */

const (
	winTypeRecent = iota
	winTypeTheirs
	winTypeOurs
)

type walkEntMsg struct{
	pth string
	de *godirwalk.Dirent
}

type walkErrMsg struct{
	pth string
	err error
}

type walkChans struct{
	dirs chan walkEntMsg
	files chan walkEntMsg
	errors chan walkErrMsg
}

func walkNodeCallback(
	chans *walkChans,
) func(string, *godirwalk.Dirent) error {
	return func(osPathname string, de *godirwalk.Dirent) error {
		if de.IsSymlink() {
			log.Printf("Skipping sym link:%s", osPathname)
			return nil
		}
		if de.IsDir() {
			chans.dirs <- walkEntMsg{pth: osPathname, de: de}
		} else {
			chans.files <- walkEntMsg{pth: osPathname, de: de}
		}
		return nil
	}
}

func (chans *walkChans) init() {
	chans.files = make(chan walkEntMsg)
	chans.dirs = make(chan walkEntMsg)
	chans.errors = make(chan walkErrMsg)
}

func (chans *walkChans) close() {
	close(chans.files)
	close(chans.dirs)
	close(chans.errors)
}

func walkChansToDirNameAndErrorChan(
	chans *walkChans,
) (chan string, chan error) {
	dirC := make(chan string)
	errC := make(chan error)
	go func() {
		defer close(dirC)
		defer close(errC)
		var (
			done bool
			msg walkEntMsg
			err walkErrMsg
		)
		for {
			select {
			case <-chans.files:
			case msg, done = <-chans.dirs:
				dirC <- msg.pth
				if done { return }
			case err = <-chans.errors:
				errC <- err.err
				return
			}
		}
	}()
	return dirC, errC
}

func walk(pth string) (chan string, chan error) {
	chans := new(walkChans)
	chans.init()
	var wg sync.WaitGroup
	wg.Add(1)
	if err := godirwalk.Walk(pth, &godirwalk.Options{
		Callback: walkNodeCallback(chans),
		ErrorCallback: func(osPathname string, err error) godirwalk.ErrorAction {
			errlog.Printf("Hit error:%s path:%s", err, osPathname)
			chans.errors <- walkErrMsg{pth: osPathname, err: err}
			return godirwalk.SkipNode
		},
		PostChildrenCallback: func(dir string, de *godirwalk.Dirent) error {
			chans.close()
			wg.Done()
			return nil
		},
		ScratchBuffer:       make([]byte, 2*1024*1024),
		FollowSymbolicLinks: false,
		Unsorted:            true,
	}); err != nil {
		fmt.Printf("Failed to start dirWalk error:%s", err)
	}
	return walkChansToDirNameAndErrorChan(chans)
}

func initFileTransfers(
	src string, dest string,
	winType int,
) chan fileTransfer {
	fts := make(chan fileTransfer)
	pths, _ := walk(src)
	go func() {
		for pth := range pths {
			sf, err := os.OpenFile(filepath.Join(src, pth),
				os.O_RDWR,
				0755,
			)
			if err != nil {
				errlog.Printf("error opening source: %v", err)
				continue
			}
			df, err := os.OpenFile(filepath.Join(dest, pth),
				os.O_RDWR & os.O_CREATE,
				0755,
			)
			if err != nil {
				errlog.Printf("error opening dest: %v", err)
				continue
			}
			var win int
			switch winType {
			case winTypeRecent:
				st, err := modifyTime(sf)
				if err != nil {
					errlog.Printf("error finding modify time: %v", err)
					continue
				}
				var dt int64
				exists, err := fileExists(df)
				if err != nil {
					errlog.Printf("error determining file existance: %v", err)
					continue
				}
				if exists {
					dt, err = modifyTime(df)
					if err != nil {
						errlog.Printf("error finding modify time: %v", err)
						continue
					}
				}
				if exists || st > dt {
					win = ftTypeSrc
				} else {
					win = ftTypeDest
				}
			case winTypeTheirs:
				exists, err := fileExists(df)
				if err != nil {
					errlog.Printf("error determining file existance: %v", err)
					continue
				}
				if exists {
					win = ftTypeSrc
				} else {
					win = ftTypeDest
				}
			case winTypeOurs:
				win = ftTypeSrc
			}
			fts <- fileTransfer{
				Src: fileToXferFile(sf),
				Dest: fileToXferFile(df),
				Win: win,
			}
		}
		close(fts)
	}()
	return fts
}


func doXfer(src, dest string) {

	fts := initFileTransfers(
		src, dest,
		winTypeRecent)

	if err := transferFiles(fts); err != nil {
		errlog.Printf("%v", err)
		return
	}
}


///////

var blockSyncCmd = &cobra.Command{
	Use:   "block-sync",
	Short: "Sync any two host os FS locations",
	Long: ``,

	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 2 {
			errlog.Println("expected src and dest args")
			os.Exit(4)
			return
		}
		src := args[0]
		dest := args[1]
		DieIfNotAccessible(src)
		DieIfNotAccessible(dest)


		doXfer(src, dest)

	},
}

func init() {
	rootCmd.AddCommand(blockSyncCmd)
}
