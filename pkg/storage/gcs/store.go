// Copyright © 2018 One Concern

// Package gcs implements datamon Store for Google GCS
package gcs

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"go.uber.org/zap"
	"google.golang.org/api/iterator"

	gcsStorage "cloud.google.com/go/storage"

	"github.com/oneconcern/datamon/pkg/dlogger"
	"github.com/oneconcern/datamon/pkg/storage"
	storagestatus "github.com/oneconcern/datamon/pkg/storage/status"
	"google.golang.org/api/option"
)

type gcs struct {
	client         *gcsStorage.Client
	readOnlyClient *gcsStorage.Client
	bucket         string
	ctx            context.Context
	l              *zap.Logger
}

func clientOpts(readOnly bool, credentialFile string) []option.ClientOption {
	opts := make([]option.ClientOption, 0, 2)
	if readOnly {
		opts = append(opts, option.WithScopes(gcsStorage.ScopeReadOnly))
	} else {
		opts = append(opts, option.WithScopes(gcsStorage.ScopeFullControl))
	}
	if credentialFile != "" {
		opts = append(opts, option.WithCredentialsFile(credentialFile))
	}
	return opts
}

// patch BucketHandle.Object with gsutil object name convention's
// "strong recommendation" regarding version generation numbers.
// https://cloud.google.com/storage/docs/naming#objectnames
func versionedObject(
	bucketHandle *gcsStorage.BucketHandle,
	canonicalObjectName string,
) *gcsStorage.ObjectHandle {
// todo: validate (elsewhere -- and where?) that object names don't include '#'
	objectNameAndMaybeVersion := strings.Split(canonicalObjectName, "#")
	objectName := objectNameAndMaybeVersion[0]
	var (
		gen int64
		err error
	)
	gen = -1
	if len(objectNameAndMaybeVersion) > 1 {
		versionStr := objectNameAndMaybeVersion[1]
		gen, err = strconv.ParseInt(versionStr, 10, 64)
// ??? panic to indicate programming error in use of this internal function, or pass errors up stack?
		if err != nil { panic(err) }
	}
	objectHandle := bucketHandle.Object(objectName)
	return objectHandle.Generation(gen)
}

// New builds a new storage object from a bucket string
func New(ctx context.Context, bucket string, credentialFile string, opts ...Option) (storage.Store, error) {
	googleStore := new(gcs)
	for _, apply := range opts {
		apply(googleStore)
	}
	if googleStore.l == nil {
		googleStore.l, _ = dlogger.GetLogger("info")
	}
	googleStore.l = googleStore.l.With(zap.String("bucket", bucket))
	googleStore.ctx = ctx
	googleStore.bucket = bucket

	var err error
	googleStore.readOnlyClient, err = gcsStorage.NewClient(ctx, clientOpts(true, credentialFile)...)
	if err != nil {
		return nil, toSentinelErrors(err)
	}
	googleStore.client, err = gcsStorage.NewClient(ctx, clientOpts(false, credentialFile)...)
	if err != nil {
		return nil, toSentinelErrors(err)
	}
	return googleStore, nil
}

func (g *gcs) String() string {
	return "gcs://" + g.bucket
}

// Has this object in the store?
func (g *gcs) Has(ctx context.Context, objectName string) (bool, error) {
	client := g.readOnlyClient
	_, err := versionedObject(client.Bucket(g.bucket), objectName).Attrs(ctx)
	if err != nil {
		if err == gcsStorage.ErrObjectNotExist {
			return false, nil
		}
		return false, toSentinelErrors(err)
	}
	return true, nil
}

type gcsReader struct {
	g            *gcs
	objectName   string
	objectReader io.ReadCloser
	l            *zap.Logger
}

func (r *gcsReader) WriteTo(writer io.Writer) (n int64, err error) {
	return storage.PipeIO(writer, r.objectReader)
}

func (r *gcsReader) Close() error {
	return r.objectReader.Close()
}

func (r *gcsReader) Read(p []byte) (n int, err error) {
	r.l.Debug("Start Read", zap.Int("chunk size", len(p)))
	defer func() {
		r.l.Debug("End Read", zap.Int("chunk size", len(p)), zap.Int("bytes read", n), zap.Error(err))
	}()
	read, err := r.objectReader.Read(p)
	return read, toSentinelErrors(err)
}

func (r *gcsReader) ReadAt(p []byte, offset int64) (n int, err error) {
	r.l.Debug("Start ReadAt", zap.Int("chunk size", len(p)), zap.Int64("offset", offset))
	defer func() {
		r.l.Debug("End ReadAt", zap.Int("chunk size", len(p)), zap.Int64("offset", offset), zap.Int("bytes read", n), zap.Error(err))
	}()
	objectReader, err := versionedObject(r.g.readOnlyClient.Bucket(r.g.bucket), r.objectName).
		NewRangeReader(r.g.ctx, offset, int64(len(p)))
	if err != nil {
		return 0, toSentinelErrors(err)
	}
	return objectReader.Read(p)
}

func (g *gcs) Get(ctx context.Context, objectName string) (io.ReadCloser, error) {
	g.l.Debug("Start Get", zap.String("objectName", objectName))
	objectReader, err := versionedObject(g.readOnlyClient.Bucket(g.bucket), objectName).NewReader(ctx)
	g.l.Debug("End Get", zap.String("objectName", objectName), zap.Error(err))
	if err != nil {
		return nil, toSentinelErrors(err)
	}
	return &gcsReader{
		g:            g,
		objectReader: objectReader,
		l:            g.l,
	}, nil
}

func (g *gcs) GetAttr(ctx context.Context, objectName string) (storage.Attributes, error) {
	g.l.Debug("Start GetAttr", zap.String("objectName", objectName))
	attr, err := versionedObject(g.readOnlyClient.Bucket(g.bucket), objectName).Attrs(ctx)
	g.l.Debug("End GetAttr", zap.String("objectName", objectName), zap.Error(err))
	if err != nil {
		return storage.Attributes{}, toSentinelErrors(err)
	}
	return storage.Attributes{
		Created: attr.Created,
		Updated: attr.Updated,
		Owner:   attr.Owner,
	}, nil
}

func (g *gcs) GetAt(ctx context.Context, objectName string) (io.ReaderAt, error) {
	return &gcsReader{
		g:          g,
		objectName: objectName,
		l:          g.l,
	}, nil
}

func (g *gcs) Touch(ctx context.Context, objectName string) error {
	g.l.Debug("Start Touch", zap.String("objectName", objectName))
	_, err := versionedObject(g.client.Bucket(g.bucket), objectName).
		Update(ctx, gcsStorage.ObjectAttrsToUpdate{})
	g.l.Debug("End touch", zap.String("objectName", objectName), zap.Error(err))
	return toSentinelErrors(err)
}

type readCloser struct {
	reader io.Reader
}

func (rc readCloser) Read(p []byte) (int, error) {
	return rc.reader.Read(p)
}

func (rc readCloser) Close() error {
	return nil
}

func (g *gcs) Put(ctx context.Context, objectName string, reader io.Reader, newObject bool) (err error) {
	g.l.Debug("Start Put", zap.String("objectName", objectName))
	defer func() {
		g.l.Debug("End Put", zap.String("objectName", objectName), zap.Error(err))
	}()
	// Put if not present
	var writer *gcsStorage.Writer
	b := false
	if newObject {
		b = true
	}
	if newObject {
		writer = versionedObject(g.client.Bucket(g.bucket), objectName).If(gcsStorage.Conditions{
			DoesNotExist: b,
		}).NewWriter(ctx)
	} else {
		writer = versionedObject(g.client.Bucket(g.bucket), objectName).NewWriter(ctx)
	}
	g.l.Debug("Start Put PipeIO", zap.String("objectName", objectName))
	_, err = storage.PipeIO(writer, readCloser{reader: reader})
	g.l.Debug("End Put PipeIO", zap.String("objectName", objectName), zap.Error(err))
	if err != nil {
		return toSentinelErrors(err)
	}
	err = writer.Close()
	return toSentinelErrors(err)
}

func (g *gcs) PutCRC(ctx context.Context, objectName string, reader io.Reader, doesNotExist bool, crc uint32) (err error) {
	g.l.Debug("Start PutCRC", zap.String("objectName", objectName))
	defer func() {
		g.l.Debug("End PutCRC", zap.String("objectName", objectName), zap.Error(err))
	}()
	// Put if not present
	var writer *gcsStorage.Writer
	if doesNotExist {
		writer = versionedObject(g.client.Bucket(g.bucket), objectName).If(gcsStorage.Conditions{
			DoesNotExist: doesNotExist,
		}).NewWriter(ctx)
	} else {
		writer = versionedObject(g.client.Bucket(g.bucket), objectName).NewWriter(ctx)
	}
	writer.CRC32C = crc
	g.l.Debug("Start PutCRC PipeIO", zap.String("objectName", objectName))
	_, err = storage.PipeIO(writer, readCloser{reader: reader})
	g.l.Debug("End PutCRC PipeIO", zap.String("objectName", objectName), zap.Error(err))
	if err != nil {
		return toSentinelErrors(err)
	}
	err = writer.Close()
	return toSentinelErrors(err)
}

func (g *gcs) Delete(ctx context.Context, objectName string) (err error) {
	g.l.Debug("Start Delete", zap.String("objectName", objectName))
	err = toSentinelErrors(versionedObject(g.client.Bucket(g.bucket), objectName).Delete(ctx))
	g.l.Debug("End Delete", zap.String("objectName", objectName), zap.Error(err))
	return
}

// Keys returns all the keys known to a store
//
// TODO: Send an error if more than a million keys. Use KeysPrefix API.
func (g *gcs) Keys(ctx context.Context) (keys []string, err error) {
	g.l.Debug("Start Keys")
	defer func() {
		g.l.Debug("End Keys", zap.Int("keys", len(keys)), zap.Error(err))
	}()
	const keysPerQuery = 1000000
	var pageToken string
	nextPageToken := "sentinel" /* could be any nonempty string to start */
	keys = make([]string, 0)
	for nextPageToken != "" {
		var keysCurr []string
		keysCurr, nextPageToken, err = g.KeysPrefix(ctx, pageToken, "", "", keysPerQuery)
		if err != nil {
			return nil, toSentinelErrors(err)
		}
		keys = append(keys, keysCurr...)
		pageToken = nextPageToken
	}
	return keys, nil
}

func (g *gcs) KeysPrefix(
	ctx context.Context,
	pageToken string,
	prefix string,
	delimiter string,
	count int,
) (keys []string, next string, err error) {
	logger := g.l.With(
		zap.String("start", pageToken),
		zap.String("prefix", prefix),
		zap.Int("keys", len(keys)),
		zap.Error(err))
	logger.Debug("Start KeysPrefix")
	defer func() {
		logger.Debug("End KeysPrefix")
	}()

	itr := g.readOnlyClient.Bucket(g.bucket).Objects(ctx, &gcsStorage.Query{Prefix: prefix, Delimiter: delimiter})
	var objects []*gcsStorage.ObjectAttrs
	next, err = iterator.NewPager(itr, count, pageToken).NextPage(&objects)
	if err != nil {
		return nil, "", toSentinelErrors(err)
	}

	keys = make([]string, 0, count)
	for _, objAttrs := range objects {
		if objAttrs.Prefix != "" {
			keys = append(keys, objAttrs.Prefix)
		} else {
			keys = append(keys, objAttrs.Name)
		}
	}
	return
}

func (g *gcs) KeyVersions(ctx context.Context, key string) ([]string, error) {

fmt.Println("top KeyVersions")

	//	var err error
	logger := g.l.With(zap.String("key", key))
	logger.Debug("start KeyVersions")

	versionsPrefix := func(pageToken string) ([]string, string, error) {
		const versionsPerPage = 100
		itr := g.readOnlyClient.Bucket(g.bucket).Objects(ctx, &gcsStorage.Query{Prefix: key, Versions: true})
		var objects []*gcsStorage.ObjectAttrs
		nextPageToken, err := iterator.NewPager(itr, versionsPerPage, pageToken).NextPage(&objects)
		if err != nil {
			return nil, "", toSentinelErrors(err)
		}
		versions := make([]string, 0, versionsPerPage)
		for _, objAttrs := range objects {

fmt.Printf("adding version from objAttrs: %v\n", objAttrs)
fmt.Printf("generation number is %d\n", objAttrs.Generation)
fmt.Printf("Prefix is %q\n", objAttrs.Prefix)


			versions = append(versions, objAttrs.Name+"#"+strconv.FormatInt(objAttrs.Generation, 10))
		}
		return versions, nextPageToken, nil
	}

	//	itr := g.readOnlyClient.Bucket(g.bucket).Objects(ctx, &gcsStorage.Query{Prefix: prefix, Delimiter: delimiter})

	pageToken := ""
	versions := make([]string, 0)
	for {
		var versionsCurr []string
		versionsCurr, pageToken, err := versionsPrefix(pageToken)
		if err != nil {
			return nil, toSentinelErrors(err)
		}
		versions = append(versions, versionsCurr...)
		if pageToken == "" { break }
	}

	return versions, nil
}

func (g *gcs) Clear(context.Context) error {
	return storagestatus.ErrNotImplemented
}
