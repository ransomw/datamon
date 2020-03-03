package core

import (
	"bytes"
	"context"
	"hash/crc32"
	"io/ioutil"
	"time"

	context2 "github.com/oneconcern/datamon/pkg/context"
	"github.com/oneconcern/datamon/pkg/metrics"

	"gopkg.in/yaml.v2"

	"github.com/oneconcern/datamon/pkg/core/status"
	"github.com/oneconcern/datamon/pkg/model"
	"github.com/oneconcern/datamon/pkg/storage"
)

// Label describes a bundle label.
//
// A label is a name given to a bundle, analogous to tags in git.
// Examples: Latest, production.
type Label struct {
	Descriptor model.LabelDescriptor

	metrics.Enable
	m *M
}

func defaultLabel() *Label {
	return &Label{
		Descriptor: *model.NewLabelDescriptor(),
	}
}

// NewLabel builds a new label with a descriptor
func NewLabel(opts ...LabelOption) *Label {
	label := defaultLabel()
	for _, apply := range opts {
		apply(label)
	}

	if label.MetricsEnabled() {
		label.m = label.EnsureMetrics("core", &M{}).(*M)
	}
	return label
}

// UploadDescriptor persists the label descriptor for a bundle
func (label *Label) UploadDescriptor(ctx context.Context, bundle *Bundle) (err error) {
	defer func(t0 time.Time) {
		if label.MetricsEnabled() {
			label.m.Usage.UsedAll(t0, "LabelUpload")(err)
		}
	}(time.Now())

	err = RepoExists(bundle.RepoID, bundle.contextStores)
	if err != nil {
		return err
	}
	label.Descriptor.BundleID = bundle.BundleID
	buffer, err := yaml.Marshal(label.Descriptor)
	if err != nil {
		return err
	}



iter := new(model.ArchivePathToLabelIterator)
currPath := iter.Next()


	labelStore := bundle.contextStores.VMetadata()
	labelStoreCRC, ok := bundle.contextStores.VMetadata().(storage.StoreCRC)
	if ok {
		crc := crc32.Checksum(buffer, crc32.MakeTable(crc32.Castagnoli))
		err = labelStoreCRC.PutCRC(ctx,
			model.GetArchivePathToLabelUpload(bundle.RepoID, label.Descriptor.Name, labelStore),
			bytes.NewReader(buffer), storage.OverWrite, crc)

	} else {
		err = labelStore.Put(ctx,
			model.GetArchivePathToLabelUpload(bundle.RepoID, label.Descriptor.Name, labelStore),
			bytes.NewReader(buffer), storage.OverWrite)
	}
	if err != nil {
		return err
	}
	return nil
}

// DownloadDescriptor retrieves the label descriptor for a bundle
func (label *Label) DownloadDescriptor(ctx context.Context, bundle *Bundle, checkRepoExists bool) (err error) {
	defer func(t0 time.Time) {
		if label.MetricsEnabled() {
			label.m.Usage.UsedAll(t0, "LabelDownload")(err)
		}
	}(time.Now())

	if checkRepoExists {
		err = RepoExists(bundle.RepoID, bundle.contextStores)
		if err != nil {
			return err
		}
	}
	labelStore := getLabelStore(bundle.contextStores)
	archivePath := model.GetArchivePathToLabel(bundle.RepoID, label.Descriptor.Name, labelStore)
	has, err := getLabelStore(bundle.contextStores).Has(context.Background(), archivePath)
	if err != nil {
		return err
	}
	if !has {
		return status.ErrNotFound
	}
	rdr, err := labelStore.Get(context.Background(), archivePath)
	if err != nil {
		return err
	}
	o, err := ioutil.ReadAll(rdr)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(o, &label.Descriptor)
	if err != nil {
		return err
	}
	return nil
}

// GetLabelStore tells which store holds label metadata
func GetLabelStore(stores context2.Stores) storage.Store {
	return getLabelStore(stores)
}

func getLabelStore(stores context2.Stores) storage.Store {
	return stores.VMetadata()
}
