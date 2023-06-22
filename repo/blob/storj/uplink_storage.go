package storj

import (
	"context"
	"fmt"
	"time"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/readonly"
	"github.com/kopia/kopia/repo/blob/retrying"
)

const (
	StorjStorageType = "storj"
)

type storjPointInTimeStorage struct {
	StorjStorage

	pointInTime time.Time
}

type StorjStorage struct {
	Options

	storageConfig *storjConfig
}

func New(ctx context.Context, opt *Options, isCreate bool) (blob.Storage, error) {
	st, err := newStorage(ctx, opt)
	if err != nil {
		return nil, err
	}

	s, err := maybePointInTimeStore(ctx, st, opt.PointInTime)
	if err != nil {
		return nil, err
	}

	return retrying.NewWrapper(s), nil
}

func newStorage(ctx context.Context, opt *Options) (*StorjStorage, error) {

	opt.ex = NewstorjExternal()

	err := SetupAccess(ctx, *opt)
	if err != nil {
		return nil, err
	}

	project, err := opt.ex.OpenProject(ctx, opt.access)
	if err != nil {
		return nil, err
	}
	defer func() { _ = project.Close() }()

	_, err = project.CreateBucket(ctx, opt.BucketName)

	if err != nil {
		return nil, err
	}

	storjStorage := StorjStorage{
		Options: *opt,

		storageConfig: &storjConfig{},
	}

	return &storjStorage, nil
}

// maybePointInTimeStore wraps s with a point-in-time store when s is versioned
// and a point-in-time value is specified. Otherwise s is returned.
func maybePointInTimeStore(ctx context.Context, s *StorjStorage, pointInTime *time.Time) (blob.Storage, error) {
	if pit := s.Options.PointInTime; pit == nil || pit.IsZero() {
		return s, nil
	}

	return readonly.NewWrapper(&storjPointInTimeStorage{
		StorjStorage: *s,
		pointInTime:  *pointInTime,
	}), nil
}

//############################ blob interface implementation

func (s *StorjStorage) PutBlob(ctx context.Context, b blob.ID, data blob.Bytes, opts blob.PutOptions) error {

	panic("someFunc not implemented")
}

// DeleteBlob removes the blob from storage. Future Get() operations will fail with ErrNotFound.
func (s *StorjStorage) DeleteBlob(ctx context.Context, b blob.ID) error {
	panic("someFunc not implemented")
}

// Close releases all resources associated with storage.
func (s *StorjStorage) Close(ctx context.Context) error {
	return nil
}

// FlushCaches flushes any local caches associated with storage.
func (s *StorjStorage) FlushCaches(ctx context.Context) error {
	return nil
}

func (s *StorjStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   StorjStorageType,
		Config: &s.Options,
	}
}

func (s *StorjStorage) DisplayName() string {
	return fmt.Sprintf("S3: %v %v", s.Endpoint, s.BucketName)
}

func (s *StorjStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	panic("someFunc not implemented")

}

func (s *StorjStorage) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	return blob.Capacity{}, blob.ErrNotAVolume
}

func (s *StorjStorage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	panic("someFunc not implemented")
}

func (s *StorjStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	panic("someFunc not implemented")
}
