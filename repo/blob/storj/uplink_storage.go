package storj

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/readonly"
	"github.com/kopia/kopia/repo/blob/retrying"
	"github.com/zeebo/errs"
	"storj.io/common/fpath"
	"storj.io/common/rpc/rpcpool"
	"storj.io/storj/cmd/uplink/ulext"
	"storj.io/storj/cmd/uplink/ulfs"
	"storj.io/storj/cmd/uplink/ulloc"
)

const (
	StorjStorageType = "storj"
	StorjPath        = "sj://"
)

type storjPointInTimeStorage struct {
	StorjStorage

	pointInTime time.Time
}

type StorjStorage struct {
	Options
	encrypted     bool
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
		fmt.Printf("bucket %q allready exists \n", opt.BucketName)
		// todo log bucket allready exists, issue from satellite
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

	fs, err := s.ex.OpenFilesystem(ctx, s.access,
		ulext.ConcurrentSegmentUploadsConfig(s.uploadConfig),
		ulext.ConnectionPoolOptions(rpcpool.Options{
			// Add a bit more capacity for connections to the satellite
			Capacity:       s.uploadConfig.SchedulerOptions.MaximumConcurrent + 5,
			KeyCapacity:    5,
			IdleExpiration: 2 * time.Minute,
		}))
	if err != nil {
		return err
	}

	defer func() { _ = fs.Close() }()

	if s.inmemoryEC {
		ctx = fpath.WithTempData(ctx, "", true)
	}

	path, err := filepath.Abs(string(b))
	if err != nil {
		return err
	}
	// set locs
	s.locs = []ulloc.Location{ulloc.NewLocal(path), ulloc.NewLocal(path)}
	//TODO fix locations
	var eg errs.Group
	for _, source := range s.locs[:len(s.locs)-1] {
		eg.Add(s.dispatchCopy(ctx, fs, source, s.locs[len(s.locs)-1]))
	}
	return combineErrs(eg)
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
	ctx, cancel := context.WithCancel(ctx)

	defer cancel()

	project, err := s.ex.OpenProject(ctx, s.Options.access)
	if err != nil {
		return err
	}
	defer func() { _ = project.Close() }()

	iter := project.ListBuckets(ctx, nil)

	for iter.Next() {
		item := iter.Item()

		if blob.ID(item.Name) == prefix {

			fs, err := s.ex.OpenFilesystem(ctx, s.access, ulext.BypassEncryption(s.encrypted))
			if err != nil {
				return err
			}
			defer func() { _ = fs.Close() }()

			prefixUloc := ulloc.NewLocal(item.Name)

			if fs.IsLocalDir(ctx, prefixUloc) {
				prefixUloc = prefixUloc.AsDirectoryish()
			}

			// create the object iterator of either existing objects or pending multipart uploads
			iter, err := fs.List(ctx, prefixUloc, &ulfs.ListOptions{
				Recursive: s.Options.recursive,
				Pending:   s.pending,
				Expanded:  s.expanded,
			})
			if err != nil {
				return err
			}
			for iter.Next() {
				obj := iter.Item()

				bm := blob.Metadata{
					BlobID:    blob.ID(obj.Loc.Loc()),
					Length:    obj.ContentLength,
					Timestamp: item.Created,
				}

				if err := callback(bm); err != nil {
					return err
				}

			}
		}
	}

	return nil
}

func combineErrs(group errs.Group) error {
	if len(group) == 0 {
		return nil
	}

	errstrings := make([]string, len(group))
	for i, err := range group {
		errstrings[i] = err.Error()
	}

	return fmt.Errorf("%s", strings.Join(errstrings, "\n"))
}
