package uplink

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/readonly"
	"github.com/kopia/kopia/repo/blob/retrying"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
)

const (
	uplinkStorageType = "uplink"
)

type uplinkPointInTimeStorage struct {
	uplinkStorage

	pointInTime time.Time
}

type uplinkStorage struct {
	Options

	cli *minio.Client

	storageConfig *StorageConfig
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

func newStorage(ctx context.Context, opt *Options) (*uplinkStorage, error) {
	creds := credentials.NewChainCredentials(
		[]credentials.Provider{
			&credentials.Static{
				Value: credentials.Value{
					// AccessKeyID:     opt.AccessKeyID,
					// SecretAccessKey: opt.SecretAccessKey,
					// SessionToken:    opt.SessionToken,
					SignerType: credentials.SignatureV4,
				},
			},
			&credentials.EnvAWS{},
			&credentials.IAM{
				Client: &http.Client{
					Transport: http.DefaultTransport,
				},
			},
		},
	)

	return newStorageWithCredentials(ctx, creds, opt)
}

func newStorageWithCredentials(ctx context.Context, creds *credentials.Credentials, opt *Options) (*uplinkStorage, error) {
	if opt.BucketName == "" {
		return nil, errors.New("bucket name must be specified")
	}
	//TODO validate bucket exists
	// TODO f not existis opne bucket

	return nil, nil
}

// maybePointInTimeStore wraps s with a point-in-time store when s is versioned
// and a point-in-time value is specified. Otherwise s is returned.
func maybePointInTimeStore(ctx context.Context, s *uplinkStorage, pointInTime *time.Time) (blob.Storage, error) {
	if pit := s.Options.PointInTime; pit == nil || pit.IsZero() {
		return s, nil
	}

	// Does the bucket supports versioning?
	vi, err := s.cli.GetBucketVersioning(ctx, s.BucketName)
	if err != nil {
		return nil, errors.Wrapf(err, "could not get determine if bucket '%s' supports versioning", s.BucketName)
	}

	if !vi.Enabled() {
		return nil, errors.Errorf("cannot create point-in-time view for non-versioned bucket '%s'", s.BucketName)
	}

	return readonly.NewWrapper(&uplinkPointInTimeStorage{
		uplinkStorage: *s,
		pointInTime:   *pointInTime,
	}), nil
}

//############################ blob interface implementation

func (s *uplinkStorage) PutBlob(ctx context.Context, b blob.ID, data blob.Bytes, opts blob.PutOptions) error {

	panic("someFunc not implemented")
}

// DeleteBlob removes the blob from storage. Future Get() operations will fail with ErrNotFound.
func (s *uplinkStorage) DeleteBlob(ctx context.Context, b blob.ID) error {
	panic("someFunc not implemented")
}

// Close releases all resources associated with storage.
func (s *uplinkStorage) Close(ctx context.Context) error {
	return nil
}

// FlushCaches flushes any local caches associated with storage.
func (s *uplinkStorage) FlushCaches(ctx context.Context) error {
	return nil
}

func (s *uplinkStorage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   uplinkStorageType,
		Config: &s.Options,
	}
}

func (s *uplinkStorage) DisplayName() string {
	return fmt.Sprintf("S3: %v %v", s.Endpoint, s.BucketName)
}

func (s *uplinkStorage) GetBlob(ctx context.Context, b blob.ID, offset, length int64, output blob.OutputBuffer) error {
	panic("someFunc not implemented")

}

func (s *uplinkStorage) GetCapacity(ctx context.Context) (blob.Capacity, error) {
	return blob.Capacity{}, blob.ErrNotAVolume
}

func (s *uplinkStorage) GetMetadata(ctx context.Context, b blob.ID) (blob.Metadata, error) {
	panic("someFunc not implemented")
}

func (s *uplinkStorage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	panic("someFunc not implemented")
}
