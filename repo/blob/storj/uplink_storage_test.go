package storj

import (
	"context"
	"testing"
	"time"

	"github.com/kopia/kopia/repo/blob/throttling"
)

func TestStorjStorage(t *testing.T) {
	t.Parallel()

}

func TestMbBucket(t *testing.T) {

	options := Options{
		BucketName:  "TestMbBucket",
		Endpoint:    "",
		Limits:      throttling.Limits{},
		ex:          nil,
		access:      "",
		PointInTime: &time.Time{},
	}
	ctx := context.Background()

	project, err := options.ex.OpenProject(ctx, options.access)
	if err != nil {
		t.Errorf("test")
	}
	defer func() { _ = project.Close() }()

	_, err = project.CreateBucket(ctx, options.BucketName)
}
