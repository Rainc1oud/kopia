package storj

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/kopia/kopia/internal/gather"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/throttling"
)

var (
	// testSatelliteAddress = "16Q6kU8G3K7BxvsHVc3kb6TnjgWAMwE6RSkxwYc7C5S9EvdBj2@asatellite.rain.cloud:7777"
	testSatelliteAddress = "asatellite.rain.cloud:7777"
	testDemoApiKEy       = "1dfJZPALYQjkVjo5Yq14jpakQxZ549HfUrUePNRVxSznDGnyihpWwGvuC3o5TjXE9LSn3ePzj2eoND3Ugt1Wm4WYGb5tcsrj94qRmnyR6odGLhUkgTFd"
	testGrant            = "1T4nEUpLyBqrJSmJZA3zNV5ykbzrN3Phes99bYKL8kieRNzhkkvormrUKyEepDbeJiC1zG1Bnkdr1Li67h1untcmn5V822jCshYMLKsvwwyUxDGSvZmuwREotJC1kZpUqPaCcgh3FhxCqY2nf6vQDh4VMGP9oyef8aQ1UQmZxqL1EwTwWi7i6erYLVSs3NTXyzUuJq4gw2F1pwFv7qrUX4K1iWKaFysJi7GzQNVSuPgLmBqsdpEnuJ9HKysinLac7NNtGEjSqXZb42w9qghqBNk"
	testPassPhrase       = "demoMypass"
	testAccessName       = "demo-storj"
	testBucketName       = "demo-storj1"
	optionsTest1         = Options{
		BucketName:            testBucketName,
		Endpoint:              "",
		Limits:                throttling.Limits{},
		ex:                    NewstorjExternal(), //Todo implement external interface (is not public)
		access:                "",
		accessName:            testAccessName,
		keyOrGrant:            testGrant,
		satelliteAddr:         "",
		passphrase:            testPassPhrase,
		unencryptedObjectKeys: false,
		PointInTime:           &time.Time{},
	}
	testfile = "./testdata/test1.txt"
)

func TestStorjStorage(t *testing.T) {
	t.Parallel()

}

// interface
func TestNew(t *testing.T) {
	testName := "TestNew"

	ctx := context.Background()
	_, err := New(ctx, &optionsTest1, false)
	if err != nil {
		t.Errorf(testName)
	}
}

func TestListBlobs(t *testing.T) {
	testName := "TestListBlobs"

	ctx := context.Background()

	storjStorage, err := New(ctx, &optionsTest1, false)
	if err != nil {
		t.Errorf(testName)
	}

	storjStorage.ListBlobs(ctx, blob.ID(testAccessName), func(b blob.Metadata) error {
		if b.BlobID == blob.ID(testBucketName) {
			fmt.Printf("found requested bucket %q  \n", b.BlobID)
		} else {
			fmt.Printf("bucket %q found \n", b.BlobID)
		}

		return nil
	})

}

// function tests
func TestMbBucket(t *testing.T) {
	testName := "TestMbBucket"

	options := Options{
		BucketName:            "demo5",
		Endpoint:              "",
		Limits:                throttling.Limits{},
		ex:                    NewstorjExternal(), //Todo implement external interface (is not public)
		access:                "",
		accessName:            "demo",
		keyOrGrant:            testGrant,
		satelliteAddr:         "",
		passphrase:            testPassPhrase,
		unencryptedObjectKeys: false,
		PointInTime:           &time.Time{},
	}
	ctx := context.Background()

	err := SetupAccess(ctx, options)
	if err != nil {
		t.Errorf(testName)
	}

	project, err := options.ex.OpenProject(ctx, options.access)
	if err != nil {
		t.Errorf(testName)
	}
	defer func() { _ = project.Close() }()

	_, err = project.CreateBucket(ctx, options.BucketName)

	if err != nil {
		t.Errorf(testName)
	}

	_, err = project.DeleteBucket(ctx, options.BucketName)
	if err != nil {
		t.Errorf(testName)
	}
}

func TestDbBucket(t *testing.T) {
	testName := "TestDbBucket"

	bucketName := "new"
	options := Options{
		BucketName:            bucketName,
		Endpoint:              "",
		Limits:                throttling.Limits{},
		ex:                    NewstorjExternal(), //Todo implement external interface (is not public)
		access:                "",
		accessName:            "demo",
		keyOrGrant:            testGrant,
		satelliteAddr:         "",
		passphrase:            testPassPhrase,
		unencryptedObjectKeys: false,
		PointInTime:           &time.Time{},
	}
	ctx := context.Background()

	err := SetupAccess(ctx, options)
	if err != nil {
		t.Errorf(testName)
	}

	project, err := options.ex.OpenProject(ctx, options.access)
	if err != nil {
		t.Errorf(testName)
	}
	defer func() { _ = project.Close() }()

	_, err = project.DeleteBucket(ctx, options.BucketName)
	if err != nil {
		t.Errorf(testName)
	}
}

func TestPutBlob(t *testing.T) {
	testName := "PutBlob"

	ctx := context.Background()
	storjStorage, err := New(ctx, &optionsTest1, false)
	if err != nil {
		t.Errorf(testName)
	}

	putOptions := blob.PutOptions{}

	err = storjStorage.PutBlob(ctx, blob.ID(testfile), gather.FromSlice([]byte{}), putOptions)
	if err != nil {
		t.Errorf(testName)
	}

}

func TestListBlob(t *testing.T) {
	testName := "TestListBlob"

	ctx := context.Background()
	storjStorage, err := New(ctx, &optionsTest1, false)
	if err != nil {
		t.Errorf(testName)
	}

	//set storj path
	path := filepath.Join(StorjPath, testAccessName)
	storjStorage.ListBlobs(ctx, blob.ID(path), func(b blob.Metadata) error {
		if b.BlobID == blob.ID(testBucketName) {
			fmt.Printf("found requested bucket %q  \n", b.BlobID)
		} else {
			fmt.Printf("bucket %q found \n", b.BlobID)
		}

		return nil
	})

}
