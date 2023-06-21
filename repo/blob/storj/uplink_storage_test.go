package storj

import (
	"context"
	"testing"
	"time"

	"github.com/kopia/kopia/repo/blob/throttling"
)

var (
	// testSatelliteAddress = "16Q6kU8G3K7BxvsHVc3kb6TnjgWAMwE6RSkxwYc7C5S9EvdBj2@asatellite.rain.cloud:7777"
	testSatelliteAddress = "asatellite.rain.cloud:7777"
	testDemoApiKEy       = "1dfJZPALYQjkVjo5Yq14jpakQxZ549HfUrUePNRVxSznDGnyihpWwGvuC3o5TjXE9LSn3ePzj2eoND3Ugt1Wm4WYGb5tcsrj94qRmnyR6odGLhUkgTFd"
	testGrant            = "1T4nEUpLyBqrJSmJZA3zNV5ykbzrN3Phes99bYKL8kieRNzhkkvormrUKyEepDbeJiC1zG1Bnkdr1Li67h1untcmn5V822jCshYMLKsvwwyUxDGSvZmuwREotJC1kZpUqPaCcgh3FhxCqY2nf6vQDh4VMGP9oyef8aQ1UQmZxqL1EwTwWi7i6erYLVSs3NTXyzUuJq4gw2F1pwFv7qrUX4K1iWKaFysJi7GzQNVSuPgLmBqsdpEnuJ9HKysinLac7NNtGEjSqXZb42w9qghqBNk"
	testPassPhrase       = "demoMypass"
)

func TestStorjStorage(t *testing.T) {
	t.Parallel()

}

func TestMbBucket(t *testing.T) {
	testName := "TestMbBucket"

	options := Options{
		BucketName:            "demo1",
		Endpoint:              "",
		Limits:                throttling.Limits{},
		ex:                    NewRcExternal(), //Todo implement external interface (is not public)
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
}
