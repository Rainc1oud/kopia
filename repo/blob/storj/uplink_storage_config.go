package storj

import "github.com/kopia/kopia/repo/blob"

// PrefixAndStorageClass defines the storage class to use for a particular blob ID prefix.
type PrefixAndStorageClass struct {
	Prefix       blob.ID `json:"prefix"`
	StorageClass string  `json:"storageClass"`
}

// storjConfig contains storage configuration optionally persisted in the storage itself.
type storjConfig struct {
	BlobOptions []PrefixAndStorageClass `json:"blobOptions,omitempty"`
}
