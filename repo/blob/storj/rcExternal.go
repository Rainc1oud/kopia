package storj

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/zeebo/errs"
	"storj.io/common/rpc/rpcpool"
	"storj.io/storj/cmd/uplink/ulext"
	"storj.io/storj/cmd/uplink/ulfs"
	"storj.io/uplink"
	storjAccess "storj.io/uplink/private/access"
	"storj.io/uplink/private/testuplink"
	"storj.io/uplink/private/transport"
)

const (
	uplinkCLIUserAgent = "blob-storj"
)

type rcExternal struct {
	access struct {
		loaded      bool              // true if we've successfully loaded access.json
		defaultName string            // default access name to use from accesses
		accesses    map[string]string // map of all of the stored accesses
	}

	dirs struct {
		loaded  bool   // true if Setup has been called
		current string // current config directory
		legacy  string // old config directory
	}
}

// NewRcExternal constructor for new rc external stuct
func NewRcExternal() *rcExternal {
	return &rcExternal{}
}

func (rc *rcExternal) OpenFilesystem(ctx context.Context, accessName string, options ...ulext.Option) (ulfs.Filesystem, error) {
	panic("func not implemented")
}

func (rc *rcExternal) OpenProject(ctx context.Context, accessName string, options ...ulext.Option) (*uplink.Project, error) {
	opts := ulext.LoadOptions(options...)

	access, err := rc.OpenAccess(accessName)
	if err != nil {
		return nil, err
	}

	if opts.EncryptionBypass {
		if err := storjAccess.EnablePathEncryptionBypass(access); err != nil {
			return nil, err
		}
	}

	config := uplink.Config{
		UserAgent: uplinkCLIUserAgent,
	}

	if opts.ConnectionPoolOptions != (rpcpool.Options{}) {
		if err := transport.SetConnectionPool(ctx, &config, rpcpool.New(opts.ConnectionPoolOptions)); err != nil {
			return nil, err
		}
	}

	if opts.ConcurrentSegmentUploadsConfig != (testuplink.ConcurrentSegmentUploadsConfig{}) {
		ctx = testuplink.WithConcurrentSegmentUploadsConfig(ctx, opts.ConcurrentSegmentUploadsConfig)
	}

	return config.OpenProject(ctx, access)
}

func (rc *rcExternal) AccessInfoFile() string {
	return filepath.Join(rc.dirs.current, "access.json")
}

func (rc *rcExternal) OpenAccess(accessName string) (access *uplink.Access, err error) {
	if access, err := parseAccessDataOrPossiblyFile(accessName); err == nil {
		return access, nil
	}

	defaultName, accesses, err := rc.GetAccessInfo(true)
	if err != nil {
		return nil, err
	}
	if accessName != "" {
		defaultName = accessName
	}

	if accessData, ok := accesses[defaultName]; ok {
		return uplink.ParseAccess(accessData)
	}

	// return nicer messages than the name
	if len(defaultName) < 20 {
		return nil, errs.New("Cannot find access named %q in saved accesses", defaultName)
	}
	return nil, errs.New("Unable to get access grant")
}

func parseAccessDataOrPossiblyFile(accessDataOrFile string) (*uplink.Access, error) {
	access, parseErr := uplink.ParseAccess(accessDataOrFile)
	if parseErr == nil {
		return access, nil
	}

	accessData, readErr := os.ReadFile(accessDataOrFile)
	if readErr != nil {
		var pathErr *os.PathError
		if errors.As(readErr, &pathErr) {
			readErr = pathErr.Err
		}
		return nil, errs.New("unable to parse access: %w", errs.Combine(parseErr, readErr))
	}

	return uplink.ParseAccess(string(bytes.TrimSpace(accessData)))
}

func (rc *rcExternal) GetAccessInfo(required bool) (string, map[string]string, error) {
	if !rc.access.loaded {
		if err := rc.loadAccesses(); err != nil {
			return "", nil, err
		}
		if required && !rc.access.loaded {
			return "", nil, errs.New("No accesses configured. Use 'access import' or 'access create' to create one")
		}
	}

	// return a copy to avoid mutations messing things up
	accesses := make(map[string]string)
	for name, accessData := range rc.access.accesses {
		accesses[name] = accessData
	}

	return rc.access.defaultName, accesses, nil
}

func (rc *rcExternal) SaveAccessInfo(defaultName string, accesses map[string]string) error {
	accessFh, err := os.OpenFile(rc.AccessInfoFile(), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return errs.Wrap(err)
	}
	defer func() { _ = accessFh.Close() }()

	var jsonOutput = struct {
		Default  string
		Accesses map[string]string
	}{
		Default:  defaultName,
		Accesses: accesses,
	}

	data, err := json.MarshalIndent(jsonOutput, "", "\t")
	if err != nil {
		return errs.Wrap(err)
	}

	if _, err := accessFh.Write(data); err != nil {
		return errs.Wrap(err)
	}

	if err := accessFh.Sync(); err != nil {
		return errs.Wrap(err)
	}

	if err := accessFh.Close(); err != nil {
		return errs.Wrap(err)
	}

	return nil
}

func (rc *rcExternal) RequestAccess(ctx context.Context, satelliteAddress, apiKey, passphrase string, unencryptedObjectKeys bool) (*uplink.Access, error) {
	panic("func not implemented")
}

func (rc *rcExternal) ExportAccess(ctx context.Context, access *uplink.Access, filename string) error {
	panic("func not implemented")
}

func (rc *rcExternal) ConfigFile() string {
	panic("func not implemented")
}

func (rc *rcExternal) SaveConfig(values map[string]string) error {
	panic("func not implemented")
}

func (rc *rcExternal) PromptInput(ctx context.Context, prompt string) (input string, err error) {
	panic("func not implemented")
}

func (rc *rcExternal) PromptSecret(ctx context.Context, prompt string) (secret string, err error) {
	panic("func not implemented")
}
