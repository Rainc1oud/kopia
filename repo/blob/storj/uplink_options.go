package storj

import (
	"context"
	"time"

	"github.com/kopia/kopia/repo/blob/throttling"
	"github.com/zeebo/errs"
	"storj.io/storj/cmd/uplink/ulext"
	"storj.io/uplink"
)

// Options defines options for S3-based storage.
type Options struct {
	// BucketName is the name of the bucket where data is stored.
	BucketName string `json:"bucket"`
	Endpoint   string `json:"endpoint"`
	throttling.Limits
	ex ulext.External

	access     string
	accessName string
	keyOrGrant string

	// apikey is used
	satelliteAddr         string
	passphrase            string
	unencryptedObjectKeys bool

	recursive bool
	pending   bool
	expanded  bool

	// PointInTime specifies a view of the (versioned) store at that time
	PointInTime *time.Time `json:"pointInTime,omitempty"`
}

// Setup Storj / uplink access
func SetupAccess(ctx context.Context, o Options) (err error) {
	name := o.accessName
	if name == "" {
		return errs.New("name cannot be empty.")
	}

	if o.keyOrGrant == "" {
		return errs.New("keyOrGrant cannot be empty.")
	}

	keyOrGrant := o.keyOrGrant

	access, err := uplink.ParseAccess(keyOrGrant)
	// Grant is used
	if err == nil {
		_, err = o.generateUplinkAccess(ctx, name, access)
		if err != nil {
			return errs.Wrap(err)
		}
		// key is used
	} else {
		satelliteAddr := o.satelliteAddr
		if satelliteAddr == "" {
			return errs.New("Satellite address cannot be empty.")
		}

		passphrase := o.passphrase
		if passphrase == "" {
			return errs.New("Encryption passphrase cannot be empty.")
		}

		access, err = o.ex.RequestAccess(ctx, satelliteAddr, keyOrGrant, passphrase, o.unencryptedObjectKeys)
		if err != nil {
			return errs.Wrap(err)
		}

		_, err = o.generateUplinkAccess(ctx, name, access)
		if err != nil {
			return errs.Wrap(err)
		}
	}
	//TODO print / promt access

	return nil
}

func (o *Options) generateUplinkAccess(ctx context.Context, name string, access *uplink.Access) (_ *uplink.Access, err error) {
	_, accesses, err := o.ex.GetAccessInfo(false)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	accessValue, err := access.Serialize()
	if err != nil {
		return nil, errs.Wrap(err)
	}
	accesses[name] = accessValue
	err = o.ex.SaveAccessInfo(name, accesses)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	return access, nil
}

// 	fmt.Fprintf(clingy.Stdout(ctx), "Switched default access to %q\n", name)

// 	answer, err := c.ex.PromptInput(ctx, "Would you like S3 backwards-compatible Gateway credentials? (y/N):")
// 	if err != nil {
// 		return errs.Wrap(err)
// 	}
// 	answer = strings.ToLower(answer)

// 	if answer != "y" && answer != "yes" {
// 		return nil
// 	}

// 	credentials, err := RegisterAccess(ctx, access, c.authService, false, "")
// 	if err != nil {
// 		return errs.Wrap(err)
// 	}
// 	return errs.Wrap(DisplayGatewayCredentials(ctx, *credentials, "", ""))
// }
