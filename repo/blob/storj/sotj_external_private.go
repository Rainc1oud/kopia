package storj

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/zeebo/errs"
)

func (se *storjExternal) loadAccesses() error {
	if se.access.accesses != nil {
		return nil
	}

	fh, err := os.Open(se.AccessInfoFile())
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return errs.Wrap(err)
	}
	defer func() { _ = fh.Close() }()

	var jsonInput struct {
		Default  string
		Accesses map[string]string
	}

	if err := json.NewDecoder(fh).Decode(&jsonInput); err != nil {
		return errs.Wrap(err)
	}

	// older versions may have written out invalid access mapping files
	// so check here and resave if necessary.
	defaultName, ok := checkAccessMapping(jsonInput.Default, jsonInput.Accesses)
	if ok {
		if err := se.SaveAccessInfo(defaultName, jsonInput.Accesses); err != nil {
			return errs.Wrap(err)
		}
	}

	se.access.defaultName = jsonInput.Default
	se.access.accesses = jsonInput.Accesses
	se.access.loaded = true

	return nil
}

func checkAccessMapping(accessName string, accesses map[string]string) (newName string, ok bool) {
	if _, ok := accesses[accessName]; ok {
		return accessName, false
	}

	// the only reason the name would not be present is because
	// it's actually an access grant. we could check that, but
	// if an error happens, the old config must be broken in
	// a way we can't repair, anyway, so let's just keep it the
	// same amount of broken. so, all we need to do is pick a
	// name that doesn't yet exist.

	newName = "main"
	for i := 2; ; i++ {
		if _, ok := accesses[newName]; !ok {
			break
		}
		newName = fmt.Sprintf("main-%d", i)
	}

	accesses[newName] = accessName
	return newName, true
}
