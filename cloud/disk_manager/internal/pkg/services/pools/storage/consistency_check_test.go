package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

func TestConsistencyCheckInflightDependents(t *testing.T) {
	ctx := newContext()

	s := &storageYDB{holdBaseDisksWithInflightDependents: true}

	// Actual number of base disks being created from each base disk.
	inflightDependents := map[string]int64{"src": 1}

	check := func(disk baseDisk) error {
		return s.checkBaseDiskConsistency(
			ctx,
			disk,
			nil, // slots
			inflightDependents,
		)
	}

	// Counter matches actual number of dependents.
	require.NoError(t, check(baseDisk{
		id:                 "src",
		status:             baseDiskStatusReady,
		inflightDependents: 1,
	}))
	require.NoError(t, check(baseDisk{
		id:     "other",
		status: baseDiskStatusReady,
	}))

	// Counter does not match.
	require.Error(t, check(baseDisk{
		id:                 "src",
		status:             baseDiskStatusReady,
		inflightDependents: 2,
	}))
	require.Error(t, check(baseDisk{
		id:     "src",
		status: baseDiskStatusReady,
	}))

	// Counter is negative.
	require.Error(t, check(baseDisk{
		id:                 "other",
		status:             baseDiskStatusReady,
		inflightDependents: -1,
	}))

	// Chain of holds: base disk that is being created from another base disk
	// has dependents itself.
	err := check(baseDisk{
		id:                 "src",
		srcDiskID:          "root",
		status:             baseDiskStatusCreating,
		inflightDependents: 1,
	})
	require.Error(t, err)
	require.ErrorContains(
		t,
		err,
		"is being created from another base disk, but has 1 inflight dependents",
	)

	// Not a chain: base disk was created from another base disk, but its
	// creation is finished.
	require.NoError(t, check(baseDisk{
		id:                 "src",
		srcDiskID:          "root",
		status:             baseDiskStatusReady,
		inflightDependents: 1,
	}))
}
