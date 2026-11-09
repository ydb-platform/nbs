package pools

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

func TestValidateIdleCleanupConfig(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		ttl   time.Duration
		hold  bool
		valid bool
	}{
		{name: "disabled_without_hold", valid: true},
		{name: "disabled_with_hold", hold: true, valid: true},
		{name: "enabled_with_hold", ttl: time.Hour, hold: true, valid: true},
		{name: "enabled_without_hold", ttl: time.Hour},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateIdleCleanupConfig(testCase.ttl, testCase.hold)
			if testCase.valid {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "BaseDiskIdleTTL")
				require.ErrorContains(t, err, "HoldBaseDisksWithInflightDependents")
			}
		})
	}
}

func TestValidateUseReadyBaseDiskAsSrcConfig(t *testing.T) {
	for _, testCase := range []struct {
		name                  string
		useReadyBaseDiskAsSrc bool
		hold                  bool
		valid                 bool
	}{
		{name: "disabled_without_hold", valid: true},
		{name: "disabled_with_hold", hold: true, valid: true},
		{name: "enabled_with_hold", useReadyBaseDiskAsSrc: true, hold: true, valid: true},
		{name: "enabled_without_hold", useReadyBaseDiskAsSrc: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateUseReadyBaseDiskAsSrcConfig(
				testCase.useReadyBaseDiskAsSrc,
				testCase.hold,
			)
			if testCase.valid {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "UseReadyBaseDiskAsSrc")
				require.ErrorContains(t, err, "HoldBaseDisksWithInflightDependents")
			}
		})
	}
}
