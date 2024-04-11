package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseMemoryItems(t *testing.T) {
	type testCase struct {
		line         string
		itemExpected memItem
	}
	cases := []testCase{
		testCase{
			"02443000-02444000 r--p 02242000 fd:11 3016                               /usr/bin/yc-disk-manager",
			memItem{memRange{0x2443000, 0x2444000}, "r--p", 0x2242000, "fd:11", 3016, "/usr/bin/yc-disk-manager"},
		},
		testCase{
			"02563000-025b3000 rw-p 00000000 00:00 0 ",
			memItem{memRange{0x2563000, 0x25b3000}, "rw-p", 0, "00:00", 0, ""},
		},
		testCase{
			"ffffffffff600000-ffffffffff601000 r-xp 00000000 00:00 0                  [vsyscall]",
			memItem{memRange{0xffffffffff600000, 0xffffffffff601000}, "r-xp", 0, "00:00", 0, "[vsyscall]"},
		},
	}

	for _, testCase := range cases {
		item, err := parseMemRange(testCase.line)
		require.NoError(t, err)
		require.EqualValues(t, testCase.itemExpected, *item)
	}
}

func TestShouldLockRange(t *testing.T) {
	type testCase struct {
		line               string
		shouldLockExpected bool
	}
	cases := []testCase{
		testCase{
			"00200000-02443000 r-xp 00000000 fd:11 3016                               /usr/bin/yc-disk-manager",
			false,
		},
		testCase{
			"02443000-02444000 r--p 02242000 fd:11 3016                               /usr/bin/yc-disk-manager",
			false,
		},
		testCase{
			"02563000-025b3000 rw-p 00000000 00:00 0 ",
			false,
		},
		testCase{
			"7fe4f2d4f000-7fe4f2d50000 rw-p 00000000 00:00 0 ",
			false,
		},
		testCase{
			"7fe4f2d50000-7fe4f3149000 ---p 00000000 00:00 0 ",
			false,
		},
		testCase{
			"7fe4f3149000-7fe4f3251000 r-xp 00000000 fd:11 21683                      /lib/x86_64-linux-gnu/libm-2.23.so",
			false,
		},
		testCase{
			"7fe4f3251000-7fe4f3450000 ---p 00108000 fd:11 21683                      /lib/x86_64-linux-gnu/libm-2.23.so",
			false,
		},
		testCase{
			"7fe4f3450000-7fe4f3451000 r--p 00107000 fd:11 21683                      /lib/x86_64-linux-gnu/libm-2.23.so",
			false,
		},
		testCase{
			"7fe4f3451000-7fe4f3452000 rw-p 00108000 fd:11 21683                      /lib/x86_64-linux-gnu/libm-2.23.so",
			false,
		},
		testCase{
			"ffffffffff600000-ffffffffff601000 r-xp 00000000 00:00 0                  [vsyscall]",
			false,
		},
	}

	for _, testCase := range cases {
		item, err := parseMemRange(testCase.line)
		require.NoError(t, err)
		shouldLock := shouldLockRange(item)
		require.EqualValues(t, testCase.shouldLockExpected, shouldLock)
	}
}

func TestLockBinaryDoesNotFail(t *testing.T) {
	err := LockBinary()
	require.NoError(t, err)
}
