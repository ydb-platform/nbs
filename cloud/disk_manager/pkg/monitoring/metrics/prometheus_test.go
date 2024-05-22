package metrics

import (
	"testing"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

func TestPrometheusSanitizeName(t *testing.T) {
	testCases := []struct {
		invalidName, expectedSanitizedName string
	}{
		{"9invalidName", "_9invalidName"},
		{"validName", "validName"},
		{"_valid_Name", "_valid_Name"},
		{"invalid-Name", "invalid_Name"},
		{"123", "_123"},
	}

	for _, testCase := range testCases {
		actualSanitizedName := sanitizeName(testCase.invalidName)
		require.Equal(
			t,
			testCase.expectedSanitizedName,
			actualSanitizedName,
			"sanitizeName(%q) should be %q, got %q",
			testCase.invalidName,
			testCase.expectedSanitizedName,
			actualSanitizedName,
		)
	}
}
