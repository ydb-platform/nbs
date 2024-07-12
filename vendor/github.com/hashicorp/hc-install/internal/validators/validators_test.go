// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package validators

import "testing"

func TestIsProductNameValid(t *testing.T) {
    testCases := []struct {
        name          string
        expectedValid bool
    }{
        {
            "terraform",
            true,
        },
        {
            "in.valid",
            false,
        },
    }
    for _, tc := range testCases {
        isValid := IsProductNameValid(tc.name)
        if !isValid && tc.expectedValid {
            t.Fatalf("expected %q to be valid", tc.name)
        }
        if isValid && !tc.expectedValid {
            t.Fatalf("expected %q to be invalid", tc.name)
        }
    }
}

func TestIsBinaryNameValid(t *testing.T) {
    testCases := []struct {
        name          string
        expectedValid bool
    }{
        {
            "terraform",
            true,
        },
        {
            "Terraform",
            true,
        },
        {
            "va_lid",
            true,
        },
        {
            "va.lid",
            true,
        },
        {
            "in/valid",
            false,
        },
    }
    for _, tc := range testCases {
        isValid := IsBinaryNameValid(tc.name)
        if !isValid && tc.expectedValid {
            t.Fatalf("expected %q to be valid", tc.name)
        }
        if isValid && !tc.expectedValid {
            t.Fatalf("expected %q to be invalid", tc.name)
        }
    }
}
