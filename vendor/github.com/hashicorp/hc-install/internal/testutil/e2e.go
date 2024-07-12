// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package testutil

import (
    "os"
    "testing"
)

const e2eTestEnvVar = "E2E_TESTING"

func EndToEndTest(t *testing.T) {
    t.Helper()
    if os.Getenv(e2eTestEnvVar) == "" {
        t.Logf("%s is not set", e2eTestEnvVar)
        t.SkipNow()
    }
}
