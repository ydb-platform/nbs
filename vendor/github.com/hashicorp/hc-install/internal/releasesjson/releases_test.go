// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package releasesjson

import (
    "context"
    "testing"

    "github.com/hashicorp/go-version"
    "github.com/hashicorp/hc-install/internal/testutil"
)

func TestListProductVersions_excludesEnterpriseBuilds(t *testing.T) {
    testutil.EndToEndTest(t)

    r := NewReleases()
    r.SetLogger(testutil.TestLogger())

    ctx := context.Background()
    pVersions, err := r.ListProductVersions(ctx, "consul")
    if err != nil {
        t.Fatal(err)
    }

    testEntVersion := "1.9.8+ent"
    _, ok := pVersions[testEntVersion]
    if ok {
        t.Fatalf("Found unexpected Consul Enterprise version %q", testEntVersion)
    }
}

func TestGetProductVersion_excludesEnterpriseBuild(t *testing.T) {
    testutil.EndToEndTest(t)

    r := NewReleases()
    r.SetLogger(testutil.TestLogger())

    ctx := context.Background()

    testEntVersion := version.Must(version.NewVersion("1.9.8+ent"))

    _, err := r.GetProductVersion(ctx, "consul", testEntVersion)
    if err == nil {
        t.Fatalf("Expected enterprise version %q to error out",
            testEntVersion.String())
    }
}
