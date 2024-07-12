// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package fs

import (
    "context"
    "os"
    "testing"

    "github.com/hashicorp/go-version"
    "github.com/hashicorp/hc-install/internal/testutil"
    "github.com/hashicorp/hc-install/product"
    "github.com/hashicorp/hc-install/releases"
    "github.com/hashicorp/hc-install/src"
)

var (
    _ src.Findable       = &AnyVersion{}
    _ src.LoggerSettable = &AnyVersion{}

    _ src.Findable       = &ExactVersion{}
    _ src.LoggerSettable = &ExactVersion{}

    _ src.Findable       = &Version{}
    _ src.LoggerSettable = &Version{}
)

func TestExactVersion(t *testing.T) {
    t.Skip("TODO")
    testutil.EndToEndTest(t)

    // TODO: mock out command execution?

    t.Setenv("PATH", "")

    ev := &ExactVersion{
        Product: product.Terraform,
        Version: version.Must(version.NewVersion("0.14.0")),
    }
    ev.SetLogger(testutil.TestLogger())
    _, err := ev.Find(context.Background())
    if err != nil {
        t.Fatal(err)
    }
}

func TestVersion(t *testing.T) {
    testutil.EndToEndTest(t)

    ctx := context.Background()

    p := t.TempDir()
    t.Setenv("PATH", p)
    ev := releases.ExactVersion{
        Product:    product.Terraform,
        Version:    version.Must(version.NewVersion("1.0.0")),
        InstallDir: p,
    }
    ev.SetLogger(testutil.TestLogger())

    execPath, err := ev.Install(ctx)
    if err != nil {
        t.Fatalf("installing release version failed: %v", err)
    }

    // Version matches constraint
    v := &Version{
        Product:     product.Terraform,
        Constraints: version.MustConstraints(version.NewConstraint(">= 1.0")),
    }
    v.SetLogger(testutil.TestLogger())
    if _, err := v.Find(ctx); err != nil {
        // this occasionally fails on Windows and we don't know why
        t.Logf("Terraform installed to %q", execPath)
        t.Logf("PATH: %q", os.Getenv("PATH"))
        t.Logf("PATHEXT: %q", os.Getenv("PATHEXT"))
        fi, err := os.Stat(execPath)
        if err != nil {
            t.Logf("stat failed: %s", err)
        } else {
            t.Logf("exec path %s FileInfo:\n"+
                " - Size: %d bytes\n"+
                " - ModTime: %s\n"+
                " - Perm: %s\n"+
                " - IsDir?: %t\n"+
                " - IsRegular?: %t\n",
                execPath,
                fi.Size(),
                fi.ModTime(),
                fi.Mode().Perm(),
                fi.Mode().IsDir(),
                fi.Mode().IsRegular())
        }

        t.Fatalf("finding: %v", err)
    }

    // Version mismatches constraint
    v.Constraints = version.MustConstraints(version.NewConstraint("> 1.0"))
    if _, err := v.Find(ctx); err == nil {
        t.Fatal("expecting error")
    }
}
