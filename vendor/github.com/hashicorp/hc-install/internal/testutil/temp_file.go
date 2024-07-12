// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package testutil

import (
    "os"
    "path/filepath"
    "runtime"
    "testing"
)

func CreateTempFile(t *testing.T, content string) (string, string) {
    tmpDir := t.TempDir()
    fileName := t.Name()

    if runtime.GOOS == "windows" {
        fileName += ".exe"
    }

    filePath := filepath.Join(tmpDir, fileName)
    f, err := os.Create(filePath)
    if err != nil {
        t.Fatal(err)
    }
    defer f.Close()
    _, err = f.WriteString(content)
    if err != nil {
        t.Fatal(err)
    }

    return tmpDir, fileName
}
