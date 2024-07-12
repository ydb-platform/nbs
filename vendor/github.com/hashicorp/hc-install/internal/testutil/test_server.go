// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package testutil

import (
    "fmt"
    "io"
    "net/http"
    "net/http/httptest"
    "os"
    "testing"
)

func NewTestServer(t testing.TB, mockDir string) *httptest.Server {
    t.Helper()

    mux := http.NewServeMux()
    mux.Handle("/", http.FileServer(http.Dir(mockDir)))

    ts := httptest.NewServer(mux)

    t.Cleanup(ts.Close)

    return ts
}

func JsonFromFile(file string) http.HandlerFunc {
    return func(w http.ResponseWriter, req *http.Request) {
        w.Header().Add("Content-Type", "application/json")

        f, err := os.Open(file)
        if err != nil {
            w.WriteHeader(500)
            fmt.Fprintf(w, `{"error": %q}`, err)
            return
        }
        defer f.Close()

        _, err = io.Copy(w, f)
        if err != nil {
            w.WriteHeader(500)
            fmt.Fprintf(w, `{"error": %q}`, err)
            return
        }
    }
}
