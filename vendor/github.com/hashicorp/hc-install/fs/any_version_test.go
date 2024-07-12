// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package fs

import (
    "fmt"
    "path/filepath"
    "testing"

    "github.com/hashicorp/hc-install/product"
)

func TestAnyVersionValidate(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        av          AnyVersion
        expectedErr error
    }{
        "no-fields-set": {
            av:          AnyVersion{},
            expectedErr: fmt.Errorf("must use either ExactBinPath or Product + ExtraPaths"),
        },
        "ExactBinPath-and-Product": {
            av: AnyVersion{
                ExactBinPath: "/test",
                Product:      &product.Terraform,
            },
            expectedErr: fmt.Errorf("use either ExactBinPath or Product + ExtraPaths, not both"),
        },
        "ExactBinPath-empty": {
            av: AnyVersion{
                ExactBinPath: "",
            },
            expectedErr: fmt.Errorf("must use either ExactBinPath or Product + ExtraPaths"),
        },
        "ExactBinPath-relative": {
            av: AnyVersion{
                ExactBinPath: "test",
            },
            expectedErr: fmt.Errorf("expected ExactBinPath (\"test\") to be an absolute path"),
        },
        "ExactBinPath-absolute": {
            av: AnyVersion{
                ExactBinPath: filepath.Join(t.TempDir(), "test"),
            },
        },
        "Product-incorrect-binary-name": {
            av: AnyVersion{
                Product: &product.Product{
                    BinaryName: func() string { return "invalid!" },
                    Name:       product.Terraform.Name,
                },
            },
            expectedErr: fmt.Errorf("invalid binary name: \"invalid!\""),
        },
        "Product-valid": {
            av: AnyVersion{
                Product: &product.Terraform,
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            err := testCase.av.Validate()

            if err == nil && testCase.expectedErr != nil {
                t.Fatalf("expected error: %s, got no error", testCase.expectedErr)
            }

            if err != nil && testCase.expectedErr == nil {
                t.Fatalf("expected no error, got error: %s", err)
            }

            if err != nil && testCase.expectedErr != nil && err.Error() != testCase.expectedErr.Error() {
                t.Fatalf("expected error: %s, got error: %s", testCase.expectedErr, err)
            }
        })
    }
}
