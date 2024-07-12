// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package releases

import (
    "fmt"
    "testing"

    "github.com/hashicorp/go-version"
    "github.com/hashicorp/hc-install/product"
)

func TestExactVersionValidate(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        ev          ExactVersion
        expectedErr error
    }{
        "Product-incorrect-binary-name": {
            ev: ExactVersion{
                Product: product.Product{
                    BinaryName: func() string { return "invalid!" },
                    Name:       product.Terraform.Name,
                },
            },
            expectedErr: fmt.Errorf("invalid binary name: \"invalid!\""),
        },
        "Product-incorrect-name": {
            ev: ExactVersion{
                Product: product.Product{
                    BinaryName: product.Terraform.BinaryName,
                    Name:       "invalid!",
                },
            },
            expectedErr: fmt.Errorf("invalid product name: \"invalid!\""),
        },
        "Product-and-Version": {
            ev: ExactVersion{
                Product: product.Terraform,
                Version: version.Must(version.NewVersion("1.0.0")),
            },
        },
        "Version-missing": {
            ev: ExactVersion{
                Product: product.Terraform,
            },
            expectedErr: fmt.Errorf("unknown version"),
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            err := testCase.ev.Validate()

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
