// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package fs

import (
    "fmt"
    "testing"

    "github.com/hashicorp/go-version"
    "github.com/hashicorp/hc-install/product"
)

func TestVersionValidate(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        v           Version
        expectedErr error
    }{
        "Product-incorrect-binary-name": {
            v: Version{
                Product: product.Product{
                    BinaryName: func() string { return "invalid!" },
                },
            },
            expectedErr: fmt.Errorf("invalid binary name: \"invalid!\""),
        },
        "Product-missing-get-version": {
            v: Version{
                Product: product.Product{
                    BinaryName: product.Terraform.BinaryName,
                },
                Constraints: version.MustConstraints(version.NewConstraint(">= 1.0")),
            },
            expectedErr: fmt.Errorf("undeclared version getter"),
        },
        "Product-missing-version-constraint": {
            v: Version{
                Product: product.Terraform,
            },
            expectedErr: fmt.Errorf("undeclared version constraints"),
        },
        "Product-and-version-constraint": {
            v: Version{
                Product:     product.Terraform,
                Constraints: version.MustConstraints(version.NewConstraint(">= 1.0")),
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            err := testCase.v.Validate()

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
