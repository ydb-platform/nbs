// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package releases

import (
    "fmt"
    "testing"

    "github.com/hashicorp/hc-install/product"
)

func TestLatestVersionValidate(t *testing.T) {
    t.Parallel()

    testCases := map[string]struct {
        lv          LatestVersion
        expectedErr error
    }{
        "Product-incorrect-binary-name": {
            lv: LatestVersion{
                Product: product.Product{
                    BinaryName: func() string { return "invalid!" },
                    Name:       product.Terraform.Name,
                },
            },
            expectedErr: fmt.Errorf("invalid binary name: \"invalid!\""),
        },
        "Product-incorrect-name": {
            lv: LatestVersion{
                Product: product.Product{
                    BinaryName: product.Terraform.BinaryName,
                    Name:       "invalid!",
                },
            },
            expectedErr: fmt.Errorf("invalid product name: \"invalid!\""),
        },
        "Product-valid": {
            lv: LatestVersion{
                Product: product.Terraform,
            },
        },
    }

    for name, testCase := range testCases {
        name, testCase := name, testCase

        t.Run(name, func(t *testing.T) {
            t.Parallel()

            err := testCase.lv.Validate()

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
