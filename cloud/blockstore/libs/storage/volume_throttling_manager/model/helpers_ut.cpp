#include "helpers.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

Y_UNIT_TEST_SUITE(TVolumeThrottlingHelpersTest)
{
    // Verify that coefficient diagnostics identify the rule and omit selector
    // IDs.
    Y_UNIT_TEST(ShouldReportInvalidCoefficientWithoutSelectorIds)
    {
        for (const bool useDiskSelector: {true, false}) {
            // Place an invalid rule after a valid rule to check its original
            // index.
            NProto::TVolumeThrottlingConfig config;
            config.AddRules()->MutableCoefficients()->SetMaxReadIops(0.5);
            auto* rule = config.AddRules();
            rule->MutableCoefficients()->SetMaxReadIops(2.0);
            if (useDiskSelector) {
                rule->MutableDisks()->AddDiskIds("private-disk");
            } else {
                auto* filter = rule->MutableFilter();
                filter->AddCloudIds("private-cloud");
                filter->AddFolderIds("private-folder");
            }

            // Validate the full list through the shared entry point.
            const auto error = ValidateThrottlingConfigRules(config.GetRules());
            const auto& message = error.GetMessage();

            // Check every diagnostic component and exclude selector values.
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                "Rules[1].Coefficients.MaxReadIops = 2; "
                "expected a finite value in [0, 1]",
                message);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
