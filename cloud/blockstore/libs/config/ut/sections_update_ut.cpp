#include <cloud/blockstore/libs/config/sections_update.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

Y_UNIT_TEST_SUITE(TConfigSectionsUpdateTest)
{
    // Verify that rejection detection handles multiple sections independently
    // of warning-only records and of the failing section's position.
    Y_UNIT_TEST(ShouldDistinguishRejectionsFromOtherSectionIssues)
    {
        // Keep a warning-only record separate from actual section rejections.
        TConfigSectionsUpdateIssues issues;
        auto& warning = issues.Sections.emplace_back();
        warning.Path = "Features";
        warning.Issues.push_back("Skipped an unsupported entry");
        UNIT_ASSERT(!issues.HasRejectedSections());

        // Detect and format more than one rejection without losing its path.
        for (const TString path: {"VolumeThrottling", "OtherSection"}) {
            auto& rejected = issues.Sections.emplace_back();
            rejected.Path = path;
            rejected.RejectionError = MakeError(E_ARGUMENT, "Invalid value");
            UNIT_ASSERT(issues.HasRejectedSections());
            UNIT_ASSERT_VALUES_EQUAL(
                TString("Rejected invalid ") + path +
                    " configuration: Invalid value",
                FormatConfigSectionRejection(rejected));
        }
    }
}

}   // namespace NCloud::NBlockStore
