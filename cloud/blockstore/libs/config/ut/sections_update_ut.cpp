#include <cloud/blockstore/libs/config/sections_update.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore {

Y_UNIT_TEST_SUITE(TConfigSectionsUpdateTest)
{
    // Verify that rejection detection distinguishes warnings from errors in
    // either order and that formatted rejections identify their sections.
    Y_UNIT_TEST(ShouldDistinguishRejectionsFromOtherSectionIssues)
    {
        // Treat a standalone warning as accepted.
        const TConfigSectionUpdateIssues warning{
            .Path = "Features",
            .Issues = {"Skipped an unsupported entry"},
        };
        const TConfigSectionsUpdateReport warningsOnly{.Sections = {warning}};
        UNIT_ASSERT(!warningsOnly.HasRejectedSections());

        // Detect each rejection regardless of its position in the report.
        for (const TString path: {"VolumeThrottling", "OtherSection"}) {
            const TConfigSectionUpdateIssues rejected{
                .Path = path,
                .RejectionError = MakeError(E_ARGUMENT, "Invalid value"),
            };
            const TConfigSectionsUpdateReport rejectionFirst{
                .Sections = {rejected, warning},
            };
            const TConfigSectionsUpdateReport rejectionLast{
                .Sections = {warning, rejected},
            };
            UNIT_ASSERT(rejectionFirst.HasRejectedSections());
            UNIT_ASSERT(rejectionLast.HasRejectedSections());

            // Include the rejected section's path in the formatted reason.
            UNIT_ASSERT_VALUES_EQUAL(
                TStringBuilder() << "Rejected invalid " << path
                                 << " configuration: Invalid value",
                FormatConfigSectionRejection(rejected));
        }
    }
}

}   // namespace NCloud::NBlockStore
