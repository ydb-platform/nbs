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

    // Verify that preparation groups nonfatal VolumeThrottling issues by
    // section and omits successfully prepared sections without issues.
    Y_UNIT_TEST(ShouldCollectOnlySectionsWithIssues)
    {
        // Keep a valid zero coefficient after skipping an unsupported rule.
        NProto::TBlockstoreConfig candidate;
        candidate.MutableVolumeThrottling()->AddRules();
        auto* rule = candidate.MutableVolumeThrottling()->AddRules();
        rule->MutableFilter();
        rule->MutableCoefficients()->SetMaxReadIops(0);
        const auto issues = PrepareDynamicConfigSectionsUpdate(candidate, nullptr);

        // Report the skip without rejecting the retained rule.
        UNIT_ASSERT_VALUES_EQUAL(1, issues.Sections.size());
        const auto& section = issues.Sections[0];
        UNIT_ASSERT_VALUES_EQUAL("VolumeThrottling", section.Path);
        UNIT_ASSERT_VALUES_EQUAL(1, section.Issues.size());
        UNIT_ASSERT_STRING_CONTAINS(section.Issues[0], "Rules[0]");
        UNIT_ASSERT(!HasError(section.RejectionError));
        UNIT_ASSERT(!issues.HasRejectedSections());
        UNIT_ASSERT_VALUES_EQUAL(1, candidate.GetVolumeThrottling().RulesSize());
        UNIT_ASSERT(
            candidate.GetVolumeThrottling()
                .GetRules(0)
                .GetCoefficients()
                .HasMaxReadIops());

        // Produce no diagnostic record when the prepared section is replayed.
        const auto replay = PrepareDynamicConfigSectionsUpdate(candidate, nullptr);
        UNIT_ASSERT(replay.Sections.empty());
        UNIT_ASSERT(!replay.HasRejectedSections());
    }

    // Verify that the rejection and nonfatal issues are reported together for
    // the same invalid section and that the last accepted section is restored
    // when available.
    Y_UNIT_TEST(ShouldKeepRejectionAndIssuesInTheSameSection)
    {
        // Cover rejection with no previous configuration, with a previous
        // configuration that lacks the section, and with a last accepted
        // section.
        NProto::TBlockstoreConfig previous;
        previous.MutableVolumeThrottling()
            ->AddRules()
            ->MutableDisks()
            ->AddDiskIds("removed");
        const auto previousBytes = previous.SerializeAsString();
        const NProto::TBlockstoreConfig empty;
        const NProto::TBlockstoreConfig* previousConfigs[] = {
            nullptr,
            &empty,
            &previous};

        // Use an invalid section for which a nonfatal issue is reported before
        // rejection. Include a valid setting in another section.
        NProto::TBlockstoreConfig invalid;
        invalid.MutableStorageService()->SetWriteBlobThreshold(300);
        invalid.MutableVolumeThrottling()->AddRules();
        auto* rule = invalid.MutableVolumeThrottling()->AddRules();
        rule->MutableFilter();
        rule->MutableCoefficients()->SetMaxReadIops(2);

        for (const auto* accepted: previousConfigs) {
            // Check that the rejection and the nonfatal issue are reported
            // together for the invalid section.
            auto candidate = invalid;
            const auto report =
                PrepareDynamicConfigSectionsUpdate(candidate, accepted);
            UNIT_ASSERT_VALUES_EQUAL(1, report.Sections.size());
            const auto& section = report.Sections[0];
            UNIT_ASSERT_VALUES_EQUAL("VolumeThrottling", section.Path);
            UNIT_ASSERT_VALUES_EQUAL(1, section.Issues.size());
            UNIT_ASSERT(HasError(section.RejectionError));
            UNIT_ASSERT(report.HasRejectedSections());

            // Check that the last accepted section is restored when available
            // and the invalid section is cleared otherwise. Check that the valid
            // setting in the other section is preserved.
            const bool shouldRestore = accepted == &previous;
            UNIT_ASSERT_VALUES_EQUAL(
                shouldRestore,
                candidate.HasVolumeThrottling());
            if (shouldRestore) {
                UNIT_ASSERT_VALUES_EQUAL(
                    previous.GetVolumeThrottling().SerializeAsString(),
                    candidate.GetVolumeThrottling().SerializeAsString());
            }
            UNIT_ASSERT_VALUES_EQUAL(
                300,
                candidate.GetStorageService().GetWriteBlobThreshold());
        }

        // Verify that the last accepted configuration remains intact.
        UNIT_ASSERT_VALUES_EQUAL(previousBytes, previous.SerializeAsString());

        // Check that an update without the section is accepted and the
        // section stays absent even when a last accepted section exists.
        NProto::TBlockstoreConfig candidate;
        const auto reset =
            PrepareDynamicConfigSectionsUpdate(candidate, &previous);
        UNIT_ASSERT(reset.Sections.empty());
        UNIT_ASSERT(!reset.HasRejectedSections());
        UNIT_ASSERT(!candidate.HasVolumeThrottling());
    }
}

}   // namespace NCloud::NBlockStore
