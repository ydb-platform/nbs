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

    // Verify that preparation retains rejected raw VolumeThrottling operations
    // and reports their error together with earlier skipped-rule issues.
    Y_UNIT_TEST(ShouldKeepRejectionAndIssuesInTheSameSection)
    {
        // Preserve a previous deletion operation while accepting another value.
        NProto::TBlockstoreConfig previous;
        previous.MutableVolumeThrottling()
            ->AddRules()
            ->MutableDisks()
            ->AddDiskIds("removed");
        NProto::TBlockstoreConfig candidate;
        candidate.MutableStorageService()->SetWriteBlobThreshold(300);
        candidate.MutableVolumeThrottling()->AddRules();
        auto* rule = candidate.MutableVolumeThrottling()->AddRules();
        rule->MutableFilter();
        rule->MutableCoefficients()->SetMaxReadIops(2);

        // Reject the complete section and retain both kinds of diagnostics.
        const auto issues = PrepareDynamicConfigSectionsUpdate(candidate, &previous);
        UNIT_ASSERT_VALUES_EQUAL(1, issues.Sections.size());
        const auto& section = issues.Sections[0];
        UNIT_ASSERT_VALUES_EQUAL(1, section.Issues.size());
        UNIT_ASSERT(HasError(section.RejectionError));
        UNIT_ASSERT(issues.HasRejectedSections());
        UNIT_ASSERT_STRING_CONTAINS(
            FormatConfigSectionRejection(section),
            "Rejected invalid VolumeThrottling configuration: ");
        UNIT_ASSERT_STRING_CONTAINS(
            section.RejectionError.GetMessage(),
            "VolumeThrottling.Rules[1].Coefficients.MaxReadIops = 2");
        UNIT_ASSERT_VALUES_EQUAL(
            previous.GetVolumeThrottling().SerializeAsString(),
            candidate.GetVolumeThrottling().SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            candidate.GetStorageService().GetWriteBlobThreshold());

        // Clear rejection diagnostics when valid absence resets the section.
        candidate.ClearVolumeThrottling();
        const auto reset = PrepareDynamicConfigSectionsUpdate(candidate, &previous);
        UNIT_ASSERT(reset.Sections.empty());
        UNIT_ASSERT(!reset.HasRejectedSections());
        UNIT_ASSERT(!candidate.HasVolumeThrottling());
    }
}

}   // namespace NCloud::NBlockStore
