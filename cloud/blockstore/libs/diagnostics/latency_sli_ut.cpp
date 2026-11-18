#include "config.h"

#include <cloud/storage/core/libs/common/media.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

Y_UNIT_TEST_SUITE(TLatencySliTest)
{
    Y_UNIT_TEST(ShouldUseManualSizeBoundariesAndInclusiveThreshold)
    {
        auto config = TLatencySliConfig::TryParse("1;0:1:4097:100;0:4097:8193:200;1:1:8193:300").value();
        UNIT_ASSERT(config.Classify(false, 4096, 100, false) == ELatencySliResult::Good);
        UNIT_ASSERT(config.Classify(false, 4096, 101, false) == ELatencySliResult::Bad);
        UNIT_ASSERT(config.Classify(false, 4097, 200, false) == ELatencySliResult::Good);
        UNIT_ASSERT(config.Classify(false, 8193, 1, false) == ELatencySliResult::Unknown);
        UNIT_ASSERT(config.Classify(true, 4096, 250, false) == ELatencySliResult::Good);
        UNIT_ASSERT_VALUES_EQUAL(config.Serialize(), TLatencySliConfig::TryParse(config.Serialize()).value().Serialize());
    }

    Y_UNIT_TEST(ShouldRejectInvalidOrOverlappingTables)
    {
        for (const TString value: {"1;0:5:5:100", "1;0:1:9:0", "1;0:1:9:100;0:8:10:200"}) {
            UNIT_ASSERT_C(!TLatencySliConfig::TryParse(value), value);
        }
        NProto::TDiagnosticsConfig proto;
        proto.SetEnableLatencySli(true);
        auto* row = proto.AddLatencySliThresholds();
        row->SetMediaKind(::NCloud::NProto::STORAGE_MEDIA_SSD);
        row->SetStartBytes(1);
        row->SetEndBytes(8193);
        row->SetThresholdUs(100);
        TDiagnosticsConfig config(proto);
        UNIT_ASSERT(config.GetLatencySliConfig(::NCloud::NProto::STORAGE_MEDIA_HDD).Thresholds[0].empty());
    }

    Y_UNIT_TEST(ShouldRejectMalformedChildConfigWithoutThrowing)
    {
        for (const TString value: {
                 ";", ";;", ";1", "1;", "1;;0:1:9:100", "2;0:1:9:100",
                 "1;0:1:9", "1;0:1:9:100:200", "1;0::9:100",
                 "1;0:1:9:", "1;2:1:9:100", "1;-1:1:9:100",
                 "1;0:-1:9:100", "1;0:1:9:-1", "1;0:1:9:abc",
                 "1;0:1:9:18446744073709551616"})
        {
            UNIT_ASSERT_C(!TLatencySliConfig::TryParse(value), value);
        }
        const auto disabled = TLatencySliConfig::TryParse("");
        UNIT_ASSERT(disabled);
        UNIT_ASSERT(!disabled->Enabled);
        const auto empty = TLatencySliConfig::TryParse("1");
        UNIT_ASSERT(empty);
        UNIT_ASSERT(empty->Enabled);
        UNIT_ASSERT_VALUES_EQUAL("1", empty->Serialize());
    }

    Y_UNIT_TEST(ShouldLeaveRequestsWithoutThresholdsUnjudged)
    {
        auto config = TLatencySliConfig::TryParse("1;0:1:8193:100").value();
        for (bool failed: {false, true}) {
            const auto expected = ELatencySliResult::Unknown;
            UNIT_ASSERT(config.Classify(true, 4096, 100, failed) == expected);
        }
        UNIT_ASSERT(config.Classify(false, 4096, 1000, false) == ELatencySliResult::Bad);
        UNIT_ASSERT(config.Classify(false, 4096, 1, true) == ELatencySliResult::Bad);
    }
}

}   // namespace NCloud::NBlockStore
