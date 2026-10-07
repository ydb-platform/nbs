#include "tablet_schema.h"

#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/ydb/library/actors/util/rope.h>

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>

#include <vector>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString GenerateValidateData(ui32 size)
{
    TString data(size, 0);
    for (ui32 i = 0; i < size; ++i) {
        data[i] = 'A' + (i % ('Z' - 'A' + 1));
    }
    return data;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_ExternalPayload)
{
    void ShouldPassDataAsPayload(
        const TFileSystemConfig& tabletConfig,
        const TTestEnvConfig& testEnvConfig,
        ui32 dataSize,
        ui32 chunkSize)
    {
        UNIT_ASSERT_GT(chunkSize, 0);

        NProto::TStorageConfig storageConfig;
        storageConfig.SetExternalReadDataPayload(true);
        storageConfig.SetExternalWriteDataPayloadEnabled(true);

        TTestEnv env(testEnvConfig, storageConfig);

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig,
            true /*updateConfig*/,
            storageConfig);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        auto data = GenerateValidateData(dataSize);
        auto request = tablet.CreateWriteDataRequest(
            handle,
            0,
            data.size(),
            data.c_str());
        request->StripPayload();

        TRope rope;
        for (ui32 offset = 0; offset < dataSize;) {
            const ui32 len = Min(chunkSize, dataSize - offset);
            rope.Insert(rope.End(), TRope(TString(data.data() + offset, len)));
            offset += len;
        }
        UNIT_ASSERT_VALUES_EQUAL(rope.IsContiguous(), dataSize <= chunkSize);

        request->AddPayload(std::move(rope));
        tablet.SendRequest(std::move(request));
        tablet.AssertWriteDataResponse(S_OK);
        tablet.Flush();

        auto response = tablet.ReadData(handle, 0, dataSize);
        const auto& buffer = response->Record.GetBuffer();
        UNIT_ASSERT(buffer.empty());
        UNIT_ASSERT_VALUES_EQUAL(data.size(), response->Record.GetLength());
        UNIT_ASSERT_VALUES_EQUAL(1, response->GetPayloadCount());
        auto& payload = response->GetPayload(0);
        UNIT_ASSERT_VALUES_EQUAL(data.size(), payload.size());
        UNIT_ASSERT_VALUES_EQUAL(data, payload.ConvertToString());
    }

    TABLET_TEST(ShouldPassDataAsPayloadTest)
    {
        std::vector<ui32> dataSizes = {124, 1_KB, 64_KB, 100_KB, 256_KB};
        for (auto dataSize : dataSizes) {
            ShouldPassDataAsPayload(
                tabletConfig,
                testEnvConfig,
                dataSize,
                dataSize);
            ShouldPassDataAsPayload(
                tabletConfig,
                testEnvConfig,
                dataSize,
                8_KB);
        }
    }

    void DoTestSoftBackpressurePostponedWriteWithExternalPayload(
        const TFileSystemConfig& tabletConfig,
        const TTestEnvConfig& testEnvConfig,
        ui32 chunkSize)
    {
        const ui32 block = tabletConfig.BlockSize;
        UNIT_ASSERT_GT(chunkSize, 0);

        NProto::TStorageConfig storageConfig;
        storageConfig.SetExternalReadDataPayload(true);
        storageConfig.SetExternalWriteDataPayloadEnabled(true);
        storageConfig.SetThrottlingEnabled(true);
        storageConfig.SetMultipleStageRequestThrottlingEnabled(true);
        storageConfig.SetSoftBackpressureEnabled(true);
        storageConfig.SetFlushThresholdForBackpressureSoft(block);
        storageConfig.SetFlushThresholdForBackpressure(3 * block);

        TTestEnv env(testEnvConfig, storageConfig);
        const ui32 nodeIdx = env.AddDynamicNode();
        const ui64 tabletId = env.BootIndexTablet(nodeIdx);
        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig,
            true /*updateConfig*/,
            storageConfig);
        tablet.InitSession("client", "session");

        TFileSystemConfig config = tabletConfig;
        config.PerformanceProfile.ThrottlingEnabled = true;
        config.PerformanceProfile.MaxReadIops = 20;
        config.PerformanceProfile.MaxWriteIops = 20;
        config.PerformanceProfile.MaxReadBandwidth = block * 4;
        config.PerformanceProfile.MaxWriteBandwidth = block * 4;
        config.PerformanceProfile.MaxPostponedWeight = block;
        config.PerformanceProfile.MaxWriteCostMultiplier = 5;
        config.PerformanceProfile.MaxPostponedTime =
            TDuration::Seconds(25).MilliSeconds();
        config.PerformanceProfile.MaxPostponedCount = 64;
        config.PerformanceProfile.BurstPercentage = 100;
        config.PerformanceProfile.DefaultPostponedRequestWeight = 1_KB;
        tablet.UpdateConfig(config);

        const auto id =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        const ui64 handle = CreateHandle(tablet, id);

        tablet.SendWriteDataRequest(handle, 0, block, 'a');
        tablet.AssertWriteDataQuickResponse(S_OK);
        tablet.SendWriteDataRequest(handle, block, block, 'b');
        tablet.AssertWriteDataQuickResponse(S_OK);

        const auto data = GenerateValidateData(block);
        auto request = tablet.CreateWriteDataRequest(
            handle,
            2 * block,
            data.size(),
            data.data());
        request->StripPayload();

        TRope rope;
        for (ui32 offset = 0; offset < block;) {
            const ui32 len = Min(chunkSize, block - offset);
            rope.Insert(rope.End(), TRope(TString(data.data() + offset, len)));
            offset += len;
        }
        UNIT_ASSERT_VALUES_EQUAL(rope.IsContiguous(), block <= chunkSize);
        request->AddPayload(std::move(rope));
        UNIT_ASSERT(request->Record.GetBuffer().empty());
        UNIT_ASSERT_VALUES_EQUAL(1, request->GetPayloadCount());

        // Soft backpressure makes the third write exceed the remaining quota.
        // Keep the original request queued until the quota is replenished.
        tablet.SendRequest(std::move(request));
        tablet.AssertWriteDataNoResponse();
        tablet.AdvanceTime(TDuration::Seconds(1));
        tablet.AssertWriteDataResponse(S_OK);

        tablet.Flush();
        auto response = tablet.ReadData(handle, 2 * block, block);
        UNIT_ASSERT(response->Record.GetBuffer().empty());
        UNIT_ASSERT_VALUES_EQUAL(data.size(), response->Record.GetLength());
        UNIT_ASSERT_VALUES_EQUAL(1, response->GetPayloadCount());
        UNIT_ASSERT_VALUES_EQUAL(
            data,
            response->GetPayload(0).ConvertToString());
        tablet.DestroyHandle(handle);
    }

    TABLET_TEST_16K(ShouldExecutePostponedWriteWithExternalPayload)
    {
        DoTestSoftBackpressurePostponedWriteWithExternalPayload(
            tabletConfig,
            testEnvConfig,
            1_KB);
        DoTestSoftBackpressurePostponedWriteWithExternalPayload(
            tabletConfig,
            testEnvConfig,
            tabletConfig.BlockSize);
    }
}

}   // namespace NCloud::NFileStore::NStorage
