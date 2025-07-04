#include "disk_agent.h"
#include "disk_agent_actor.h"

#include "testlib/test_env.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NServer;
using namespace NThreading;

using namespace NDiskAgentTest;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDiskAgentLargeTest)
{
    void ShouldSecureErase5GiBUnitImpl(NProto::EDiskAgentBackendType backend)
    {
        const ui64 totalSize = 5_GB;
        const ui32 blockSize = 4_KB;
        const ui64 totalBlockCount = totalSize / blockSize;
        const TString uuid = "FileDevice-1";
        const auto filePath = TFsPath(GetSystemTempDir()) / "test";

        TFile fileData(filePath, EOpenModeFlag::CreateAlways);
        fileData.Resize(totalSize);

        TTestBasicRuntime runtime;

        runtime.SetScheduledLimit(10'000'000);

        auto env = TTestEnvBuilder(runtime)
            .With([&] {
                auto config = CreateDefaultAgentConfig();
                config.SetBackend(backend);
                config.SetAcquireRequired(true);
                config.SetEnabled(true);

                *config.AddFileDevices() = PrepareFileDevice(
                    filePath,
                    uuid,
                    blockSize,
                    totalBlockCount*blockSize);

                return config;
            }())
            .Build();

        TDiskAgentClient diskAgent(runtime);
        diskAgent.WaitReady();

        const TString sessionId = "session-1";
        diskAgent.AcquireDevices(
            TVector{uuid},
            sessionId,
            NProto::VOLUME_ACCESS_READ_WRITE);

        const ui32 chunkSize = 4_MB;
        const ui32 blocksPerChunk = chunkSize / blockSize;

        auto invokeIO = [&] (auto op) {
            for (ui64 i = 0; i < totalBlockCount; i += 1_GB / blockSize) {
                op(i, Min<ui64>(totalBlockCount - i, blocksPerChunk));
            }
        };

        const TString content(blockSize, 'X');

        TVector<TString> writeBlocks;
        auto writeSgList = ResizeBlocks(
            writeBlocks,
            blocksPerChunk,
            content);

        // write data

        invokeIO([&] (ui64 startIndex, ui64 blockCount) {
            UNIT_ASSERT_VALUES_EQUAL(blockCount, writeSgList.size());

            auto response = WriteDeviceBlocks(
                runtime,
                diskAgent,
                uuid,
                startIndex,
                writeSgList,
                sessionId);
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                response->GetError().GetCode());
        });

        // verify data

        invokeIO([&] (ui64 startIndex, ui64 blockCount) {
            auto response = ReadDeviceBlocks(
                runtime,
                diskAgent,
                uuid,
                startIndex,
                blockCount,
                sessionId);

            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                response->GetError().GetCode());

            UNIT_ASSERT_VALUES_EQUAL(
                blockCount,
                response->Record.GetBlocks().BuffersSize());

            auto data = ConvertToSgList(response->Record.GetBlocks(), blockSize);

            UNIT_ASSERT_VALUES_EQUAL(blockCount, data.size());
            for (TBlockDataRef dr: data) {
                UNIT_ASSERT_VALUES_EQUAL(blockSize, dr.Size());
                UNIT_ASSERT_STRINGS_EQUAL(content, dr.AsStringBuf());
            }
        });

        {
            auto response = diskAgent.CollectStats();

            const auto& stats = response->Stats;
            UNIT_ASSERT_VALUES_EQUAL(1, stats.DeviceStatsSize());
            auto& deviceStats = stats.GetDeviceStats(0);
            UNIT_ASSERT_VALUES_EQUAL("FileDevice-1", deviceStats.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(0, deviceStats.GetBytesZeroed());
            UNIT_ASSERT_VALUES_EQUAL(0, deviceStats.GetNumZeroOps());
        }

        // erase

        diskAgent.ReleaseDevices(TVector{uuid}, sessionId);
        {
            diskAgent.SendSecureEraseDeviceRequest("FileDevice-1");
            auto response = diskAgent.RecvSecureEraseDeviceResponse(TDuration::Max());
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                response->GetError().GetCode());
        }
        diskAgent.AcquireDevices(
            TVector{uuid},
            sessionId,
            NProto::VOLUME_ACCESS_READ_WRITE);

        // verify zeroes

        {
            auto response = diskAgent.CollectStats();

            const auto& stats = response->Stats;
            UNIT_ASSERT_VALUES_EQUAL(1, stats.DeviceStatsSize());
            auto& deviceStats = stats.GetDeviceStats(0);
            UNIT_ASSERT_VALUES_EQUAL("FileDevice-1", deviceStats.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, deviceStats.GetNumEraseOps());
        }

        for (ui64 i = 0; i < totalBlockCount; i += blocksPerChunk) {
            auto response = ReadDeviceBlocks(
                runtime,
                diskAgent,
                uuid,
                i,
                Min<ui32>(totalBlockCount - i, blocksPerChunk),
                sessionId);

            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                response->GetError().GetCode());

            auto data = ConvertToSgList(response->Record.GetBlocks(), blockSize);

            UNIT_ASSERT_VALUES_EQUAL(blocksPerChunk, data.size());
            for (TBlockDataRef dr: data) {
                UNIT_ASSERT_VALUES_EQUAL(blockSize, dr.Size());
                UNIT_ASSERT_VALUES_EQUAL(
                    blockSize,
                    std::count(dr.Data(), dr.Data() + dr.Size(), '\0'));
            }
        }
    }

    Y_UNIT_TEST(ShouldSecureErase5GiBUnitAio)
    {
        ShouldSecureErase5GiBUnitImpl(NProto::DISK_AGENT_BACKEND_AIO);
    }

    Y_UNIT_TEST(ShouldSecureErase5GiBUnitIoUring)
    {
        ShouldSecureErase5GiBUnitImpl(NProto::DISK_AGENT_BACKEND_IO_URING);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
