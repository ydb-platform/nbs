#include "compare_data_action_runner.h"

#include "client_factory.h"
#include "helpers.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/json/proto2json.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

TCompareDataActionRunner::TCompareDataActionRunner(
        TLog& log,
        TAppContext& appContext,
        const TAliasedVolumes& aliasedVolumes,
        NClient::TClientAppConfigPtr clientConfig,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging,
        IRequestStatsPtr requestStats,
        IVolumeStatsPtr volumeStats,
        IClientFactory& clientFactory,
        TTestContext& testContext)
    : Log(log)
    , AppContext(appContext)
    , AliasedVolumes(aliasedVolumes)
    , ClientConfig(std::move(clientConfig))
    , Timer(std::move(timer))
    , Scheduler(std::move(scheduler))
    , Logging(std::move(logging))
    , RequestStats(std::move(requestStats))
    , VolumeStats(std::move(volumeStats))
    , ClientFactory(clientFactory)
    , TestContext(testContext)
{
}

int TCompareDataActionRunner::Run(
    const NProto::TActionGraph::TCompareDataAction& action)
{
    int code = 0;

    try {
        TestContext.Client = ClientFactory.CreateClient({}, action.GetName());
        TestContext.Client->Start();

        Y_ENSURE(
            action.GetExpected().GetVolumeName()
                == action.GetActual().GetVolumeName(),
            "different volume names in Expected/Actual not supported yet"
        );

        auto volumeName = AliasedVolumes.ResolveAlias(
            action.GetExpected().GetVolumeName()
        );

        NClient::TSessionConfig sessionConfig;
        sessionConfig.DiskId = volumeName;
        sessionConfig.AccessMode = NProto::VOLUME_ACCESS_READ_ONLY;
        sessionConfig.MountMode = NProto::VOLUME_MOUNT_REMOTE;
        SetProtoFlag(sessionConfig.MountFlags, NProto::MF_THROTTLING_DISABLED);
        sessionConfig.IpcType = NProto::IPC_GRPC;

        TestContext.Session = CreateSession(
            Timer,
            Scheduler,
            Logging,
            RequestStats,
            VolumeStats,
            TestContext.Client,
            ClientConfig,
            sessionConfig
        );
        STORAGE_INFO("Mount volume: " << volumeName);
        auto response = WaitForCompletion(
            "MountVolume",
            TestContext.Session->MountVolume(),
            {}
        );

        TestContext.Volume = response.GetVolume();

        auto blocksCount = TestContext.Volume.GetBlocksCount();
        const ui64 chunkSize = 1024;
        const ui64 bufferSize = TestContext.Volume.GetBlockSize() * chunkSize;

        TVector<char> expectedBuffer(bufferSize);
        TVector<char> actualBuffer(bufferSize);

        for (ui32 blockIndex = 0; blockIndex < blocksCount; blockIndex += chunkSize) {
            if (AppContext.ShouldStop.load(std::memory_order_acquire)) {
                STORAGE_INFO(
                    "Cancelled block crcs initialization for volume: "
                    << volumeName
                );
                return EC_COMPARE_DATA_ACTION_FAILED;
            }

            ui32 requestBlocks = Min(chunkSize, blocksCount - blockIndex);
            ReadBlocks(
                action.GetExpected(),
                blockIndex,
                requestBlocks,
                &expectedBuffer
            );

            ReadBlocks(
                action.GetActual(),
                blockIndex,
                requestBlocks,
                &actualBuffer
            );

            ui32 lastBadBlock = -1;
            for (ui32 i = 0; i < bufferSize; ++i) {
                if (expectedBuffer[i] != actualBuffer[i]) {
                    const auto block =
                        blockIndex + i / TestContext.Volume.GetBlockSize();
                    if (block != lastBadBlock) {
                        STORAGE_ERROR(
                            "CompareData: inconsistency in block "
                            << block
                            << ", byte " << (i % TestContext.Volume.GetBlockSize())
                            << ", expected=" << ui32(ui8(expectedBuffer[i]))
                            << ", actual=" << ui32(ui8(actualBuffer[i]))
                        );
                        code = EC_COMPARE_DATA_ACTION_FAILED;
                    }
                    lastBadBlock = block;
                }
            }
        }

        STORAGE_INFO("CompareData completed for volume: " << volumeName);

        NProtobufJson::Proto2Json(response, TestContext.Result, {});

        if (TestContext.Volume.GetDiskId()) {
            Y_ABORT_UNLESS(TestContext.Session);
            STORAGE_INFO("Unmount volume: " << TestContext.Volume.GetDiskId());
            WaitForCompletion(
                "UnmountVolume",
                TestContext.Session->UnmountVolume(),
                {}
            );
        }
    } catch (...) {
        STORAGE_ERROR("Exception during CompareData execution: "
            << CurrentExceptionMessage());
        AppContext.FailedTests.fetch_add(1);
        return EC_COMPARE_DATA_ACTION_FAILED;
    }

    if (TestContext.Client) {
        TestContext.Client->Stop();
    }

    return code;
}

void TCompareDataActionRunner::ReadBlocks(
    const TString& checkpointId,
    ui32 blockIndex,
    ui32 blocksCount,
    TVector<char>* buffer)
{
    auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
    request->SetStartIndex(blockIndex);
    request->SetBlocksCount(blocksCount);
    request->SetCheckpointId(checkpointId);
    request->BlockSize = TestContext.Volume.GetBlockSize();
    request->Sglist = TGuardedSgList({{buffer->data(), buffer->size()}});
    auto guardedSgList = request->Sglist;

    auto future = TestContext.Session->ReadBlocksLocal(
        MakeIntrusive<TCallContext>(),
        std::move(request));
    auto response = future.GetValueSync();
    guardedSgList.Close();

    if (HasError(response)) {
        const auto& error = response.GetError();
        STORAGE_ERROR(
            "ReadBlocks failed" <<
            " for disk " << TestContext.Volume.GetDiskId() <<
            " at block " << blockIndex <<
            " range " << blocksCount <<
            " with error " << error.GetCode() <<
            " , message " << error.GetMessage()
        );
        throw yexception()
            << "CompareData failed because of a ReadBlocks failure: "
            << FormatError(error);
    }
}

void TCompareDataActionRunner::ReadBlocks(
    const NProto::TActionGraph::TSource& source,
    ui32 blockIndex,
    ui32 blocksCount,
    TVector<char>* buffer)
{
    ReadBlocks(source.GetCheckpointId(), blockIndex, blocksCount, buffer);

    if (source.GetSecondCheckpointId()) {
        TVector<char> update(buffer->size());
        ReadBlocks(
            source.GetSecondCheckpointId(),
            blockIndex,
            blocksCount,
            &update
        );

        auto request = std::make_shared<NProto::TGetChangedBlocksRequest>();
        request->SetDiskId(TestContext.Volume.GetDiskId());
        request->SetStartIndex(blockIndex);
        request->SetBlocksCount(blocksCount);
        request->SetLowCheckpointId(source.GetCheckpointId());
        request->SetHighCheckpointId(source.GetSecondCheckpointId());

        auto response = WaitForCompletion(
            "GetChangedBlocks",
            TestContext.Client->GetChangedBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request)
            ),
            {}
        );

        for (ui32 i = 0; i < response.GetMask().size(); ++i) {
            for (ui32 j = 0; j < 8; ++j) {
                if (response.GetMask()[i] & (1 << j)) {
                    const auto idx = (i * 8 + j) * TestContext.Volume.GetBlockSize();
                    memcpy(
                        buffer->begin() + idx,
                        update.begin() + idx,
                        TestContext.Volume.GetBlockSize()
                    );
                }
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NLoadTest
