#include "service_ut_helpers.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/model/utils.h>
#include <cloud/filestore/libs/storage/testlib/service_client.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>
#include <cloud/filestore/libs/storage/testlib/ut_helpers.h>
#include <cloud/filestore/private/api/protos/actions.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/base/tablet.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/json_util.h>

#include <util/digest/city.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TShardedFileSystemConfig
{
    const TString FsId = "test";
    const TString Shard1Id = FsId + "_s1";
    const ui64 ShardBlockCount = 1'000;
    const ui64 MainFsBlockCount = ShardBlockCount;
};

////////////////////////////////////////////////////////////////////////////////

struct TTestSetup
{
    NProto::TStorageConfig StorageConfig;
    std::unique_ptr<TTestEnv> Env;
    ui32 NodeIdx = 0;
    std::unique_ptr<TServiceClient> Service;
    THeaders Headers;
    ui64 NodeId = 0;
    ui64 Handle = 0;
    TString FileSystemId = "test";

    explicit TTestSetup(
        bool unconfirmedEnabled = true,
        bool threeStageWriteEnabled = true,
        bool unalignedThreeStageWriteEnabled = true,
        IProfileLogPtr profileLog = CreateProfileLogStub())
    {
        StorageConfig.SetAddingUnconfirmedDataEnabled(unconfirmedEnabled);
        StorageConfig.SetUnconfirmedDataCountHardLimit(100);
        StorageConfig.SetWriteBlobThreshold(128_KB);
        StorageConfig.SetThreeStageWriteEnabled(threeStageWriteEnabled);
        StorageConfig.SetUnalignedThreeStageWriteEnabled(
            unalignedThreeStageWriteEnabled);

        Env = std::make_unique<TTestEnv>(
            TTestEnvConfig{},
            StorageConfig,
            NKikimr::NFake::TCaches{},
            std::move(profileLog));
        NodeIdx = Env->AddDynamicNode();
        Service = std::make_unique<TServiceClient>(Env->GetRuntime(), NodeIdx);
        Service->CreateFileStore(
            FileSystemId,
            10000,
            DefaultBlockSize,
            NProto::EStorageMediaKind::STORAGE_MEDIA_SSD);

        Headers = Service->InitSession(FileSystemId, "client");

        NodeId =
            Service
                ->CreateNode(Headers, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();

        Handle = Service
                     ->CreateHandle(
                         Headers,
                         FileSystemId,
                         NodeId,
                         "",
                         TCreateHandleArgs::RDWR)
                     ->Record.GetHandle();
    }

    TTestActorRuntime& GetRuntime()
    {
        return Env->GetRuntime();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCriticalEventLogBackend final: public TLogBackend
{
private:
    TVector<TString> Messages;

public:
    void WriteData(const TLogRecord& rec) override
    {
        Messages.emplace_back(rec.Data);
    }

    void ReopenLog() override
    {}

    bool Contains(TStringBuf pattern) const
    {
        for (const auto& message: Messages) {
            if (message.Contains(pattern)) {
                return true;
            }
        }
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TEvService::TEvWriteDataResponse>
WriteData(TTestSetup& setup, ui64 offset, const TString& data)
{
    return setup.Service->WriteData(
        setup.Headers,
        setup.FileSystemId,
        setup.NodeId,
        setup.Handle,
        offset,
        data);
}

std::unique_ptr<TEvService::TEvWriteDataResponse>
WriteData(TTestSetup& setup, ui64 offset, const TVector<TString>& iovecs)
{
    return setup.Service->WriteData(
        setup.Headers,
        setup.FileSystemId,
        setup.NodeId,
        setup.Handle,
        offset,
        iovecs);
}

std::unique_ptr<TEvService::TEvReadDataResponse>
ReadData(TTestSetup& setup, ui64 offset, ui64 bytes)
{
    return setup.Service->ReadData(
        setup.Headers,
        setup.FileSystemId,
        setup.NodeId,
        setup.Handle,
        offset,
        bytes);
}

NProtoPrivate::TConfigureUnconfirmedWriteForgetResponse
ConfigureUnconfirmedWriteForget(
    TTestSetup& setup,
    ui64 requestsToForget)
{
    NProtoPrivate::TConfigureUnconfirmedWriteForgetRequest request;
    request.SetRequestsToForget(requestsToForget);

    TString input;
    google::protobuf::util::MessageToJsonString(request, &input);

    auto actionResponse =
        setup.Service->ExecuteAction("configureunconfirmedwriteforget", input);
    UNIT_ASSERT_VALUES_EQUAL(S_OK, actionResponse->GetStatus());

    NProtoPrivate::TConfigureUnconfirmedWriteForgetResponse response;
    UNIT_ASSERT(
        google::protobuf::util::JsonStringToMessage(
            actionResponse->Record.GetOutput(),
            &response)
            .ok());
    return response;
}

NProtoPrivate::TStorageStats GetStorageStats(TTestSetup& setup)
{
    const auto tabletId =
        setup.Service->GetFileStoreInfo(setup.FileSystemId)
            ->Record.GetFileStore()
            .GetMainTabletId();
    TIndexTabletClient tablet(setup.GetRuntime(), setup.NodeIdx, tabletId);
    return GetStorageStats(tablet);
}

enum class EShardPipeToDisconnect
{
    // Pipe between the main filesystem tablet and the shard tablet, used for
    // shard control requests such as CreateSession.
    Control,
    // Pipe between the service write actor and the shard tablet, used by the
    // data write path such as GenerateBlobIds.
    Data,
};

void DoShouldDeleteShardUnconfirmedDataOnServicePipeDisconnect(
    EShardPipeToDisconnect pipeToDisconnect)
{
    TShardedFileSystemConfig fsConfig;

    NProto::TStorageConfig storageConfig;
    storageConfig.SetAutomaticShardCreationEnabled(true);
    storageConfig.SetAutomaticallyCreatedShardSize(1_GB);
    storageConfig.SetShardAllocationUnit(1_GB);
    storageConfig.SetShardBalancerPolicy(NProto::SBP_ROUND_ROBIN);
    storageConfig.SetAddingUnconfirmedDataEnabled(true);
    storageConfig.SetUnconfirmedDataCountHardLimit(100);
    storageConfig.SetWriteBlobThreshold(128_KB);
    storageConfig.SetThreeStageWriteEnabled(true);
    storageConfig.SetUnalignedThreeStageWriteEnabled(true);

    TTestEnvConfig envConfig;
    envConfig.DynamicNodes = 2;
    TTestEnv env(envConfig, storageConfig);

    const ui32 fsNodeIdx = env.AddDynamicNode();

    TServiceClient fsService(env.GetRuntime(), fsNodeIdx);
    fsService.CreateFileStore(fsConfig.FsId, fsConfig.MainFsBlockCount);

    auto& runtime = env.GetRuntime();
    ui64 shardTabletId = 0;
    const TString targetClientId = "client";

    TMap<TActorId, TActorId> shardPipeClientsByServer;
    TActorId shardControlPipeServer;
    TActorId shardControlClient;
    TActorId shardDataPipeServer;
    TActorId shardDataClient;
    TActorId writeActor;
    TString shardControlSessionId;
    TString shardDataSessionId;
    TAutoPtr<IEventHandle> blockedPutResult;

    // Capture the shard control pipe and the independent shard data pipe.
    // The blob put result is held to keep the write actor alive while the test
    // disconnects one of those pipes.
    auto prevFilter = runtime.SetEventFilter(
        [&](auto& runtime, TAutoPtr<IEventHandle>& event)
        {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvTabletPipe::EvClientConnected: {
                    const auto* msg =
                        event->Get<TEvTabletPipe::TEvClientConnected>();
                    if (msg->Status == NKikimrProto::OK) {
                        shardPipeClientsByServer[msg->ServerId] = msg->ClientId;
                    }
                    break;
                }

                case TEvTabletPipe::EvServerConnected: {
                    const auto* msg =
                        event->Get<TEvTabletPipe::TEvServerConnected>();
                    shardPipeClientsByServer[msg->ServerId] = msg->ClientId;
                    break;
                }

                case TEvIndexTablet::EvCreateSessionRequest: {
                    if (event->Recipient == MakeIndexTabletProxyServiceId()) {
                        break;
                    }

                    const auto* msg =
                        event->Get<TEvIndexTablet::TEvCreateSessionRequest>();
                    if (msg->Record.GetFileSystemId() == fsConfig.Shard1Id &&
                        msg->Record.GetHeaders().GetClientId() ==
                            targetClientId &&
                        !shardControlPipeServer)
                    {
                        shardControlPipeServer = event->Recipient;
                        shardControlClient = event->Sender;
                        shardControlSessionId =
                            msg->Record.GetHeaders().GetSessionId();
                    }

                    break;
                }

                case TEvIndexTablet::EvGenerateBlobIdsRequest: {
                    const auto* msg =
                        event->Get<TEvIndexTablet::TEvGenerateBlobIdsRequest>();
                    if (msg->Record.GetFileSystemId() != fsConfig.Shard1Id ||
                        msg->Record.GetHeaders().GetClientId() !=
                            targetClientId)
                    {
                        break;
                    }

                    if (event->Recipient == MakeIndexTabletProxyServiceId()) {
                        writeActor = event->Sender;
                        break;
                    }

                    shardDataPipeServer = event->Recipient;
                    shardDataClient = event->Sender;
                    shardDataSessionId =
                        msg->Record.GetHeaders().GetSessionId();
                    break;
                }

                case TEvBlobStorage::EvPutResult: {
                    if (writeActor && event->Recipient == writeActor &&
                        !blockedPutResult)
                    {
                        blockedPutResult = event.Release();
                        return true;
                    }
                    break;
                }
            }

            return false;
        });

    fsService.ResizeFileStore(
        fsConfig.FsId,
        fsConfig.MainFsBlockCount,
        false /* force */,
        1 /* shardCount */);
    WaitForTabletStart(fsService);

    const auto mainTabletId = fsService.GetFileStoreInfo(fsConfig.FsId)
                                  ->Record.GetFileStore()
                                  .GetMainTabletId();
    const auto mainTabletActorId = ResolveTablet(runtime, mainTabletId);

    shardTabletId = fsService.GetFileStoreInfo(fsConfig.Shard1Id)
                        ->Record.GetFileStore()
                        .GetMainTabletId();
    UNIT_ASSERT(shardTabletId);

    // Create a file on the shard and write through the service. The write
    // service is placed on a node different from the main tablet node, so the
    // data path uses a different tablet pipe than the shard control path.
    const ui32 serviceNodeIdx = env.AddDynamicNode();
    UNIT_ASSERT_VALUES_UNEQUAL(
        mainTabletActorId.NodeId(),
        runtime.GetNodeId(serviceNodeIdx));
    TServiceClient service(env.GetRuntime(), serviceNodeIdx);
    auto headers = service.InitSession(fsConfig.FsId, targetClientId);

    const auto nodeId =
        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
            ->Record.GetNode()
            .GetId();
    UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(nodeId));

    const auto handle = service
                            .CreateHandle(
                                headers,
                                fsConfig.FsId,
                                nodeId,
                                "",
                                TCreateHandleArgs::RDWR)
                            ->Record.GetHandle();

    service.SendWriteDataRequest(
        headers,
        fsConfig.FsId,
        nodeId,
        handle,
        0,
        GenerateValidateData(256_KB));

    UNIT_ASSERT_C(
        runtime.DispatchEvents(
            TDispatchOptions{
                .CustomFinalCondition =
                    [&]()
                {
                    return !!blockedPutResult && !!shardControlPipeServer &&
                           !!shardDataPipeServer;
                }},
            TDuration::Seconds(5)),
        "Timed out waiting for blocked write and shard pipe capture");

    // Verify that the test captured the exact two-pipe situation it is about to
    // use: same logical session, different tablet pipe servers.
    UNIT_ASSERT_C(blockedPutResult, "Write blob result was not blocked");
    UNIT_ASSERT_C(
        shardControlPipeServer,
        "Shard control pipe CreateSession request was not observed");
    UNIT_ASSERT_C(
        shardDataPipeServer,
        "Shard data pipe GenerateBlobIds request was not observed");
    UNIT_ASSERT_VALUES_EQUAL(headers.SessionId, shardControlSessionId);
    UNIT_ASSERT_VALUES_EQUAL(headers.SessionId, shardDataSessionId);
    UNIT_ASSERT_C(
        shardControlPipeServer != shardDataPipeServer,
        TStringBuilder()
            << "Shard control path and data path used the same pipe server: "
            << shardControlPipeServer);
    UNIT_ASSERT_VALUES_EQUAL(
        mainTabletActorId.NodeId(),
        shardControlClient.NodeId());
    UNIT_ASSERT_VALUES_EQUAL(
        runtime.GetNodeId(serviceNodeIdx),
        shardDataClient.NodeId());

    const auto controlPipeClientIt =
        shardPipeClientsByServer.find(shardControlPipeServer);
    UNIT_ASSERT_C(
        controlPipeClientIt != shardPipeClientsByServer.end(),
        TStringBuilder()
            << "Pipe client for shard control pipe server was not observed: "
            << shardControlPipeServer);

    const auto dataPipeClientIt =
        shardPipeClientsByServer.find(shardDataPipeServer);
    UNIT_ASSERT_C(
        dataPipeClientIt != shardPipeClientsByServer.end(),
        TStringBuilder()
            << "Pipe client for shard data pipe server was not observed: "
            << shardDataPipeServer);

    TIndexTabletClient shardTablet(
        runtime,
        serviceNodeIdx,
        shardTabletId,
        {},
        false /* updateConfig */);

    // At this point the write actor is blocked on blob storage, so the
    // unconfirmed entry must stay visible until either the data pipe or the
    // control pipe is disconnected.
    UNIT_ASSERT_VALUES_EQUAL(
        1,
        GetStorageStats(shardTablet).GetUnconfirmedDataCount());

    // Select either the shard control pipe or the active data pipe.
    struct TPipeToDisconnect
    {
        TActorId Server;
        TActorId Client;
        ui32 NodeIdx = 0;
    };

    const TPipeToDisconnect controlPipeToDisconnect{
        shardControlPipeServer,
        controlPipeClientIt->second,
        fsNodeIdx};
    const TPipeToDisconnect dataPipeToDisconnect{
        shardDataPipeServer,
        dataPipeClientIt->second,
        serviceNodeIdx};

    const TPipeToDisconnect pipeToDisconnectInfo =
        pipeToDisconnect == EShardPipeToDisconnect::Control
            ? controlPipeToDisconnect
            : dataPipeToDisconnect;

    runtime.ClosePipe(
        pipeToDisconnectInfo.Client,
        TActorId(),
        pipeToDisconnectInfo.NodeIdx);

    // Wait until the shard observes the selected tablet pipe server
    // disconnect.
    TDispatchOptions disconnectOptions;
    disconnectOptions.FinalEvents = {TDispatchOptions::TFinalEventCondition(
        [server = pipeToDisconnectInfo.Server](IEventHandle& event)
        {
            if (event.GetTypeRewrite() != TEvTabletPipe::EvServerDisconnected) {
                return false;
            }

            const auto* msg = event.Get<TEvTabletPipe::TEvServerDisconnected>();
            return msg->ServerId == server;
        })};
    UNIT_ASSERT_C(
        runtime.DispatchEvents(disconnectOptions, TDuration::Seconds(5)),
        TStringBuilder() << "Timed out waiting for shard pipe disconnect "
                         << pipeToDisconnectInfo.Server);

    // The disconnect dispatch can already execute and commit the cleanup tx.
    // Querying stats after it is the synchronization point for the assertion.
    UNIT_ASSERT_VALUES_EQUAL(
        0,
        GetStorageStats(shardTablet).GetUnconfirmedDataCount());

    runtime.SetEventFilter(prevFilter);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TWriteDataUnconfirmedTest)
{
    // =========================================================================
    // Success path tests
    // =========================================================================

    Y_UNIT_TEST(ShouldUseUnconfirmedFlowWhenEnabled)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        bool unconfirmedFlowEnabled = false;

        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                    TEvIndexTablet::EvGenerateBlobIdsResponse)
                {
                    auto* msg = event->template Get<
                        TEvIndexTablet::TEvGenerateBlobIdsResponse>();
                    unconfirmedFlowEnabled =
                        msg->Record.GetUnconfirmedFlowEnabled();
                }
                return false;
            });

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);
        UNIT_ASSERT_C(
            unconfirmedFlowEnabled,
            "Tablet should enable unconfirmed data");

        UNIT_ASSERT_C(
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest) >= 1,
            "ConfirmAddData should be called");

        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch");
    }

    Y_UNIT_TEST(ShouldWriteLargeDataWithMultipleBlobs)
    {
        TTestSetup setup;

        TString data =
            GenerateValidateData(DefaultBlockSize * BlockGroupSize * 2);
        WriteData(setup, 0, data);
        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch");
        const auto stat = setup.Service
                              ->GetNodeAttr(
                                  setup.Headers,
                                  setup.FileSystemId,
                                  RootNodeId,
                                  "file")
                              ->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(data.size(), stat.GetSize());
    }

    Y_UNIT_TEST(ShouldWriteDataWithUnalignedHead)
    {
        TTestSetup setup;

        const ui64 offset = 100;
        TString data = GenerateValidateData(256_KB);

        WriteData(setup, offset, data);
        const auto actualResponse = ReadData(setup, offset, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch");

        const auto firstBlockResponse = ReadData(setup, 0, 4_KB);
        const auto firstBlock = firstBlockResponse->Record.GetBuffer();
        TString expectedZeros(offset, '\0');
        UNIT_ASSERT_VALUES_EQUAL(
            expectedZeros,
            TString(firstBlock.substr(0, offset)));
    }

    Y_UNIT_TEST(ShouldWriteDataWithUnalignedTail)
    {
        TTestSetup setup;

        TString data = GenerateValidateData(256_KB + 100);

        WriteData(setup, 0, data);
        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch");
        const auto stat = setup.Service
                              ->GetNodeAttr(
                                  setup.Headers,
                                  setup.FileSystemId,
                                  RootNodeId,
                                  "file")
                              ->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(data.size(), stat.GetSize());
    }

    Y_UNIT_TEST(ShouldWriteDataWithUnalignedHeadAndTail)
    {
        TTestSetup setup;

        const ui64 offset = 100;
        const ui64 dataSize = 512_KB + 200;
        TString data = GenerateValidateData(dataSize, 42);

        WriteData(setup, offset, data);
        const auto actualResponse = ReadData(setup, offset, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch");
        const auto stat = setup.Service
                              ->GetNodeAttr(
                                  setup.Headers,
                                  setup.FileSystemId,
                                  RootNodeId,
                                  "file")
                              ->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(offset + data.size(), stat.GetSize());

        const auto firstBlockResponse = ReadData(setup, 0, 4_KB);
        const auto firstBlock = firstBlockResponse->Record.GetBuffer();

        TString expectedZeros(offset, '\0');
        UNIT_ASSERT_VALUES_EQUAL(
            expectedZeros,
            TString(firstBlock.substr(0, offset)));
    }

    Y_UNIT_TEST(ShouldWriteWithIovecs)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        // Track whether unconfirmed data is allowed
        bool unconfirmedFlowEnabled = false;

        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                    TEvIndexTablet::EvGenerateBlobIdsResponse)
                {
                    auto* msg = event->template Get<
                        TEvIndexTablet::TEvGenerateBlobIdsResponse>();
                    unconfirmedFlowEnabled =
                        msg->Record.GetUnconfirmedFlowEnabled();
                }
                return false;
            });

        TVector<TString> iovecs;
        for (ui32 i = 0; i < 8; ++i) {
            iovecs.push_back(GenerateValidateData(32_KB, i));
        }

        WriteData(setup, 0, iovecs);

        UNIT_ASSERT_C(
            runtime.GetCounter(TEvIndexTablet::EvGenerateBlobIdsRequest) >= 1,
            "Three-stage write should trigger GenerateBlobIds");

        UNIT_ASSERT_C(
            unconfirmedFlowEnabled,
            "Tablet should enable unconfirmed data for iovecs");

        UNIT_ASSERT_C(
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest) >= 1,
            "ConfirmAddData should be called in unconfirmed flow");

        ui64 offset = 0;
        for (const auto& chunk: iovecs) {
            const auto readBufferResponse =
                ReadData(setup, offset, chunk.size());
            const auto readBuffer = readBufferResponse->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL_C(
                CityHash64(chunk),
                CityHash64(readBuffer),
                "Data mismatch at offset " << offset);
            offset += chunk.size();
        }
    }

    Y_UNIT_TEST(ShouldHandleMultipleWritesSequentially)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        for (ui32 i = 0; i < 3; ++i) {
            TString data = GenerateValidateData(256_KB, i);
            WriteData(setup, i * 256_KB, data);
            const auto readDataResponse =
                ReadData(setup, i * 256_KB, data.size());
            const auto readResponse = readDataResponse->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL_C(
                CityHash64(data),
                CityHash64(readResponse),
                "Data mismatch at write " << i);
        }

        UNIT_ASSERT_C(
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest) >= 3,
            "ConfirmAddData should be called 3 times for 3 writes");
    }

    Y_UNIT_TEST(ShouldForgetUnconfirmedCommitWhenArmed)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        auto state = ConfigureUnconfirmedWriteForget(
            setup,
            1 /* requestsToForget */);
        UNIT_ASSERT_VALUES_EQUAL(1, state.GetRequestsToForget());

        ui32 confirmRequests = 0;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->Recipient != MakeIndexTabletProxyServiceId()) {
                    return false;
                }

                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvConfirmAddDataRequest:
                        ++confirmRequests;
                        break;
                }

                return false;
            });

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);

        UNIT_ASSERT_VALUES_EQUAL(0, confirmRequests);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            GetStorageStats(setup).GetUnconfirmedDataCount());

        state = ConfigureUnconfirmedWriteForget(setup, 0 /* requestsToForget */);
        UNIT_ASSERT_VALUES_EQUAL(0, state.GetRequestsToForget());
    }

    // =========================================================================
    // Flow control tests
    // =========================================================================

    Y_UNIT_TEST(ShouldNotUseUnconfirmedFlowWhenDisabled)
    {
        TTestSetup setup(false);
        auto& runtime = setup.GetRuntime();

        bool unconfirmedFlowEnabled = false;

        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                    TEvIndexTablet::EvGenerateBlobIdsResponse)
                {
                    auto* msg = event->template Get<
                        TEvIndexTablet::TEvGenerateBlobIdsResponse>();
                    unconfirmedFlowEnabled =
                        msg->Record.GetUnconfirmedFlowEnabled();
                }
                return false;
            });

        // Use unaligned offset to verify that session-level disable still
        // forces regular AddData flow.
        const ui64 offset = 100;
        TString data = GenerateValidateData(256_KB);
        WriteData(setup, offset, data);

        UNIT_ASSERT_C(
            !unconfirmedFlowEnabled,
            "Unconfirmed data should be disabled");

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest));
        UNIT_ASSERT_C(
            runtime.GetCounter(TEvIndexTablet::EvAddDataRequest) >= 1,
            "AddData should be called in regular flow");

        const auto actualResponse = ReadData(setup, offset, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch");
    }

    Y_UNIT_TEST(ShouldNotUseUnconfirmedFlowWhenTabletRejects)
    {
        // Session has unconfirmed enabled, but the tablet clears
        // UnconfirmedFlowEnabled in the response (e.g. hit hard limit).
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                    TEvIndexTablet::EvGenerateBlobIdsResponse)
                {
                    auto* msg = event->template Get<
                        TEvIndexTablet::TEvGenerateBlobIdsResponse>();
                    msg->Record.SetUnconfirmedFlowEnabled(false);
                }
                return false;
            });

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);

        UNIT_ASSERT_VALUES_EQUAL_C(
            0,
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest),
            "ConfirmAddData should not be called when tablet rejects");

        UNIT_ASSERT_C(
            runtime.GetCounter(TEvIndexTablet::EvAddDataRequest) >= 1,
            "AddData should be called in regular flow");

        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch");
    }

    Y_UNIT_TEST(ShouldNotCallConfirmAddDataInRegularFlow)
    {
        TTestSetup setup(false /*unconfirmedEnabled*/);
        auto& runtime = setup.GetRuntime();

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);

        UNIT_ASSERT_C(
            runtime.GetCounter(TEvIndexTablet::EvAddDataRequest) >= 1,
            "AddData should be called");
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest));
    }

    // =========================================================================
    // Error handling and fallback tests
    // =========================================================================

    Y_UNIT_TEST(ShouldFallbackOnGenerateBlobIdsError)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        NProto::TError error;
        error.SetCode(E_REJECTED);
        error.SetMessage("test error");

        bool errorInjected = false;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                        TEvIndexTablet::EvGenerateBlobIdsResponse &&
                    !errorInjected)
                {
                    errorInjected = true;
                    auto* msg = event->template Get<
                        TEvIndexTablet::TEvGenerateBlobIdsResponse>();
                    msg->Record.MutableError()->CopyFrom(error);
                }
                return false;
            });

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);

        UNIT_ASSERT_VALUES_EQUAL_C(
            0,
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest),
            "ConfirmAddData should not be called when GenerateBlobIds fails");

        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch after fallback");
    }

    Y_UNIT_TEST(ShouldFallbackOnWriteBlobError)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        NActors::TActorId worker;
        ui32 evPuts = 0;
        ui32 cancelAddDataRequests = 0;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvGenerateBlobIdsRequest: {
                        if (!worker) {
                            worker = event->Sender;
                        }
                        break;
                    }
                    case NKikimr::TEvBlobStorage::EvPutResult: {
                        auto* msg = event->template Get<
                            NKikimr::TEvBlobStorage::TEvPutResult>();
                        if (event->Recipient == worker && evPuts == 0) {
                            msg->Status = NKikimrProto::ERROR;
                        }
                        if (event->Recipient == worker) {
                            ++evPuts;
                        }
                        break;
                    }
                    case TEvIndexTablet::EvCancelAddDataRequest: {
                        if (event->Recipient ==
                            MakeIndexTabletProxyServiceId()) {
                            ++cancelAddDataRequests;
                        }
                        break;
                    }
                }
                return false;
            });

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);

        UNIT_ASSERT_VALUES_EQUAL_C(
            0,
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest),
            "ConfirmAddData should not be called after WriteBlob error");
        UNIT_ASSERT_VALUES_EQUAL_C(
            1,
            cancelAddDataRequests,
            "CancelAddData should be called once after WriteBlob error");

        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch after fallback");
        UNIT_ASSERT_VALUES_EQUAL(1, evPuts);
    }

    Y_UNIT_TEST(ShouldProfileCancelAddDataAsSeparateRequestType)
    {
        const auto profileLog = std::make_shared<TTestProfileLog>();
        TTestSetup setup(
            true,   // unconfirmedEnabled
            true,   // threeStageWriteEnabled
            true,   // unalignedThreeStageWriteEnabled
            profileLog);
        auto& runtime = setup.GetRuntime();

        NActors::TActorId worker;
        bool enableWriteBlobErrorInjection = false;
        bool writeBlobErrorInjected = false;
        bool targetCommitIdCaptured = false;
        ui64 targetCommitId = 0;
        const ui32 confirmAddDataType =
            static_cast<ui32>(EFileStoreRequest::ConfirmAddData);
        const ui32 cancelAddDataType =
            static_cast<ui32>(EFileStoreRequest::CancelAddData);
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvGenerateBlobIdsRequest: {
                        if (enableWriteBlobErrorInjection && !worker) {
                            worker = event->Sender;
                        }
                        break;
                    }
                    case TEvIndexTablet::EvGenerateBlobIdsResponse: {
                        if (enableWriteBlobErrorInjection &&
                            !targetCommitIdCaptured)
                        {
                            auto* msg = event->template Get<
                                TEvIndexTablet::TEvGenerateBlobIdsResponse>();
                            targetCommitId = msg->Record.GetCommitId();
                            targetCommitIdCaptured = true;
                        }
                        break;
                    }
                    // Ensure that we corrupt needed Put and not random one.
                    case NKikimr::TEvBlobStorage::EvPutResult: {
                        auto* msg = event->template Get<
                            NKikimr::TEvBlobStorage::TEvPutResult>();
                        if (enableWriteBlobErrorInjection &&
                            event->Recipient == worker &&
                            targetCommitIdCaptured &&
                            MakePartialBlobId(msg->Id).CommitId() ==
                                targetCommitId &&
                            !writeBlobErrorInjected)
                        {
                            msg->Status = NKikimrProto::ERROR;
                            writeBlobErrorInjected = true;
                        }
                        break;
                    }
                }
                return false;
            });

        const auto confirmBefore =
            profileLog->Requests[confirmAddDataType].size();
        const auto cancelBefore =
            profileLog->Requests[cancelAddDataType].size();

        // Successful unconfirmed flow: ConfirmAddData should be profiled.
        TString firstData = GenerateValidateData(256_KB, 1);
        WriteData(setup, 0, firstData);
        UNIT_ASSERT_C(
            profileLog->Requests[confirmAddDataType].size() >=
                confirmBefore + 1,
            "ConfirmAddData must be logged under its own request type");
        UNIT_ASSERT_VALUES_EQUAL(
            cancelBefore,
            profileLog->Requests[cancelAddDataType].size());

        // WriteBlob error path: CancelAddData should be profiled, but not
        // ConfirmAddData for this write.
        const auto confirmAfterFirstWrite =
            profileLog->Requests[confirmAddDataType].size();
        const auto cancelAfterFirstWrite =
            profileLog->Requests[cancelAddDataType].size();
        enableWriteBlobErrorInjection = true;
        writeBlobErrorInjected = false;

        TString secondData = GenerateValidateData(256_KB, 2);
        WriteData(setup, 0, secondData);
        UNIT_ASSERT_C(
            writeBlobErrorInjected,
            "WriteBlob error should be injected for CancelAddData path");
        UNIT_ASSERT_VALUES_EQUAL(
            confirmAfterFirstWrite,
            profileLog->Requests[confirmAddDataType].size());
        UNIT_ASSERT_C(
            profileLog->Requests[cancelAddDataType].size() >=
                cancelAfterFirstWrite + 1,
            "CancelAddData must be logged under its own request type");
    }

    Y_UNIT_TEST(ShouldRetryCancelAddDataOnProxyErrorAndThenFallback)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        NProto::TError error;
        error.SetCode(E_REJECTED);
        error.SetMessage("proxy test error");

        NActors::TActorId worker;
        ui32 evPuts = 0;
        ui32 cancelAddDataRequests = 0;
        ui32 fallbackWriteDataRequests = 0;
        bool errorInjected = false;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvGenerateBlobIdsRequest: {
                        if (!worker) {
                            worker = event->Sender;
                        }
                        break;
                    }
                    case NKikimr::TEvBlobStorage::EvPutResult: {
                        auto* msg = event->template Get<
                            NKikimr::TEvBlobStorage::TEvPutResult>();
                        if (event->Recipient == worker && evPuts == 0) {
                            msg->Status = NKikimrProto::ERROR;
                        }
                        if (event->Recipient == worker) {
                            ++evPuts;
                        }
                        break;
                    }
                    case TEvIndexTablet::EvCancelAddDataRequest: {
                        if (event->Recipient ==
                            MakeIndexTabletProxyServiceId()) {
                            ++cancelAddDataRequests;
                        }
                        break;
                    }
                    case TEvIndexTablet::EvCancelAddDataResponse: {
                        if (!errorInjected) {
                            errorInjected = true;
                            auto* msg = event->template Get<
                                TEvIndexTablet::TEvCancelAddDataResponse>();
                            msg->Record.MutableError()->CopyFrom(error);
                            event->Sender = MakeIndexTabletProxyServiceId();
                        }
                        break;
                    }
                    case TEvService::EvWriteDataRequest: {
                        if (event->Recipient ==
                            MakeIndexTabletProxyServiceId()) {
                            ++fallbackWriteDataRequests;
                        }
                        break;
                    }
                }
                return false;
            });

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);
        UNIT_ASSERT_C(
            cancelAddDataRequests >= 2,
            "CancelAddData should be retried on proxy-origin error");
        UNIT_ASSERT_VALUES_EQUAL_C(
            1,
            fallbackWriteDataRequests,
            "Fallback to WriteData should happen once after CancelAddData "
            "response is received");

        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch after CancelAddData retry");
    }

    Y_UNIT_TEST(ShouldFallbackOnAddDataErrorInUnconfirmedFlow)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        NProto::TError error;
        error.SetCode(E_REJECTED);
        error.SetMessage("test error");

        bool errorInjected = false;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                        TEvIndexTablet::EvAddDataResponse &&
                    !errorInjected)
                {
                    errorInjected = true;
                    auto* msg = event->template Get<
                        TEvIndexTablet::TEvAddDataResponse>();
                    msg->Record.MutableError()->CopyFrom(error);
                }
                return false;
            });

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);
        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch after fallback");
    }

    Y_UNIT_TEST(ShouldFallbackOnConfirmAddDataError)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        NProto::TError error;
        error.SetCode(E_REJECTED);
        error.SetMessage("test error");

        bool errorInjected = false;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                        TEvIndexTablet::EvConfirmAddDataResponse &&
                    !errorInjected)
                {
                    errorInjected = true;
                    auto* msg = event->template Get<
                        TEvIndexTablet::TEvConfirmAddDataResponse>();
                    msg->Record.MutableError()->CopyFrom(error);
                }
                return false;
            });

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);
        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch after fallback");
    }

    Y_UNIT_TEST(ShouldRetryConfirmAddDataOnProxyErrorAndThenUseTabletAnswer)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        NProto::TError error;
        error.SetCode(E_REJECTED);
        error.SetMessage("proxy test error");

        bool errorInjected = false;
        ui32 fallbackWriteDataRequests = 0;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvWriteDataRequest &&
                    event->Recipient == MakeIndexTabletProxyServiceId())
                {
                    ++fallbackWriteDataRequests;
                }

                if (event->GetTypeRewrite() ==
                        TEvIndexTablet::EvConfirmAddDataResponse &&
                    !errorInjected)
                {
                    errorInjected = true;
                    auto* msg = event->template Get<
                        TEvIndexTablet::TEvConfirmAddDataResponse>();
                    msg->Record.MutableError()->CopyFrom(error);
                    event->Sender = MakeIndexTabletProxyServiceId();
                }

                return false;
            });

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);

        UNIT_ASSERT_C(
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest) >= 2,
            "ConfirmAddData should be retried on proxy-origin error");
        UNIT_ASSERT_VALUES_EQUAL_C(
            1,
            fallbackWriteDataRequests,
            "After retry, tablet ConfirmAddData error should follow regular "
            "flow and fallback to WriteData");

        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch after ConfirmAddData retry");
    }

    Y_UNIT_TEST(ShouldReportCriticalEventOnConfirmAddDataProxyRetryThreshold)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        auto criticalEventBackend =
            std::make_shared<TCriticalEventLogBackend>();
        auto logging = CreateLoggingService(criticalEventBackend);
        logging->Start();
        NCloud::SetCriticalEventsLog(logging->CreateLog("NFS_TEST"));

        NProto::TError error;
        error.SetCode(E_REJECTED);
        error.SetMessage("proxy retry threshold test error");

        constexpr ui32 proxyErrorInjectionLimit = 20;
        ui32 proxyErrorsInjected = 0;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                        TEvIndexTablet::EvConfirmAddDataResponse &&
                    proxyErrorsInjected < proxyErrorInjectionLimit)
                {
                    ++proxyErrorsInjected;
                    auto* msg = event->template Get<
                        TEvIndexTablet::TEvConfirmAddDataResponse>();
                    msg->Record.MutableError()->CopyFrom(error);
                    event->Sender = MakeIndexTabletProxyServiceId();
                }
                return false;
            });

        TString data = GenerateValidateData(256_KB, 17);
        WriteData(setup, 0, data);

        UNIT_ASSERT_VALUES_EQUAL(proxyErrorInjectionLimit, proxyErrorsInjected);
        UNIT_ASSERT_C(
            criticalEventBackend->Contains(
                "CRITICAL_EVENT:AppCriticalEvents/"
                "UnconfirmedFlowProxyRetryThresholdReached"),
            "Expected retry threshold critical event log");

        logging->Stop();

        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch");
    }

    // =========================================================================
    // Request verification tests (continued)
    // =========================================================================

    Y_UNIT_TEST(ShouldSendHeadAndTailUnalignedRangesToGenerateBlobIds)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        NProtoPrivate::TGenerateBlobIdsRequest capturedGenRequest;

        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                    TEvIndexTablet::EvGenerateBlobIdsRequest)
                {
                    capturedGenRequest =
                        event
                            ->template Get<
                                TEvIndexTablet::TEvGenerateBlobIdsRequest>()
                            ->Record;
                }
                return false;
            });

        const ui64 offset = 100;
        const ui64 alignedOffset = 4_KB;
        const ui64 alignedLength = 128_KB;
        const ui64 tailLength = 300;
        const ui64 headLength = alignedOffset - offset;
        const ui64 writeLength = headLength + alignedLength + tailLength;
        TString data = GenerateValidateData(writeLength, 42);

        WriteData(setup, offset, data);

        UNIT_ASSERT_VALUES_EQUAL(alignedOffset, capturedGenRequest.GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(alignedLength, capturedGenRequest.GetLength());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            capturedGenRequest.UnalignedDataRangesSize());

        const ui64 tailOffset = alignedOffset + alignedLength;
        UNIT_ASSERT_C(
            offset + data.size() > tailOffset,
            "Expected head+tail write to include tail bytes");
        const ui64 actualTailLength = offset + data.size() - tailOffset;
        const ui64 alignedDataOffset = headLength;

        UNIT_ASSERT_VALUES_EQUAL(
            offset,
            capturedGenRequest.GetUnalignedDataRanges(0).GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(data).SubStr(0, headLength),
            TStringBuf(
                capturedGenRequest.GetUnalignedDataRanges(0).GetContent()));

        UNIT_ASSERT_VALUES_EQUAL(
            tailOffset,
            capturedGenRequest.GetUnalignedDataRanges(1).GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(data).SubStr(
                alignedDataOffset + alignedLength,
                actualTailLength),
            TStringBuf(
                capturedGenRequest.GetUnalignedDataRanges(1).GetContent()));

        const auto actualResponse = ReadData(setup, offset, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch");
    }

    // =========================================================================
    // Sharding tests
    // =========================================================================

    Y_UNIT_TEST(ShouldNotEnableShardUnconfirmedFlowUnlessServiceRequested)
    {
        TShardedFileSystemConfig fsConfig;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetAutomaticShardCreationEnabled(true);
        storageConfig.SetAutomaticallyCreatedShardSize(1_GB);
        storageConfig.SetShardAllocationUnit(1_GB);
        storageConfig.SetShardBalancerPolicy(NProto::SBP_ROUND_ROBIN);
        storageConfig.SetAddingUnconfirmedDataEnabled(false);
        storageConfig.SetUnconfirmedDataCountHardLimit(100);
        storageConfig.SetWriteBlobThreshold(128_KB);
        storageConfig.SetThreeStageWriteEnabled(true);
        storageConfig.SetUnalignedThreeStageWriteEnabled(true);

        TTestEnvConfig envConfig;
        envConfig.DynamicNodes = 2;
        TTestEnv env(envConfig, storageConfig);

        const ui32 nodeIdx = env.AddDynamicNode();
        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsConfig.FsId, fsConfig.MainFsBlockCount);
        service.ResizeFileStore(
            fsConfig.FsId,
            fsConfig.MainFsBlockCount,
            false /* force */,
            1 /* shardCount */);
        WaitForTabletStart(service);

        const auto shardTabletId = service.GetFileStoreInfo(fsConfig.Shard1Id)
                                       ->Record.GetFileStore()
                                       .GetMainTabletId();
        UNIT_ASSERT(shardTabletId);

        TIndexTabletClient shardTablet(
            env.GetRuntime(),
            nodeIdx,
            shardTabletId,
            {},
            false /* updateConfig */);
        NProto::TStorageConfig shardStorageConfig;
        shardStorageConfig.SetAddingUnconfirmedDataEnabled(true);
        shardStorageConfig.SetUnconfirmedDataCountHardLimit(100);
        shardTablet.ChangeStorageConfig(std::move(shardStorageConfig));
        shardTablet.RebootTablet();

        THeaders headers;
        auto session = service.InitSession(headers, fsConfig.FsId, "client");
        UNIT_ASSERT(!session->Record.GetFileStore()
                         .GetFeatures()
                         .GetUnconfirmedFlowEnabled());

        const auto nodeId =
            service
                .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();
        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(nodeId));

        const auto handle = service
                                .CreateHandle(
                                    headers,
                                    fsConfig.FsId,
                                    nodeId,
                                    "",
                                    TCreateHandleArgs::RDWR)
                                ->Record.GetHandle();

        auto& runtime = env.GetRuntime();
        TMaybe<bool> unconfirmedFlowRequested;
        TMaybe<bool> unconfirmedFlowEnabled;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvGenerateBlobIdsRequest: {
                        const auto* msg = event->template Get<
                            TEvIndexTablet::TEvGenerateBlobIdsRequest>();
                        if (msg->Record.GetFileSystemId() ==
                            fsConfig.Shard1Id) {
                            unconfirmedFlowRequested =
                                msg->Record.GetUnconfirmedFlowRequested();
                        }
                        break;
                    }
                    case TEvIndexTablet::EvGenerateBlobIdsResponse: {
                        const auto* msg = event->template Get<
                            TEvIndexTablet::TEvGenerateBlobIdsResponse>();
                        unconfirmedFlowEnabled =
                            msg->Record.GetUnconfirmedFlowEnabled();
                        break;
                    }
                }

                return false;
            });

        const ui64 confirmRequestCount =
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest);
        const ui64 cancelRequestCount =
            runtime.GetCounter(TEvIndexTablet::EvCancelAddDataRequest);

        const TString data = GenerateValidateData(256_KB);
        service.WriteData(headers, fsConfig.FsId, nodeId, handle, 0, data);

        UNIT_ASSERT(unconfirmedFlowRequested.Defined());
        UNIT_ASSERT(!*unconfirmedFlowRequested);
        UNIT_ASSERT(unconfirmedFlowEnabled.Defined());
        UNIT_ASSERT(!*unconfirmedFlowEnabled);
        UNIT_ASSERT_VALUES_EQUAL(
            confirmRequestCount,
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest));
        UNIT_ASSERT_VALUES_EQUAL(
            cancelRequestCount,
            runtime.GetCounter(TEvIndexTablet::EvCancelAddDataRequest));

        const auto actualResponse = service.ReadData(
            headers,
            fsConfig.FsId,
            nodeId,
            handle,
            0,
            data.size());
        const auto actualData = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actualData),
            "Data mismatch");
    }

    Y_UNIT_TEST(ShouldDeleteShardUnconfirmedDataOnServiceDataPipeDisconnect)
    {
        DoShouldDeleteShardUnconfirmedDataOnServicePipeDisconnect(
            EShardPipeToDisconnect::Data);
    }

    Y_UNIT_TEST(ShouldDeleteShardUnconfirmedDataOnServiceControlPipeDisconnect)
    {
        DoShouldDeleteShardUnconfirmedDataOnServicePipeDisconnect(
            EShardPipeToDisconnect::Control);
    }

    // =========================================================================
    // Tablet reboot tests
    // =========================================================================

    Y_UNIT_TEST(ShouldReadAndWriteUnalignedDataAfterTabletReboot)
    {
        TTestSetup setup;
        const auto tabletId =
            setup.Service->GetFileStoreInfo(setup.FileSystemId)
                ->Record.GetFileStore()
                .GetMainTabletId();

        const ui64 firstOffset = 101;
        const TString firstData = GenerateValidateData(256_KB + 137, 13);
        WriteData(setup, firstOffset, firstData);
        const auto actualFirstResponse =
            ReadData(setup, firstOffset, firstData.size());
        const auto actualFirst = actualFirstResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(firstData),
            CityHash64(actualFirst),
            "Data mismatch");

        TIndexTabletClient tablet(setup.GetRuntime(), setup.NodeIdx, tabletId);
        tablet.RebootTablet();

        // Remake session/handle after reboot; old session becomes invalid.
        setup.Headers =
            setup.Service->InitSession(setup.FileSystemId, "client");
        setup.Handle = setup.Service
                           ->CreateHandle(
                               setup.Headers,
                               setup.FileSystemId,
                               setup.NodeId,
                               "",
                               TCreateHandleArgs::RDWR)
                           ->Record.GetHandle();

        const auto actualAfterReboot =
            ReadData(setup, firstOffset, firstData.size());
        const auto actualAfterRebootBuffer =
            actualAfterReboot->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(firstData),
            CityHash64(actualAfterRebootBuffer),
            "Data mismatch after tablet reboot");

        const ui64 secondOffset = 4_KB + 77;
        const TVector<TString> iovecs{
            GenerateValidateData(1_KB, 21),
            GenerateValidateData(3_KB, 22),
            GenerateValidateData(static_cast<ui32>(128_KB + 211 - 4_KB), 23)};
        TString secondData;
        for (const auto& iovec: iovecs) {
            secondData += iovec;
        }

        WriteData(setup, secondOffset, iovecs);
        const auto actualSecond =
            ReadData(setup, secondOffset, secondData.size());
        const auto actualSecondBuffer = actualSecond->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(secondData),
            CityHash64(actualSecondBuffer),
            "Data mismatch for unaligned iovec write after tablet reboot");
    }
}

}   // namespace NCloud::NFileStore::NStorage
