#include "service.h"

#include <cloud/filestore/libs/storage/testlib/service_client.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>
#include <cloud/filestore/private/api/protos/actions.pb.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <contrib/libs/protobuf/src/google/protobuf/stubs/stringpiece.h>
#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore::NStorage {

using namespace NKikimr;
using namespace std::string_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

[[maybe_unused]] TString ToString(const NProtoBuf::StringPiece& piece)
{
    return piece.ToString();
}

NProto::TStorageConfig ExecuteGetStorageConfig(
    const TString& fsId,
    TServiceClient& service)
{
    NProtoPrivate::TGetStorageConfigRequest request;
    request.SetFileSystemId(fsId);

    TString buf;
    google::protobuf::util::MessageToJsonString(request, &buf);

    auto jsonResponse = service.ExecuteAction("getstorageconfig", buf);
    UNIT_ASSERT_VALUES_EQUAL(S_OK, jsonResponse->GetStatus());

    NProto::TStorageConfig response;
    auto status = google::protobuf::util::JsonStringToMessage(
        jsonResponse->Record.GetOutput(),
        &response);
    UNIT_ASSERT_C(status.ok(), ToString(status.message()));
    return response;
}

NProtoPrivate::TChangeStorageConfigResponse ExecuteChangeStorageConfig(
    const TString& fsId,
    NProto::TStorageConfig config,
    TServiceClient& service,
    bool mergeWithConfig = false)
{
    NProtoPrivate::TChangeStorageConfigRequest request;
    request.SetFileSystemId(fsId);

    *request.MutableStorageConfig() = std::move(config);
    request.SetMergeWithStorageConfigFromTabletDB(mergeWithConfig);

    TString buf;
    google::protobuf::util::MessageToJsonString(request, &buf);

    auto jsonResponse = service.ExecuteAction("changestorageconfig", buf);
    UNIT_ASSERT_VALUES_EQUAL(S_OK, jsonResponse->GetStatus());

    NProtoPrivate::TChangeStorageConfigResponse response;
    auto status = google::protobuf::util::JsonStringToMessage(
        jsonResponse->Record.GetOutput(),
        &response);
    UNIT_ASSERT_C(status.ok(), ToString(status.message()));

    return response;
}

TString GenerateValidateData(ui32 size, ui32 seed = 0)
{
    TString data(size, 0);
    for (ui32 i = 0; i < size; ++i) {
        data[i] = 'A' + ((i + seed) % ('Z' - 'A' + 1));
    }
    return data;
}

void WaitForTabletStart(TServiceClient& service)
{
    TDispatchOptions options;
    options.FinalEvents = {
        TDispatchOptions::TFinalEventCondition(
            TEvIndexTabletPrivate::EvLoadCompactionMapChunkRequest)};
    service.AccessRuntime().DispatchEvents(options);
}

void GetStorageStats(
    TServiceClient& service,
    const NProtoPrivate::TGetStorageStatsRequest& request,
    NProtoPrivate::TGetStorageStatsResponse& response)
{
    TString buf;
    google::protobuf::util::MessageToJsonString(request, &buf);
    const auto actionResponse = service.ExecuteAction("GetStorageStats", buf);
    auto status = google::protobuf::util::JsonStringToMessage(
        actionResponse->Record.GetOutput(),
        &response);
}

auto GetFileSystemCounters(TTestEnv& env, const TString& fsId)
{
    const auto counters = env.GetRuntime().GetAppData().Counters;
    auto subgroup = counters->FindSubgroup("counters", "filestore");
    UNIT_ASSERT(subgroup);
    subgroup = subgroup->FindSubgroup("component", "storage_fs");
    UNIT_ASSERT(subgroup);
    subgroup = subgroup->FindSubgroup("host", "cluster");
    UNIT_ASSERT(subgroup);
    subgroup = subgroup->FindSubgroup("filesystem", fsId);
    UNIT_ASSERT(subgroup);
    subgroup = subgroup->FindSubgroup("cloud", "test_cloud");
    UNIT_ASSERT(subgroup);
    subgroup = subgroup->FindSubgroup("folder", "test_folder");
    UNIT_ASSERT(subgroup);

    return subgroup;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStorageServiceActionsTest)
{
    Y_UNIT_TEST(ShouldFail)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        auto response =
            service.AssertExecuteActionFailed("NonExistingAction", "{}");

        UNIT_ASSERT_VALUES_UNEQUAL(S_OK, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldDrainTablets)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);

        ui64 observedNodeId = 0;
        bool observedKeepDown = false;
        env.GetRuntime().SetObserverFunc([&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvHive::EvDrainNode: {
                        auto* msg = event->Get<TEvHive::TEvDrainNode>();
                        observedNodeId = msg->Record.GetNodeID();
                        observedKeepDown = msg->Record.GetKeepDown();
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        static constexpr auto DrainTabletsActionName = "draintablets";
        {
            NProtoPrivate::TDrainNodeRequest request;
            TString requestJson;
            google::protobuf::util::MessageToJsonString(request, &requestJson);

            auto response = service.ExecuteAction(DrainTabletsActionName, requestJson);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            UNIT_ASSERT_VALUES_EQUAL(service.GetSender().NodeId(), observedNodeId);
            UNIT_ASSERT(!observedKeepDown);
        }

        {
            NProtoPrivate::TDrainNodeRequest request;
            request.SetKeepDown(true);
            TString requestJson;
            google::protobuf::util::MessageToJsonString(request, &requestJson);

            auto response = service.ExecuteAction(DrainTabletsActionName, requestJson);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(service.GetSender().NodeId(), observedNodeId);
            UNIT_ASSERT(observedKeepDown);
        }
    }

    Y_UNIT_TEST(ShouldGetStorageConfigFromNodeOrFs)
    {
        NProto::TStorageConfig config;
        config.SetReadAheadCacheMaxNodes(42);

        TTestEnv env{{}, config};
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateFileStore("fs0", 1'000);

        auto response = ExecuteGetStorageConfig("", service);
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(
            response, env.GetStorageConfig()->GetStorageConfigProto()));

        UNIT_ASSERT_VALUES_EQUAL(
            42,
            response.GetReadAheadCacheMaxNodes());

        {
            NProto::TStorageConfig newConfig;
            newConfig.SetMultiTabletForwardingEnabled(true);
            const auto response = ExecuteChangeStorageConfig(
                "fs0",
                std::move(newConfig),
                service);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetMultiTabletForwardingEnabled(),
                true);
        }

        {
            auto response = ExecuteGetStorageConfig("fs0", service);

            UNIT_ASSERT_VALUES_EQUAL(
                response.GetMultiTabletForwardingEnabled(),
                true);
        }
    }

    Y_UNIT_TEST(ShouldPerformUnsafeNodeManipulations)
    {
        NProto::TStorageConfig config;
        TTestEnv env{{}, config};
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);

        const TString fsId = "test";
        service.CreateFileStore(fsId, 1'000);

        auto headers = service.InitSession("test", "client");

        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file1"));
        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file2"));

        ui64 id1 = 0;
        ui64 id2 = 0;
        ui64 id3 = 0;

        {
            auto r = service.ListNodes(headers, fsId, RootNodeId)->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, r.NodesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, r.GetNodes(0).GetSize());
            UNIT_ASSERT_VALUES_EQUAL(0, r.GetNodes(1).GetSize());

            id1 = r.GetNodes(0).GetId();
            id2 = r.GetNodes(1).GetId();

            UNIT_ASSERT_VALUES_UNEQUAL(0, id1);
            UNIT_ASSERT_VALUES_UNEQUAL(0, id2);
        }

        id3 = Max(id1, id2) + 1;

        {
            NProtoPrivate::TUnsafeUpdateNodeRequest request;
            request.SetFileSystemId(fsId);
            auto* node = request.MutableNode();
            node->SetId(id3);
            node->SetSize(333);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("UnsafeUpdateNode", buf);
        }

        {
            NProtoPrivate::TUnsafeGetNodeRequest request;
            request.SetFileSystemId(fsId);
            request.SetId(id3);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto r = service.ExecuteAction("UnsafeGetNode", buf)->Record;

            NProtoPrivate::TUnsafeGetNodeResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                r.GetOutput(),
                &response).ok());

            UNIT_ASSERT_VALUES_EQUAL(id3, response.GetNode().GetId());
            UNIT_ASSERT_VALUES_EQUAL(333, response.GetNode().GetSize());
        }

        {
            NProtoPrivate::TUnsafeUpdateNodeRequest request;
            request.SetFileSystemId(fsId);
            auto* node = request.MutableNode();
            node->SetId(id2);
            node->SetSize(222);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("UnsafeUpdateNode", buf);
        }

        {
            auto r = service.ListNodes(headers, fsId, RootNodeId)->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, r.NodesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, r.GetNodes(0).GetSize());
            UNIT_ASSERT_VALUES_EQUAL(222, r.GetNodes(1).GetSize());
        }

        {
            NProtoPrivate::TUnsafeDeleteNodeRequest request;
            request.SetFileSystemId(fsId);
            request.SetId(id3);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("UnsafeDeleteNode", buf);
        }

        {
            NProtoPrivate::TUnsafeGetNodeRequest request;
            request.SetFileSystemId(fsId);
            request.SetId(id3);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto r = service.ExecuteAction("UnsafeGetNode", buf)->Record;

            NProtoPrivate::TUnsafeGetNodeResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                r.GetOutput(),
                &response).ok());

            UNIT_ASSERT(!response.HasNode());
        }

        service.DestroySession(headers);
    }

    void DoShouldGetStorageStats(
        const bool strictFileSystemSizeEnforcementEnabled)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetAutomaticShardCreationEnabled(true);
        storageConfig.SetShardAllocationUnit(1_GB);
        storageConfig.SetAutomaticallyCreatedShardSize(1_GB);
        storageConfig.SetMultiTabletForwardingEnabled(true);
        storageConfig.SetStrictFileSystemSizeEnforcementEnabled(
            strictFileSystemSizeEnforcementEnabled);

        NProto::TDiagnosticsConfig diagConfig;
        NProto::TFileSystemPerformanceProfile pp;
        pp.MutableRead()->SetRPS(10);
        pp.MutableRead()->SetThroughput(1_MB);
        pp.MutableWrite()->SetRPS(20);
        pp.MutableWrite()->SetThroughput(2_MB);

        *diagConfig.MutableSSDFileSystemPerformanceProfile() = pp;
        *diagConfig.MutableHDDFileSystemPerformanceProfile() = pp;

        TTestEnv env{{}, storageConfig, {}, CreateProfileLogStub(), diagConfig};
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);

        const TString fsId = "test";
        service.CreateFileStore(fsId, 2_GB / 4_KB);
        // waiting for IndexTablet start after the restart triggered by
        // configureshards
        WaitForTabletStart(service);

        auto headers = service.InitSession("test", "client");

        TString data1 = GenerateValidateData(256_KB, 1);
        TString data2 = GenerateValidateData(256_KB, 2);
        TString data3 = GenerateValidateData(512_KB, 3);

        auto writeToFile =
            [&](const TString& fileName, const TString& d1, const TString& d2)
        {
            const auto handle = service.CreateHandle(
                headers,
                fsId,
                RootNodeId,
                fileName,
                TCreateHandleArgs::CREATE)->Record;

            const auto nodeId = handle.GetNodeAttr().GetId();
            const auto handleId = handle.GetHandle();

            service.WriteData(headers, fsId, nodeId, handleId, 0, d1);
            service.WriteData(headers, fsId, nodeId, handleId, d1.size(), d2);
        };

        writeToFile("file1", data1, data2);
        writeToFile("file2", data2, data3);

        struct TFileSystemInfo {
            const TString Id;
            const ui64 Size;
        };
        TVector<TFileSystemInfo> fileSystems = {
            {.Id = fsId + "_s1", .Size = data1.size() + data2.size()},
            {.Id = fsId + "_s2", .Size = data2.size() + data3.size()},
            {.Id = fsId, .Size = 0}};
        const ui64 shardsCount = 2;
        const ui64 totalSize = fileSystems[0].Size + fileSystems[1].Size;

        // waiting for async stats calculation
        env.GetRuntime().AdvanceCurrentTime(TDuration::Seconds(15));
        TDispatchOptions options;
        options.FinalEvents = {
            TDispatchOptions::TFinalEventCondition(
                TEvIndexTabletPrivate::EvGetShardStatsCompleted,
                strictFileSystemSizeEnforcementEnabled ? 3 : 1)
        };
        env.GetRuntime().DispatchEvents(options);

        auto checkShardStats = [&](const NProtoPrivate::TStorageStats& stats) {
            UNIT_ASSERT_VALUES_EQUAL(2, stats.ShardStatsSize());

            for (ui64 i = 0; i < shardsCount; ++i) {
                const auto& shardStats = stats.GetShardStats(i);
                const auto& fsInfo = fileSystems[i];
                UNIT_ASSERT_VALUES_EQUAL(
                    fsInfo.Id,
                    shardStats.GetShardId());
                UNIT_ASSERT_VALUES_EQUAL(
                    1_GB / 4_KB,
                    shardStats.GetTotalBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    fsInfo.Size / 4_KB,
                    shardStats.GetUsedBlocksCount());
                UNIT_ASSERT_VALUES_UNEQUAL(
                    0,
                    shardStats.GetCurrentLoad());
            }
        };

        {
            NProtoPrivate::TGetStorageStatsRequest request;
            request.SetFileSystemId(fsId);
            request.SetAllowCache(true);

            NProtoPrivate::TGetStorageStatsResponse response;
            GetStorageStats(service, request, response);

            checkShardStats(response.GetStats());
        }

        // Only in this mode shards know of other shards
        if (strictFileSystemSizeEnforcementEnabled) {
            for (ui64 i = 0; i < shardsCount; ++i) {
                NProtoPrivate::TGetStorageStatsRequest request;
                request.SetFileSystemId(fileSystems[i].Id);
                request.SetAllowCache(false);
                request.SetMode(
                    NProtoPrivate::STATS_REQUEST_MODE_FORCE_FETCH_SHARDS);

                NProtoPrivate::TGetStorageStatsResponse response;
                GetStorageStats(service, request, response);

                checkShardStats(response.GetStats());
            }
        }

        {
            NProtoPrivate::TGetStorageStatsRequest request;
            request.SetFileSystemId(fsId + "_s1");
            request.SetCompactionRangeCountByCompactionScore(2);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            const auto response = service.ExecuteAction("GetStorageStats", buf);
            NProtoPrivate::TGetStorageStatsResponse record;
            auto status = google::protobuf::util::JsonStringToMessage(
                response->Record.GetOutput(),
                &record);
            const auto& stats = record.GetStats();
            const auto& compactionRanges = stats.GetCompactionRangeStats();
            UNIT_ASSERT_VALUES_EQUAL(1, stats.GetUsedNodesCount());
            UNIT_ASSERT_VALUES_UNEQUAL(0, stats.GetCurrentLoad());
            UNIT_ASSERT_VALUES_EQUAL(2, stats.GetUsedCompactionRanges());
            UNIT_ASSERT_VALUES_EQUAL(2, compactionRanges.size());
            UNIT_ASSERT_VALUES_EQUAL(1, compactionRanges[0].GetBlobCount());
            UNIT_ASSERT_VALUES_EQUAL(1, compactionRanges[1].GetBlobCount());
        }

        env.GetRegistry()->Update(env.GetRuntime().GetCurrentTime());

        for (const auto& fsInfo: fileSystems) {
            auto counters = GetFileSystemCounters(env, fsInfo.Id);

            UNIT_ASSERT_VALUES_EQUAL(
                fsInfo.Size,
                counters->GetCounter("UsedBytesCount")->GetAtomic());

            // Aggregated counters in shards are calculated if
            // strictFileSystemSizeEnforcementEnabled == true
            if (strictFileSystemSizeEnforcementEnabled || fsInfo.Id == fsId) {
                UNIT_ASSERT_VALUES_EQUAL(
                    totalSize,
                    counters->GetCounter("AggregateUsedBytesCount")
                        ->GetAtomic());
                UNIT_ASSERT_VALUES_EQUAL(
                    2,
                    counters->GetCounter("AggregateUsedNodesCount")
                        ->GetAtomic());
            }
        }

        service.DestroySession(headers);
    }

    Y_UNIT_TEST(ShouldGetStorageStats)
    {
        // Run the test with strictFileSystemSizeEnforcementEnabled ==
        // false/true
        DoShouldGetStorageStats(false);
        DoShouldGetStorageStats(true);
    }

    Y_UNIT_TEST(ShouldRunForcedOperation)
    {
        NProto::TStorageConfig config;
        config.SetCompactionThreshold(1000);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);

        auto headers = service.InitSession("test", "client");

        ui64 nodeId = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file")
        )->Record.GetNode().GetId();

        ui64 handle = service.CreateHandle(
            headers,
            "test",
            nodeId,
            "",
            TCreateHandleArgs::RDWR)->Record.GetHandle();

        service.WriteData(
            headers,
            "test",
            nodeId,
            handle,
            0,
            TString(1_MB, 'a'));

        TAutoPtr<IEventHandle> completion;
        ui32 compactionCounter = 0;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                        TEvIndexTabletPrivate::EvCompactionCompleted)
                {
                    ++compactionCounter;
                    if (compactionCounter == 2) {
                        completion = event.Release();

                        return true;
                    }
                }
                return false;
            });

        TString operationId;

        {
            NProtoPrivate::TForcedOperationRequest request;
            request.SetFileSystemId("test");
            request.SetOpType(
                NProtoPrivate::TForcedOperationRequest::E_COMPACTION);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("forcedoperation", buf);
            NProtoPrivate::TForcedOperationResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());
            UNIT_ASSERT_VALUES_EQUAL(4, response.GetRangeCount());
            operationId = response.GetOperationId();
            UNIT_ASSERT_VALUES_UNEQUAL("", operationId);
        }

        {
            NProtoPrivate::TForcedOperationStatusRequest request;
            request.SetFileSystemId("test");
            request.SetOperationId(operationId);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse =
                service.ExecuteAction("forcedoperationstatus", buf);
            NProtoPrivate::TForcedOperationStatusResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());
            UNIT_ASSERT_VALUES_EQUAL(4, response.GetRangeCount());
            UNIT_ASSERT_VALUES_EQUAL(2, response.GetProcessedRangeCount());
            UNIT_ASSERT_VALUES_EQUAL(
                1177944066,
                response.GetLastProcessedRangeId());
        }

        UNIT_ASSERT(completion);
        env.GetRuntime().Send(completion.Release());
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

        {
            NProtoPrivate::TForcedOperationStatusRequest request;
            request.SetFileSystemId("test");
            request.SetOperationId(operationId);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("forcedoperationstatus", buf);
            auto jsonResponse = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                jsonResponse->GetStatus(),
                jsonResponse->GetErrorReason());
            NProtoPrivate::TForcedOperationStatusResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());
            UNIT_ASSERT_VALUES_EQUAL(4, response.GetRangeCount());
            UNIT_ASSERT_VALUES_EQUAL(4, response.GetProcessedRangeCount());
        }

        env.GetRegistry()->Update(env.GetRuntime().GetCurrentTime());

        const auto counters = env.GetRuntime().GetAppData().Counters;
        auto subgroup = counters->FindSubgroup("counters", "filestore");
        UNIT_ASSERT(subgroup);
        subgroup = subgroup->FindSubgroup("component", "storage_fs");
        UNIT_ASSERT(subgroup);
        subgroup = subgroup->FindSubgroup("host", "cluster");
        UNIT_ASSERT(subgroup);
        subgroup = subgroup->FindSubgroup("filesystem", "test");
        UNIT_ASSERT(subgroup);
        subgroup = subgroup->FindSubgroup("cloud", "test_cloud");
        UNIT_ASSERT(subgroup);
        subgroup = subgroup->FindSubgroup("folder", "test_folder");
        UNIT_ASSERT(subgroup);
        UNIT_ASSERT_VALUES_EQUAL(
            4,
            subgroup->GetCounter("Compaction.Count")->GetAtomic());
    }
}

}   // namespace NCloud::NFileStore::NStorage
