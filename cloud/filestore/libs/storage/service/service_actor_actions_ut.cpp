#include "service.h"
#include "service_ut_helpers.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/testlib/service_client.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>
#include <cloud/filestore/private/api/protos/actions.pb.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/filestore/private/api/unsafe_protos/unsafe.pb.h>

#include <cloud/storage/core/libs/features/features_config.h>

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
    TServiceClient& service,
    bool onlyOverride = false)
{
    NProtoPrivate::TGetStorageConfigRequest request;
    request.SetFileSystemId(fsId);
    request.SetOnlyOverride(onlyOverride);

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

void WaitForTabletStart(TServiceClient& service)
{
    TDispatchOptions options;
    options.FinalEvents = {
        TDispatchOptions::TFinalEventCondition(
            TEvIndexTabletPrivate::EvLoadCompactionMapChunkRequest)};
    service.AccessRuntime().DispatchEvents(options);
}

NProtoPrivate::TGetStorageStatsResponse GetStorageStats(
    TServiceClient& service,
    const NProtoPrivate::TGetStorageStatsRequest& request)
{
    TString buf;
    NProtoPrivate::TGetStorageStatsResponse response;
    google::protobuf::util::MessageToJsonString(request, &buf);
    const auto actionResponse = service.ExecuteAction("GetStorageStats", buf);
    auto status = google::protobuf::util::JsonStringToMessage(
        actionResponse->Record.GetOutput(),
        &response);

    return response;
}

NProtoPrivate::TMarkNodeRefsExhaustiveResponse ExecuteMarkNodeRefsExhaustive(
    TServiceClient& service,
    const TString& fsId,
    ui64 nodeId)
{
    NProtoPrivate::TMarkNodeRefsExhaustiveRequest request;
    request.SetFileSystemId(fsId);
    request.SetNodeId(nodeId);

    TString buf;
    google::protobuf::util::MessageToJsonString(request, &buf);

    auto jsonResponse = service.ExecuteAction("marknoderefsexhaustive", buf);
    NProtoPrivate::TMarkNodeRefsExhaustiveResponse response;
    UNIT_ASSERT(
        google::protobuf::util::JsonStringToMessage(
            jsonResponse->Record.GetOutput(),
            &response)
            .ok());
    return response;
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

        ui32 nodeIdx = env.AddDynamicNode();

        TServiceClient service(env.GetRuntime(), nodeIdx);
        auto response =
            service.AssertExecuteActionFailed("NonExistingAction", "{}");

        UNIT_ASSERT_VALUES_UNEQUAL(S_OK, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldDrainTablets)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();

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

        ui32 nodeIdx = env.AddDynamicNode();

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
            newConfig.SetThrottlingEnabled(true);
            const auto response = ExecuteChangeStorageConfig(
                "fs0",
                std::move(newConfig),
                service);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetThrottlingEnabled(),
                true);
        }

        {
            auto response = ExecuteGetStorageConfig("fs0", service);

            UNIT_ASSERT_VALUES_EQUAL(response.GetThrottlingEnabled(), true);
        }
    }

    Y_UNIT_TEST(ShouldGetStorageConfigOnlyOverride)
    {
        NProto::TStorageConfig config;
        config.SetReadAheadCacheMaxNodes(42);

        TTestEnv env{{}, config};

        ui32 nodeIdx = env.AddDynamicNode();

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateFileStore("fs0", 1'000);

        {
            auto response = ExecuteGetStorageConfig("fs0", service, true);
            UNIT_ASSERT(!response.HasReadAheadCacheMaxNodes());
        }

        {
            NProto::TStorageConfig newConfig;
            newConfig.SetThrottlingEnabled(true);
            ExecuteChangeStorageConfig("fs0", std::move(newConfig), service);
        }

        {
            auto response = ExecuteGetStorageConfig("fs0", service, false);
            UNIT_ASSERT_VALUES_EQUAL(42, response.GetReadAheadCacheMaxNodes());
            UNIT_ASSERT_VALUES_EQUAL(true, response.GetThrottlingEnabled());
        }

        {
            auto response = ExecuteGetStorageConfig("fs0", service, true);
            UNIT_ASSERT(!response.HasReadAheadCacheMaxNodes());
            UNIT_ASSERT_VALUES_EQUAL(true, response.GetThrottlingEnabled());
        }
    }

    Y_UNIT_TEST(ShouldPerformUnsafeNodeManipulations)
    {
        NProto::TStorageConfig config;
        TTestEnv env{{}, config};

        ui32 nodeIdx = env.AddDynamicNode();

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
            NProtoPrivate::TUnsafeCreateNodeRequest request;
            request.SetFileSystemId(fsId);
            auto* node = request.MutableNode();
            node->SetId(id3);
            node->SetSize(333);
            service.SendRequest(
                MakeStorageServiceId(),
                std::make_unique<TEvIndexTablet::TEvUnsafeCreateNodeRequest>(
                    MakeIntrusive<TCallContext>(),
                    request));
            auto response =
                service.RecvResponse<TEvIndexTablet::TEvUnsafeCreateNodeResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                FormatError(response->GetError()));
        }

        {
            NProtoPrivate::TUnsafeCreateNodeRequest request;
            request.SetFileSystemId(fsId);
            auto* node = request.MutableNode();
            node->SetId(id3);
            node->SetSize(333);
            service.SendRequest(
                MakeStorageServiceId(),
                std::make_unique<TEvIndexTablet::TEvUnsafeCreateNodeRequest>(
                    MakeIntrusive<TCallContext>(),
                    request));
            auto response =
                service.RecvResponse<TEvIndexTablet::TEvUnsafeCreateNodeResponse>();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_EXIST,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

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
            NProtoPrivate::TUnsafeUpdateNodeRequest request;
            request.SetFileSystemId(fsId);
            auto* node = request.MutableNode();
            node->SetId(id3 + 1);
            node->SetSize(333);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("UnsafeUpdateNode", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOENT,
                response->GetStatus(),
                FormatError(response->GetError()));
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
            service.SendRequest(
                MakeStorageServiceId(),
                std::make_unique<TEvIndexTablet::TEvUnsafeDeleteNodeRequest>(
                    MakeIntrusive<TCallContext>(),
                    request));
            auto response =
                service.RecvResponse<TEvIndexTablet::TEvUnsafeDeleteNodeResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                FormatError(response->GetError()));
        }

        {
            NProtoPrivate::TUnsafeGetNodeRequest request;
            request.SetFileSystemId(fsId);
            request.SetId(id3);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("UnsafeGetNode", buf);
            auto response = service.RecvExecuteActionResponse();

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOENT,
                response->Record.GetError().GetCode(),
                FormatError(response->Record.GetError()));
        }

        {
            NProtoPrivate::TUnsafeDeleteNodeRequest request;
            request.SetFileSystemId(fsId);
            request.SetId(id3);
            service.SendRequest(
                MakeStorageServiceId(),
                std::make_unique<TEvIndexTablet::TEvUnsafeDeleteNodeRequest>(
                    MakeIntrusive<TCallContext>(),
                    request));
            auto response =
                service.RecvResponse<TEvIndexTablet::TEvUnsafeDeleteNodeResponse>();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOENT,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        service.DestroySession(headers);
    }

    Y_UNIT_TEST(ShouldPerformUnsafeNodeRefManipulations)
    {
        NProto::TStorageConfig config;
        TTestEnv env{{}, config};

        ui32 nodeIdx = env.AddDynamicNode();

        TServiceClient service(env.GetRuntime(), nodeIdx);

        const TString fsId = "test";
        service.CreateFileStore(fsId, 1'000);

        auto headers = service.InitSession("test", "client");
        // being explicit
        headers.DisableMultiTabletForwarding = true;

        const ui64 parentId = RootNodeId;
        const TString name1 = "file1";
        const TString name2 = "file2";

        service.CreateNode(headers, TCreateNodeArgs::File(parentId, name1));

        const TString shardId1 = "shard1";
        const TString shardNodeName1 = CreateGuidAsString();
        const TString shardId2 = "shard2";
        const TString shardNodeName2 = CreateGuidAsString();

        {
            NProtoPrivate::TUnsafeUpdateNodeRefRequest request;
            request.SetFileSystemId(fsId);
            request.SetParentId(parentId);
            request.SetName(name1);
            request.SetShardId(shardId1);
            request.SetShardNodeName(shardNodeName1);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("UnsafeUpdateNodeRef", buf);
        }

        {
            NProtoPrivate::TUnsafeUpdateNodeRefRequest request;
            request.SetFileSystemId(fsId);
            request.SetParentId(parentId);
            request.SetName(name1 + ".nonexistent");
            request.SetShardId(shardId1);
            request.SetShardNodeName(shardNodeName1);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("UnsafeUpdateNodeRef", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOENT,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        {
            NProtoPrivate::TUnsafeGetNodeRefRequest request;
            request.SetFileSystemId(fsId);
            request.SetParentId(parentId);
            request.SetName(name1);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto r = service.ExecuteAction("UnsafeGetNodeRef", buf)->Record;

            NProtoPrivate::TUnsafeGetNodeRefResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                r.GetOutput(),
                &response).ok());

            UNIT_ASSERT_VALUES_EQUAL(shardId1, response.GetShardId());
            UNIT_ASSERT_VALUES_EQUAL(
                shardNodeName1,
                response.GetShardNodeName());
        }

        {
            NProtoPrivate::TUnsafeCreateNodeRefRequest request;
            request.SetFileSystemId(fsId);
            request.SetParentId(parentId);
            request.SetName(name2);
            request.SetShardId(shardId2);
            request.SetShardNodeName(shardNodeName2);
            service.SendRequest(
                MakeStorageServiceId(),
                std::make_unique<TEvIndexTablet::TEvUnsafeCreateNodeRefRequest>(
                    MakeIntrusive<TCallContext>(),
                    request));
            auto response =
                service.RecvResponse<TEvIndexTablet::TEvUnsafeCreateNodeRefResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                FormatError(response->GetError()));
        }

        {
            NProtoPrivate::TUnsafeCreateNodeRefRequest request;
            request.SetFileSystemId(fsId);
            request.SetParentId(parentId);
            request.SetName(name2);
            request.SetShardId(shardId2);
            request.SetShardNodeName(shardNodeName2);
            service.SendRequest(
                MakeStorageServiceId(),
                std::make_unique<TEvIndexTablet::TEvUnsafeCreateNodeRefRequest>(
                    MakeIntrusive<TCallContext>(),
                    request));
            auto response =
                service.RecvResponse<TEvIndexTablet::TEvUnsafeCreateNodeRefResponse>();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_EXIST,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        {
            NProtoPrivate::TUnsafeGetNodeRefRequest request;
            request.SetFileSystemId(fsId);
            request.SetParentId(parentId);
            request.SetName(name2);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto r = service.ExecuteAction("UnsafeGetNodeRef", buf)->Record;

            NProtoPrivate::TUnsafeGetNodeRefResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                r.GetOutput(),
                &response).ok());

            UNIT_ASSERT_VALUES_EQUAL(shardId2, response.GetShardId());
            UNIT_ASSERT_VALUES_EQUAL(
                shardNodeName2,
                response.GetShardNodeName());
        }

        {
            auto r = service.ListNodes(headers, fsId, parentId)->Record;
            UNIT_ASSERT_VALUES_EQUAL(2, r.NodesSize());
            UNIT_ASSERT_VALUES_EQUAL(2, r.NamesSize());
            UNIT_ASSERT_VALUES_EQUAL(name1, r.GetNames(0));
            UNIT_ASSERT_VALUES_EQUAL(
                shardId1,
                r.GetNodes(0).GetShardFileSystemId());
            UNIT_ASSERT_VALUES_EQUAL(
                shardNodeName1,
                r.GetNodes(0).GetShardNodeName());
            UNIT_ASSERT_VALUES_EQUAL(name2, r.GetNames(1));
            UNIT_ASSERT_VALUES_EQUAL(
                shardId2,
                r.GetNodes(1).GetShardFileSystemId());
            UNIT_ASSERT_VALUES_EQUAL(
                shardNodeName2,
                r.GetNodes(1).GetShardNodeName());
        }

        {
            NProtoPrivate::TUnsafeDeleteNodeRefRequest request;
            request.SetFileSystemId(fsId);
            request.SetParentId(parentId);
            request.SetName(name1);
            service.SendRequest(
                MakeStorageServiceId(),
                std::make_unique<TEvIndexTablet::TEvUnsafeDeleteNodeRefRequest>(
                    MakeIntrusive<TCallContext>(),
                    request));
            auto response =
                service.RecvResponse<TEvIndexTablet::TEvUnsafeDeleteNodeRefResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                FormatError(response->GetError()));
        }

        {
            NProtoPrivate::TUnsafeGetNodeRefRequest request;
            request.SetFileSystemId(fsId);
            request.SetParentId(parentId);
            request.SetName(name1);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("UnsafeGetNodeRef", buf);
            auto response = service.RecvExecuteActionResponse();

            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOENT,
                response->Record.GetError().GetCode(),
                FormatError(response->Record.GetError()));
        }

        {
            NProtoPrivate::TUnsafeDeleteNodeRefRequest request;
            request.SetFileSystemId(fsId);
            request.SetParentId(parentId);
            request.SetName(name1);
            service.SendRequest(
                MakeStorageServiceId(),
                std::make_unique<TEvIndexTablet::TEvUnsafeDeleteNodeRefRequest>(
                    MakeIntrusive<TCallContext>(),
                    request));
            auto response =
                service.RecvResponse<TEvIndexTablet::TEvUnsafeDeleteNodeRefResponse>();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOENT,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        {
            auto r = service.ListNodes(headers, fsId, parentId)->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, r.NodesSize());
            UNIT_ASSERT_VALUES_EQUAL(1, r.NamesSize());
            UNIT_ASSERT_VALUES_EQUAL(name2, r.GetNames(0));
            UNIT_ASSERT_VALUES_EQUAL(
                shardId2,
                r.GetNodes(0).GetShardFileSystemId());
            UNIT_ASSERT_VALUES_EQUAL(
                shardNodeName2,
                r.GetNodes(0).GetShardNodeName());
        }

        service.DestroySession(headers);
    }

    Y_UNIT_TEST(ShouldPerformResponseLogEntryManipulations)
    {
        NProto::TStorageConfig config;
        TTestEnv env{{}, config};

        ui32 nodeIdx = env.AddDynamicNode();

        TServiceClient service(env.GetRuntime(), nodeIdx);

        const TString fsId = "test";
        const ui64 clientTabletId = 111;
        const ui64 requestId = 222;
        const ui32 responseCode = E_FS_ISDIR;
        service.CreateFileStore(fsId, 1'000);

        auto headers = service.InitSession("test", "client");
        // being explicit
        headers.DisableMultiTabletForwarding = true;

        {
            NProtoPrivate::TWriteResponseLogEntryRequest request;
            request.SetFileSystemId(fsId);
            auto* entry = request.MutableEntry();
            entry->SetClientTabletId(clientTabletId);
            entry->SetRequestId(requestId);
            auto* rr = entry->MutableRenameNodeInDestinationResponse();
            rr->MutableError()->SetCode(responseCode);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("WriteResponseLogEntry", buf);
        }

        {
            NProtoPrivate::TGetResponseLogEntryRequest request;
            request.SetFileSystemId(fsId);
            request.SetClientTabletId(clientTabletId);
            request.SetRequestId(requestId);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("GetResponseLogEntry", buf);
            auto r = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                r->GetStatus(),
                FormatError(r->GetError()));
            NProtoPrivate::TGetResponseLogEntryResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                r->Record.GetOutput(),
                &response).ok());
            UNIT_ASSERT(response.HasEntry());
            UNIT_ASSERT_VALUES_EQUAL(
                responseCode,
                response.GetEntry().GetRenameNodeInDestinationResponse()
                    .GetError().GetCode());
        }

        {
            NProtoPrivate::TDeleteResponseLogEntryRequest request;
            request.SetFileSystemId(fsId);
            request.SetClientTabletId(clientTabletId);
            request.SetRequestId(requestId);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("DeleteResponseLogEntry", buf);
        }

        {
            NProtoPrivate::TGetResponseLogEntryRequest request;
            request.SetFileSystemId(fsId);
            request.SetClientTabletId(clientTabletId);
            request.SetRequestId(requestId);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("GetResponseLogEntry", buf);
            auto r = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                r->GetStatus(),
                FormatError(r->GetError()));
            NProtoPrivate::TGetResponseLogEntryResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                r->Record.GetOutput(),
                &response).ok());
            UNIT_ASSERT(!response.HasEntry());
        }

        service.DestroySession(headers);
    }

    Y_UNIT_TEST(ShouldFreezeTablet)
    {
        //
        // Testing a manual repair scenario:
        // 1. create node
        // 2. freeze tablet
        // 3. check that user ops get rejected
        // 4. recreate the node and the node-ref via unsafe ops
        // 5. unfreeze tablet
        // 6. check that user ops work properly again and that unsafe ops'
        //  results are visible
        //

        NProto::TStorageConfig config;
        TTestEnv env{{}, config};

        ui32 nodeIdx = env.AddDynamicNode();

        TServiceClient service(env.GetRuntime(), nodeIdx);

        ui64 tabletId = -1;
        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                        TEvSSProxy::EvDescribeFileStoreResponse)
                {
                    using TDesc = TEvSSProxy::TEvDescribeFileStoreResponse;
                    const auto* msg = event->Get<TDesc>();
                    const auto& desc =
                        msg->PathDescription.GetFileStoreDescription();
                    tabletId = desc.GetIndexTabletId();
                }

                return false;
            });

        const TString fsId = "test";
        service.CreateFileStore(fsId, 1'000);

        auto headers = service.InitSession(fsId, "client");
        // being explicit
        headers.DisableMultiTabletForwarding = true;

        const ui64 nodeId = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1")
        )->Record.GetNode().GetId();

        {
            NProtoPrivate::TUnsafeChangeTabletStateRequest request;
            request.SetFileSystemId(fsId);
            request.SetFrozen(true);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("UnsafeChangeTabletState", buf);
        }

        //
        // Public API operations are not allowed.
        //

        {
            auto response = service.SendAndRecvListNodes(
                headers,
                fsId,
                RootNodeId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        {
            auto response = service.SendAndRecvUnlinkNode(
                headers,
                RootNodeId,
                "file1",
                false /* unlinkDirectory */);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        {
            auto response = service.SendAndRecvCreateNode(
                headers,
                TCreateNodeArgs::File(RootNodeId, "file2"));
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        //
        // Unsafe operations are allowed.
        //

        {
            NProtoPrivate::TUnsafeDeleteNodeRefRequest request;
            request.SetFileSystemId(fsId);
            request.SetParentId(RootNodeId);
            request.SetName("file1");
            service.SendRequest(
                MakeStorageServiceId(),
                std::make_unique<TEvIndexTablet::TEvUnsafeDeleteNodeRefRequest>(
                    MakeIntrusive<TCallContext>(),
                    request));
            auto response =
                service.RecvResponse<TEvIndexTablet::TEvUnsafeDeleteNodeRefResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                FormatError(response->GetError()));
        }

        {
            NProtoPrivate::TUnsafeDeleteNodeRequest request;
            request.SetFileSystemId(fsId);
            request.SetId(nodeId);
            service.SendRequest(
                MakeStorageServiceId(),
                std::make_unique<TEvIndexTablet::TEvUnsafeDeleteNodeRequest>(
                    MakeIntrusive<TCallContext>(),
                    request));
            auto response =
                service.RecvResponse<TEvIndexTablet::TEvUnsafeDeleteNodeResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                FormatError(response->GetError()));
        }

        {
            NProtoPrivate::TUnsafeCreateNodeRequest request;
            request.SetFileSystemId(fsId);
            auto* node = request.MutableNode();
            node->SetId(nodeId);
            node->SetSize(333);
            service.SendRequest(
                MakeStorageServiceId(),
                std::make_unique<TEvIndexTablet::TEvUnsafeCreateNodeRequest>(
                    MakeIntrusive<TCallContext>(),
                    request));
            auto response =
                service.RecvResponse<TEvIndexTablet::TEvUnsafeCreateNodeResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                FormatError(response->GetError()));
        }

        {
            NProtoPrivate::TUnsafeCreateNodeRefRequest request;
            request.SetFileSystemId(fsId);
            request.SetParentId(RootNodeId);
            request.SetName("file2");
            request.SetChildId(nodeId);
            service.SendRequest(
                MakeStorageServiceId(),
                std::make_unique<TEvIndexTablet::TEvUnsafeCreateNodeRefRequest>(
                    MakeIntrusive<TCallContext>(),
                    request));
            auto response =
                service.RecvResponse<TEvIndexTablet::TEvUnsafeCreateNodeRefResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                FormatError(response->GetError()));
        }

        //
        // The Frozen flag should survive tablet reboots.
        //

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            {} /* config */,
            false /* updateConfig */);
        tablet.RebootTablet();

        // remaking session since CreateSessionActor doesn't do it by itself
        // because EvWakeup never arrives because Scheduling doesn't work by
        // default and RegistrationObservers get reset after RebootTablet
        headers = service.InitSession(fsId, "client");

        {
            auto response = service.SendAndRecvListNodes(
                headers,
                fsId,
                RootNodeId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        {
            NProtoPrivate::TUnsafeChangeTabletStateRequest request;
            request.SetFileSystemId(fsId);
            request.SetFrozen(false);
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.ExecuteAction("UnsafeChangeTabletState", buf);
        }

        //
        // Public API operations are allowed after unfreezing.
        //

        {
            auto response = service.SendAndRecvListNodes(
                headers,
                fsId,
                RootNodeId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                FormatError(response->GetError()));

            const auto& r = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, r.NamesSize());
            UNIT_ASSERT_VALUES_EQUAL("file2", r.GetNames(0));
            UNIT_ASSERT_VALUES_EQUAL(1, r.NodesSize());
            UNIT_ASSERT_VALUES_EQUAL(nodeId, r.GetNodes(0).GetId());
            UNIT_ASSERT_VALUES_EQUAL(333, r.GetNodes(0).GetSize());
        }

        {
            auto response = service.SendAndRecvUnlinkNode(
                headers,
                RootNodeId,
                "file2",
                false /* unlinkDirectory */);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        ui64 newNodeId = 0;
        {
            auto response = service.SendAndRecvCreateNode(
                headers,
                TCreateNodeArgs::File(RootNodeId, "file2"));
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                FormatError(response->GetError()));
            newNodeId = response->Record.GetNode().GetId();
        }

        {
            auto response = service.SendAndRecvListNodes(
                headers,
                fsId,
                RootNodeId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                FormatError(response->GetError()));

            const auto& r = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, r.NamesSize());
            UNIT_ASSERT_VALUES_EQUAL("file2", r.GetNames(0));
            UNIT_ASSERT_VALUES_EQUAL(1, r.NodesSize());
            UNIT_ASSERT_VALUES_EQUAL(newNodeId, r.GetNodes(0).GetId());
            UNIT_ASSERT_VALUES_EQUAL(0, r.GetNodes(0).GetSize());
        }

        service.DestroySession(headers);
    }

    void DoShouldGetStorageStats(
        const bool strictFileSystemSizeEnforcementEnabled)
    {
        const ui64 filestoreSize = 2_GB;
        const ui64 autoShardsSize = 1_GB;
        const ui64 blockSize = 4_KB;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetAutomaticShardCreationEnabled(true);
        storageConfig.SetShardAllocationUnit(1_GB);
        storageConfig.SetAutomaticallyCreatedShardSize(autoShardsSize);
        storageConfig.SetThrottlingEnabled(true);
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

        ui32 nodeIdx = env.AddDynamicNode();

        TServiceClient service(env.GetRuntime(), nodeIdx);

        const TString fsId = "test";
        service.CreateFileStore(fsId, filestoreSize / blockSize);
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

        // Waiting for async stats calculation. In case
        // strictFileSystemSizeEnforcementEnabled shards know of other shards
        // and fetch statistics from them in order to have correct
        // AggregateUsedBytesCount and AggregateUsedNodesCount. These counters
        // are updated in HandleAggregateStatsCompleted and we wait for
        // EvAggregateStatsCompleted for the main filesystem and every shard.
        env.GetRuntime().AdvanceCurrentTime(TDuration::Seconds(15));
        TDispatchOptions options;
        options.FinalEvents = {
            TDispatchOptions::TFinalEventCondition(
                TEvIndexTabletPrivate::EvAggregateStatsCompleted,
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
                const ui64 shardTotalBlocksCount =
                    (strictFileSystemSizeEnforcementEnabled ? filestoreSize
                                                            : autoShardsSize) /
                    blockSize;
                UNIT_ASSERT_VALUES_EQUAL(
                    shardTotalBlocksCount,
                    shardStats.GetTotalBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(
                    fsInfo.Size / blockSize,
                    shardStats.GetUsedBlocksCount());
                UNIT_ASSERT_VALUES_EQUAL(1, shardStats.GetUsedNodesCount());
                UNIT_ASSERT_VALUES_UNEQUAL(0, shardStats.GetCurrentLoad());
            }
        };

        {
            NProtoPrivate::TGetStorageStatsRequest request;
            request.SetFileSystemId(fsId);
            request.SetAllowCache(true);

            NProtoPrivate::TGetStorageStatsResponse response =
                GetStorageStats(service, request);

            checkShardStats(response.GetStats());
        }

        for (ui64 i = 0; i < shardsCount; ++i) {
            NProtoPrivate::TGetStorageStatsRequest request;
            request.SetFileSystemId(fileSystems[i].Id);
            request.SetAllowCache(false);
            request.SetMode(
                NProtoPrivate::STATS_REQUEST_MODE_FORCE_FETCH_SHARDS);

            NProtoPrivate::TGetStorageStatsResponse response =
                GetStorageStats(service, request);
            const NProtoPrivate::TStorageStats& stats = response.GetStats();

            if (strictFileSystemSizeEnforcementEnabled) {
                // Check that shards have stats from other shards
                checkShardStats(response.GetStats());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(0, stats.ShardStatsSize());
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

    void DoShouldGetStorageStatsFormShardlessFilesystem(
        const bool strictFileSystemSizeEnforcementEnabled)
    {
        // Create a filesystem without shards
        const ui64 filestoreSize = 1_GB;
        const ui64 autoShardsSize = 2_GB;
        const ui64 blockSize = 4_KB;
        const ui64 filestoreBlockSize = filestoreSize / blockSize;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetAutomaticShardCreationEnabled(true);
        storageConfig.SetShardAllocationUnit(2_GB);
        storageConfig.SetAutomaticallyCreatedShardSize(autoShardsSize);
        storageConfig.SetThrottlingEnabled(true);
        storageConfig.SetStrictFileSystemSizeEnforcementEnabled(
            strictFileSystemSizeEnforcementEnabled);

        TTestEnv env{{}, storageConfig};

        ui32 nodeIdx = env.AddDynamicNode();

        TServiceClient service(env.GetRuntime(), nodeIdx);

        const TString fsId = "test";
        service.CreateFileStore(fsId, filestoreBlockSize);
        // Wait for IndexTablet to restart triggered by ConfigureShards.
        // ConfigureShards is called only if
        // strictFileSystemSizeEnforcementEnabled.
        if (strictFileSystemSizeEnforcementEnabled) {
            WaitForTabletStart(service);
        }

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

        env.GetRuntime().AdvanceCurrentTime(TDuration::Seconds(30));

        // If strictFileSystemSizeEnforcementEnabled the IndexTablet restarts
        // and we need to pump one more EvUpdateCounters.
        const ui32 requiredUdpdateEventsCount =
            strictFileSystemSizeEnforcementEnabled ? 2 : 1;
        TDispatchOptions options;
        options.FinalEvents = {TDispatchOptions::TFinalEventCondition(
            TEvIndexTabletPrivate::EvUpdateCounters,
            requiredUdpdateEventsCount)};
        env.GetRuntime().DispatchEvents(options);

        NProtoPrivate::TGetStorageStatsRequest request;
        request.SetFileSystemId(fsId);
        request.SetAllowCache(true);

        NProtoPrivate::TGetStorageStatsResponse response =
            GetStorageStats(service, request);

        const auto& stats = response.GetStats();
        const auto usedBlocks = stats.GetUsedBlocksCount();
        const auto usedBytes = usedBlocks * blockSize;

        UNIT_ASSERT_VALUES_EQUAL(2, stats.GetUsedNodesCount());
        UNIT_ASSERT_VALUES_EQUAL(2, stats.GetUsedHandlesCount());
        UNIT_ASSERT_VALUES_EQUAL(320, usedBlocks);
        UNIT_ASSERT_VALUES_EQUAL(
            filestoreBlockSize,
            stats.GetTotalBlocksCount());

        env.GetRegistry()->Update(env.GetRuntime().GetCurrentTime());

        auto counters = GetFileSystemCounters(env, fsId);

        UNIT_ASSERT_VALUES_EQUAL(
            usedBytes,
            counters->GetCounter("UsedBytesCount")->GetAtomic());
        UNIT_ASSERT_VALUES_EQUAL(
            usedBytes,
            counters->GetCounter("AggregateUsedBytesCount")->GetAtomic());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            counters->GetCounter("AggregateUsedNodesCount")->GetAtomic());

        service.DestroySession(headers);
    }

    Y_UNIT_TEST(ShouldGetStorageStatsForShardlessFSWithStrictSizeEnforcement)
    {
        const bool strictFileSystemSizeEnforcementEnabled = true;
        DoShouldGetStorageStatsFormShardlessFilesystem(
            strictFileSystemSizeEnforcementEnabled);
    }

    Y_UNIT_TEST(ShouldGetStorageStatsForShardlessFSWithoutStrictSizeEnforcement)
    {
        const bool strictFileSystemSizeEnforcementEnabled = false;
        DoShouldGetStorageStatsFormShardlessFilesystem(
            strictFileSystemSizeEnforcementEnabled);
    }

    Y_UNIT_TEST(ShouldGetStorageStatsWithFileSystemSizeEnforcement)
    {
        const bool strictFileSystemSizeEnforcementEnabled = true;
        DoShouldGetStorageStats(strictFileSystemSizeEnforcementEnabled);
    }

    Y_UNIT_TEST(ShouldGetStorageStatsWithoutFileSystemSizeEnforcement)
    {
        const bool strictFileSystemSizeEnforcementEnabled = false;
        DoShouldGetStorageStats(strictFileSystemSizeEnforcementEnabled);
    }

    Y_UNIT_TEST(ShouldRunForcedOperation)
    {
        NProto::TStorageConfig config;
        config.SetCompactionThreshold(1000);
        TTestEnv env({}, config);

        ui32 nodeIdx = env.AddDynamicNode();

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
        env.GetRuntime().Send(completion.Release(), nodeIdx);

        TDispatchOptions options;
        options.CustomFinalCondition = [&compactionCounter]()
        {
            return compactionCounter == 4;
        };
        env.GetRuntime().DispatchEvents(options);

        UNIT_ASSERT_VALUES_EQUAL(4, compactionCounter);

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

    Y_UNIT_TEST(ShouldMarkNodeRefsExhaustive)
    {
        NProto::TStorageConfig config;
        config.SetInMemoryIndexCacheEnabled(true);
        config.SetInMemoryIndexCacheNodesCapacity(10);
        config.SetInMemoryIndexCacheNodeRefsCapacity(10);
        config.SetInMemoryIndexCacheNodeRefsExhaustivenessCapacity(1);
        TTestEnv env({}, config);

        ui32 nodeIdx = env.AddDynamicNode();

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const TString fsId = "test";
        service.CreateFileStore(fsId, 1'000);

        auto headers = service.InitSession(fsId, "client");

        auto getCacheHit = [&]() -> i64
        {
            env.GetRegistry()->Update(env.GetRuntime().GetCurrentTime());
            return GetFileSystemCounters(env, fsId)
                ->GetCounter("InMemoryIndexStateROCacheHitCount")
                ->GetAtomic();
        };

        // Create a directory with children. Both refs and exhaustiveness are
        // cached on creation.
        const ui64 dirId =
            service
                .CreateNode(
                    headers,
                    TCreateNodeArgs::Directory(RootNodeId, "testdir"))
                ->Record.GetNode()
                .GetId();
        service.CreateNode(headers, TCreateNodeArgs::File(dirId, "file1"));
        service.CreateNode(headers, TCreateNodeArgs::File(dirId, "file2"));

        // Creating a second directory evicts dirId from the exhaustiveness LRU
        // (capacity=1). Refs remain in cache; only exhaustiveness is lost.
        service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(RootNodeId, "other"));

        // The action restores exhaustiveness for dirId without a listing.
        auto response = ExecuteMarkNodeRefsExhaustive(service, fsId, dirId);
        UNIT_ASSERT_C(!HasError(response.GetError()), response.GetError());

        // Cache hit: refs were in cache and exhaustiveness was restored by the action.
        auto listResponse = service.ListNodes(headers, dirId);
        UNIT_ASSERT_VALUES_EQUAL(2, listResponse->Record.GetNodes().size());
        UNIT_ASSERT_VALUES_EQUAL(1, getCacheHit());
    }

    Y_UNIT_TEST(ShouldNotLeakFeaturesAcrossFileStoresWithSharedConfig)
    {
        // Reproduce the bug: all tablet actors share the same TStorageConfig
        // passed by shared_ptr. When SetCloudFolderEntity is modifies the
        // shared ProtoConfig for all tablets, not only the matching one
        NCloud::NProto::TFeaturesConfig featuresConfigProto;
        auto* feature = featuresConfigProto.AddFeatures();
        feature->SetName("ParentlessFilesOnly");
        feature->MutableWhitelist()->AddEntityIds("with-feature");

        NProto::TStorageConfig config;
        TTestEnv env{{}, config};
        env.GetStorageConfig()->SetFeaturesConfig(
            NFeatures::TFeaturesConfig(featuresConfigProto));

        ui32 nodeIdx = env.AddDynamicNode();
        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateFileStore("with-feature", 1'000);
        service.CreateFileStore("without-feature", 1'000);

        {
            auto response = ExecuteGetStorageConfig("with-feature", service);
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                response.GetParentlessFilesOnly());
        }

        {
            auto response = ExecuteGetStorageConfig("without-feature", service);
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                response.GetParentlessFilesOnly());
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
