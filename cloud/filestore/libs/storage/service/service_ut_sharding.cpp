#include "service.h"
#include "service_private.h"
#include "service_ut_sharding.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/model/utils.h>
#include <cloud/filestore/libs/storage/testlib/service_client.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>
#include <cloud/filestore/private/api/protos/actions.pb.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <contrib/ydb/core/base/hive.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString GenerateValidateData(ui32 size, ui32 seed = 0)
{
    TString data(size, 0);
    for (ui32 i = 0; i < size; ++i) {
        data[i] = 'A' + ((i + seed) % ('Z' - 'A' + 1));
    }
    return data;
}

NProto::TStorageConfig MakeStorageConfig()
{
    NProto::TStorageConfig config;
    return config;
}

NProto::TStorageConfig MakeStorageConfigWithShardIdSelectionInLeader()
{
    NProto::TStorageConfig config;
    config.SetShardIdSelectionInLeaderEnabled(true);
    return config;
}

NProtoPrivate::TGetFileSystemTopologyResponse GetFileSystemTopology(
    TServiceClient& service,
    const TString& fsId)
{
    NProtoPrivate::TGetFileSystemTopologyRequest request;
    request.SetFileSystemId(fsId);

    TString buf;
    google::protobuf::util::MessageToJsonString(request, &buf);
    const auto actionResponse =
        service.ExecuteAction("getfilesystemtopology", buf);
    NProtoPrivate::TGetFileSystemTopologyResponse response;
    auto status = google::protobuf::util::JsonStringToMessage(
        actionResponse->Record.GetOutput(),
        &response);

    return response;
}

NProtoPrivate::TGetStorageStatsResponse GetStorageStats(
    TServiceClient& service,
    const TString& fsId,
    const bool allowCache = false,
    const NProtoPrivate::StatsRequestMode mode =
        NProtoPrivate::STATS_REQUEST_MODE_DEFAULT)
{
    NProtoPrivate::TGetStorageStatsRequest request;
    request.SetFileSystemId(fsId);
    request.SetAllowCache(allowCache);
    request.SetMode(mode);
    TString buf;
    google::protobuf::util::MessageToJsonString(request, &buf);
    const auto actionResponse = service.ExecuteAction("GetStorageStats", buf);
    NProtoPrivate::TGetStorageStatsResponse response;
    auto status = google::protobuf::util::JsonStringToMessage(
        actionResponse->Record.GetOutput(),
        &response);

    return response;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define SERVICE_TEST_DECL(name)                                                \
    void TestImpl##name(NProto::TStorageConfig config)                         \
// SERVICE_TEST_DECL

#define SERVICE_TEST_SID_SELECT_IN_LEADER(name)                                \
    SERVICE_TEST_DECL(name);                                                   \
    Y_UNIT_TEST(name)                                                          \
    {                                                                          \
        TestImpl##name(MakeStorageConfig());                                   \
    }                                                                          \
    Y_UNIT_TEST(name##WithShardIdSelectionInLeader)                            \
    {                                                                          \
        TestImpl##name(MakeStorageConfigWithShardIdSelectionInLeader());       \
    }                                                                          \
    SERVICE_TEST_DECL(name)                                                    \
// SERVICE_TEST_SID_SELECT_IN_LEADER

#define SERVICE_TEST_SIMPLE(name)                                              \
    SERVICE_TEST_DECL(name);                                                   \
    Y_UNIT_TEST(name)                                                          \
    {                                                                          \
        TestImpl##name(MakeStorageConfig());                                   \
    }                                                                          \
    SERVICE_TEST_DECL(name)                                                    \
// SERVICE_TEST_SIMPLE

#define SERVICE_TEST_SID_SELECT_IN_LEADER_ONLY(name)                           \
    SERVICE_TEST_DECL(name);                                                   \
    Y_UNIT_TEST(name##WithShardIdSelectionInLeader)                            \
    {                                                                          \
        TestImpl##name(MakeStorageConfigWithShardIdSelectionInLeader());       \
    }                                                                          \
    SERVICE_TEST_DECL(name)                                                    \
// SERVICE_TEST_SID_SELECT_IN_LEADER_ONLY

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStorageServiceShardingTest)
{
    const auto StartupEventType =
        TEvIndexTabletPrivate::EvLoadCompactionMapChunkRequest;

    void WaitForTabletStart(TServiceClient& service)
    {
        TDispatchOptions options;
        options.FinalEvents = {
            TDispatchOptions::TFinalEventCondition(StartupEventType)};
        service.AccessRuntime().DispatchEvents(options);
    }

    void CatchActorIds(TServiceClient& service, TVector<TActorId>& ids)
    {
        service.AccessRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case StartupEventType: {
                        if (Find(ids, event->Sender) == ids.end()) {
                            ids.push_back(event->Sender);
                        }

                        break;
                    }
                }

                return false;
            });
    }

#define CREATE_ENV_AND_SHARDED_FILESYSTEM()                                   \
    /* Enable automatic shard creation */                                     \
    config.SetAutomaticShardCreationEnabled(true);                            \
    config.SetAutomaticallyCreatedShardSize(fsConfig.ShardBlockCount * 4_KB); \
    config.SetShardAllocationUnit(fsConfig.ShardBlockCount * 4_KB);           \
                                                                              \
    TTestEnv env({}, config);                                                 \
    env.CreateSubDomain("nfs");                                               \
                                                                              \
    ui32 nodeIdx = env.CreateNode("nfs");                                     \
                                                                              \
    TServiceClient service(env.GetRuntime(), nodeIdx);                        \
    auto fsInfo = CreateFileSystem(service, fsConfig);                        \
    Y_UNUSED(fsInfo);                                                         \
    // CREATE_ENV_AND_SHARDED_FILESYSTEM

    SERVICE_TEST_SIMPLE(ShouldCreateSessionInShards)
    {
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");
        auto headers1 = headers;
        headers1.FileSystemId = fsConfig.Shard1Id;
        auto headers2 = headers;
        headers2.FileSystemId = fsConfig.Shard2Id;

        ui64 nodeId1 =
            service
                .CreateNode(headers1, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();

        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        ui64 handle1 =
            service
                .CreateHandle(
                    headers1,
                    headers1.FileSystemId,
                    nodeId1,
                    "",
                    TCreateHandleArgs::RDWR)
                ->Record.GetHandle();

        UNIT_ASSERT_C(
            handle1 >= (1LU << 56U) && handle1 < (2LU << 56U),
            handle1);

        service.WriteData(
            headers1,
            headers1.FileSystemId,
            nodeId1,
            handle1,
            0,
            TString(1_MB, 'a'));

        ui64 nodeId2 =
            service
                .CreateNode(headers2, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();

        UNIT_ASSERT_VALUES_EQUAL((2LU << 56U) + 2, nodeId2);

        ui64 handle2 =
            service
                .CreateHandle(
                    headers2,
                    headers2.FileSystemId,
                    nodeId2,
                    "",
                    TCreateHandleArgs::RDWR)
                ->Record.GetHandle();

        UNIT_ASSERT_C(
            handle2 >= (2LU << 56U) && handle2 < (3LU << 56U),
            handle2);

        service.WriteData(
            headers2,
            headers2.FileSystemId,
            nodeId2,
            handle2,
            0,
            TString(1_MB, 'a'));

        for (const auto& shardId: fsConfig.ShardIds()) {
            NProtoPrivate::TDescribeSessionsRequest request;
            request.SetFileSystemId(shardId);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("describesessions", buf);
            NProtoPrivate::TDescribeSessionsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());

            const auto& sessions = response.GetSessions();
            UNIT_ASSERT_VALUES_EQUAL(1, sessions.size());

            UNIT_ASSERT_VALUES_EQUAL(
                headers.SessionId,
                sessions[0].GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(
                headers.ClientId,
                sessions[0].GetClientId());
            UNIT_ASSERT_VALUES_EQUAL("", sessions[0].GetSessionState());
        }

        const TString sessionState = "some_state";
        service.ResetSession(headers, sessionState);

        for (const auto& shardId: fsConfig.ShardIds()) {
            NProtoPrivate::TDescribeSessionsRequest request;
            request.SetFileSystemId(shardId);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("describesessions", buf);
            NProtoPrivate::TDescribeSessionsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());

            const auto& sessions = response.GetSessions();
            UNIT_ASSERT_VALUES_EQUAL(1, sessions.size());

            UNIT_ASSERT_VALUES_EQUAL(
                headers.SessionId,
                sessions[0].GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(
                headers.ClientId,
                sessions[0].GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(
                sessionState,
                sessions[0].GetSessionState());
        }

        service.DestroySession(headers);

        for (const auto& shardId: fsConfig.ShardIds()) {
            NProtoPrivate::TDescribeSessionsRequest request;
            request.SetFileSystemId(shardId);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("describesessions", buf);
            NProtoPrivate::TDescribeSessionsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());

            const auto& sessions = response.GetSessions();
            UNIT_ASSERT_VALUES_EQUAL(0, sessions.size());
        }
    }

    SERVICE_TEST_SIMPLE(ShouldRestoreSessionInShardAfterShardRestart)
    {
        const auto idleSessionTimeout = TDuration::Minutes(2);
        config.SetIdleSessionTimeout(idleSessionTimeout.MilliSeconds());

        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");
        auto headers1 = headers;
        headers1.FileSystemId = fsConfig.Shard1Id;
        auto headers2 = headers;
        headers2.FileSystemId = fsConfig.Shard2Id;

        // creating nodes and handles in both shards

        ui64 nodeId1 =
            service
                .CreateNode(headers1, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();

        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        ui64 handle1 =
            service
                .CreateHandle(
                    headers1,
                    headers1.FileSystemId,
                    nodeId1,
                    "",
                    TCreateHandleArgs::RDWR)
                ->Record.GetHandle();

        UNIT_ASSERT_C(
            handle1 >= (1LU << 56U) && handle1 < (2LU << 56U),
            handle1);

        ui64 nodeId2 =
            service
                .CreateNode(headers2, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();

        UNIT_ASSERT_VALUES_EQUAL((2LU << 56U) + 2, nodeId2);

        ui64 handle2 =
            service
                .CreateHandle(
                    headers2,
                    headers2.FileSystemId,
                    nodeId2,
                    "",
                    TCreateHandleArgs::RDWR)
                ->Record.GetHandle();

        UNIT_ASSERT_C(
            handle2 >= (2LU << 56U) && handle2 < (3LU << 56U),
            handle2);

        // rebooting shards

        TVector<TActorId> fsActorIds;
        CatchActorIds(service, fsActorIds);

        TIndexTabletClient shard1(
            env.GetRuntime(),
            nodeIdx,
            fsInfo.Shard1TabletId,
            {}, // config
            false // updateConfig
        );
        shard1.RebootTablet();

        TIndexTabletClient shard2(
            env.GetRuntime(),
            nodeIdx,
            fsInfo.Shard2TabletId,
            {}, // config
            false // updateConfig
        );
        shard2.RebootTablet();

        // triggering shard sessions sync
        // sending the event manually since registration observers which enable
        // scheduling for actors are reset upon tablet reboot

        env.GetRuntime().AdvanceCurrentTime(idleSessionTimeout / 2);

        {
            using TRequest = TEvIndexTabletPrivate::TEvSyncSessionsRequest;

            env.GetRuntime().Send(
                new IEventHandle(
                    fsInfo.MainTabletActorId, // recipient
                    TActorId(), // sender
                    new TRequest(),
                    0, // flags
                    0),
                0);
        }

        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));
        // waiting for idle session expiration
        // sending the event manually since registration observers which enable
        // scheduling for actors are reset upon tablet reboot

        env.GetRuntime().AdvanceCurrentTime(idleSessionTimeout / 2);
        service.PingSession(headers);

        for (const auto& actorId: fsActorIds) {
            using TRequest = TEvIndexTabletPrivate::TEvCleanupSessionsRequest;

            env.GetRuntime().Send(
                new IEventHandle(
                    actorId, // recipient
                    TActorId(), // sender
                    new TRequest(),
                    0, // flags
                    0),
                0);
        }

        // need to pass deadline instead of timeout here since otherwise the
        // adjusted time gets added to the timeout
        env.GetRuntime().DispatchEvents(
            {},
            TInstant::Now() + TDuration::MilliSeconds(100));

        // shard sessions should exist

        for (const auto& id: fsConfig.MainAndShardIds()) {
            NProtoPrivate::TDescribeSessionsRequest request;
            request.SetFileSystemId(id);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("describesessions", buf);
            NProtoPrivate::TDescribeSessionsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());

            const auto& sessions = response.GetSessions();
            UNIT_ASSERT_VALUES_EQUAL(1, sessions.size());

            UNIT_ASSERT_VALUES_EQUAL(
                headers.SessionId,
                sessions[0].GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(
                headers.ClientId,
                sessions[0].GetClientId());
        }

        // handles should be alive

        service.WriteData(
            headers1,
            headers1.FileSystemId,
            nodeId1,
            handle1,
            0,
            TString(1_MB, 'a'));

        service.WriteData(
            headers2,
            headers2.FileSystemId,
            nodeId2,
            handle2,
            0,
            TString(1_MB, 'a'));
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(ShouldCreateNodeInShardViaLeader)
    {
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        ui64 nodeId1 =
            service
                .CreateNode(headers, TCreateNodeArgs::File(
                    RootNodeId,
                    "file1",
                    0, // mode
                    fsConfig.Shard1Id))
                ->Record.GetNode()
                .GetId();

        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        auto createHandleResponse = service.CreateHandle(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::RDWR)->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            fsConfig.Shard1Id,
            createHandleResponse.GetShardFileSystemId());

        UNIT_ASSERT_VALUES_UNEQUAL(
            "",
            createHandleResponse.GetShardNodeName());

        auto headers1 = headers;
        headers1.FileSystemId = fsConfig.Shard1Id;

        createHandleResponse = service.CreateHandle(
            headers1,
            headers1.FileSystemId,
            RootNodeId,
            createHandleResponse.GetShardNodeName(),
            TCreateHandleArgs::RDWR)->Record;

        auto handle1 = createHandleResponse.GetHandle();

        UNIT_ASSERT_C(
            handle1 >= (1LU << 56U) && handle1 < (2LU << 56U),
            handle1);

        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            createHandleResponse.GetNodeAttr().GetId());

        service.WriteData(
            headers1,
            headers1.FileSystemId,
            nodeId1,
            handle1,
            0,
            TString(1_MB, 'a'));
    }

    void DoShouldCheckAttrForNodeCreatedInShardViaLeader(
        NProto::TStorageConfig & config,
        std::function<TSetNodeAttrArgs()> newArgsConstructor,
        bool shouldTriggerCriticalEvent)
    {
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        TAutoPtr<IEventHandle> createNodeInShard;
        TString name;
        bool intercept = true;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& e) {
                Y_UNUSED(runtime);
                if (e->GetTypeRewrite() == TEvService::EvCreateNodeRequest) {
                    const auto* msg =
                        e->Get<TEvService::TEvCreateNodeRequest>();
                    if (intercept && msg->Record.GetFileSystemId()
                            == fsConfig.Shard1Id)
                    {
                        name = msg->Record.GetName();
                        createNodeInShard = e;
                        return true;
                    }
                }
                return false;
            });

        service.SendCreateNodeRequest(
            headers,
            TCreateNodeArgs::File(
                RootNodeId,
                "file1",
                0, // mode
                fsConfig.Shard1Id));

        ui32 iterations = 0;
        while (!createNodeInShard && iterations++ < 100) {
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT(createNodeInShard);
        intercept = false;

        auto headers1 = headers;
        headers1.FileSystemId = fsConfig.Shard1Id;

        const auto nodeId = service.CreateNode(
            headers1,
            TCreateNodeArgs::File(RootNodeId, name))->Record.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(nodeId));
        TSetNodeAttrArgs newArgs = newArgsConstructor();
        newArgs.Node = nodeId;
        service.SetNodeAttr(headers1, fsConfig.Shard1Id, newArgs);

        const auto counters =
            env.GetCounters()->FindSubgroup("component", "service");
        UNIT_ASSERT(counters);
        const auto counter = counters->GetCounter(
            "AppCriticalEvents/CreateNodeRequestResponseMismatchInShard");

        UNIT_ASSERT_VALUES_EQUAL(0, counter->GetAtomic());

        env.GetRuntime().Send(createNodeInShard.Release());

        auto response = service.RecvCreateNodeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason());

        UNIT_ASSERT_VALUES_EQUAL(nodeId, response->Record.GetNode().GetId());

        UNIT_ASSERT_VALUES_EQUAL(
            shouldTriggerCriticalEvent,
            counter->GetAtomic());
    }

    SERVICE_TEST_SIMPLE(ShouldCheckAttrForNodeCreatedInShardViaLeader)
    {
        // If the size is changed with mtime update, this is an acceptable
        // situation and should not trigger the critical event
        DoShouldCheckAttrForNodeCreatedInShardViaLeader(
            config,
            []()
            {
                return TSetNodeAttrArgs(InvalidNodeId)
                    .SetSize(1_MB)
                    .SetMTime(
                        (TInstant::Now() + TDuration::Hours(1)).MicroSeconds());
            },
            false);

        // Even if the MTime is within a reasonable range, it should not
        // trigger the critical event
        DoShouldCheckAttrForNodeCreatedInShardViaLeader(
            config,
            []()
            {
                return TSetNodeAttrArgs(InvalidNodeId)
                    .SetSize(1_MB)
                    .SetMTime((TInstant::Now() - TDuration::Seconds(10))
                                  .MicroSeconds());
            },
            false);

        // If the MTime is too long ago, it should trigger the critical
        // event, since it is likely that the creation has overwritten
        // the node created earlier in the shard
        DoShouldCheckAttrForNodeCreatedInShardViaLeader(
            config,
            []()
            {
                return TSetNodeAttrArgs(InvalidNodeId)
                    .SetSize(1_MB)
                    .SetMTime((TInstant::Now() - TDuration::Minutes(10))
                                  .MicroSeconds());
            },
            true);
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(
        ShouldCreateNodeInShardByCreateHandleViaLeader)
    {
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        auto createHandleResponse = service.CreateHandle(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::CREATE,
            fsConfig.Shard1Id)->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            fsConfig.Shard1Id,
            createHandleResponse.GetShardFileSystemId());

        const auto shardNodeName =
            createHandleResponse.GetShardNodeName();

        UNIT_ASSERT_VALUES_UNEQUAL("", shardNodeName);

        const auto nodeId1 = createHandleResponse.GetNodeAttr().GetId();
        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        auto headers1 = headers;
        headers1.FileSystemId = fsConfig.Shard1Id;

        createHandleResponse = service.CreateHandle(
            headers1,
            headers1.FileSystemId,
            RootNodeId,
            shardNodeName,
            TCreateHandleArgs::RDWR)->Record;

        auto handle1 = createHandleResponse.GetHandle();

        UNIT_ASSERT_C(
            handle1 >= (1LU << 56U) && handle1 < (2LU << 56U),
            handle1);

        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            createHandleResponse.GetNodeAttr().GetId());

        service.WriteData(
            headers1,
            headers1.FileSystemId,
            nodeId1,
            handle1,
            0,
            TString(1_MB, 'a'));

        auto getAttrResponse = service.GetNodeAttr(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file1")->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            fsConfig.Shard1Id,
            getAttrResponse.GetNode().GetShardFileSystemId());

        UNIT_ASSERT_VALUES_EQUAL(
            shardNodeName,
            getAttrResponse.GetNode().GetShardNodeName());

        getAttrResponse = service.GetNodeAttr(
            headers1,
            fsConfig.Shard1Id,
            RootNodeId,
            shardNodeName)->Record;

        UNIT_ASSERT_VALUES_EQUAL(nodeId1, getAttrResponse.GetNode().GetId());
        UNIT_ASSERT_VALUES_EQUAL(1_MB, getAttrResponse.GetNode().GetSize());

        auto listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("file1", listNodesResponse.GetNames(0));

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            fsConfig.Shard1Id,
            listNodesResponse.GetNodes(0).GetShardFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL(
            shardNodeName,
            listNodesResponse.GetNodes(0).GetShardNodeName());
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(ShouldForwardRequestsToShard)
    {
        config.SetLazyXAttrsEnabled(false);
        config.SetMultiTabletForwardingEnabled(true);

        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        auto createHandleResponse = service.CreateHandle(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::CREATE_EXL)->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            createHandleResponse.GetShardFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            createHandleResponse.GetShardNodeName());

        const auto nodeId1 = createHandleResponse.GetNodeAttr().GetId();
        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        const auto handle1 = createHandleResponse.GetHandle();

        UNIT_ASSERT_C(
            handle1 >= (1LU << 56U) && handle1 < (2LU << 56U),
            handle1);

        auto accessNodeResponse = service.AccessNode(
            headers,
            fsConfig.FsId,
            nodeId1)->Record;

        Y_UNUSED(accessNodeResponse);

        auto setNodeAttrResponse = service.SetNodeAttr(
            headers,
            fsConfig.FsId,
            nodeId1,
            1_MB)->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            setNodeAttrResponse.GetNode().GetId());

        UNIT_ASSERT_VALUES_EQUAL(1_MB, setNodeAttrResponse.GetNode().GetSize());

        auto allocateDataResponse = service.AllocateData(
            headers,
            fsConfig.FsId,
            nodeId1,
            handle1,
            0,
            2_MB)->Record;

        Y_UNUSED(allocateDataResponse);

        auto data = GenerateValidateData(256_KB);
        service.WriteData(headers, fsConfig.FsId, nodeId1, handle1, 0, data);
        auto readDataResponse = service.ReadData(
            headers,
            fsConfig.FsId,
            nodeId1,
            handle1,
            0,
            data.size())->Record;
        UNIT_ASSERT_VALUES_EQUAL(data, readDataResponse.GetBuffer());

        auto destroyHandleResponse = service.DestroyHandle(
            headers,
            fsConfig.FsId,
            nodeId1,
            handle1)->Record;

        Y_UNUSED(destroyHandleResponse);

        const auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file2"))->Record;

        const auto nodeId2 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL((2LU << 56U) + 2, nodeId2);

        auto getNodeAttrResponse = service.GetNodeAttr(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file1")->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            getNodeAttrResponse.GetNode().GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            2_MB,
            getNodeAttrResponse.GetNode().GetSize());

        getNodeAttrResponse = service.GetNodeAttr(
            headers,
            fsConfig.FsId,
            nodeId1,
            "")->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            getNodeAttrResponse.GetNode().GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            2_MB,
            getNodeAttrResponse.GetNode().GetSize());

        auto listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(2, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("file1", listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL("file2", listNodesResponse.GetNames(1));

        UNIT_ASSERT_VALUES_EQUAL(2, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            listNodesResponse.GetNodes(0).GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            2_MB,
            listNodesResponse.GetNodes(0).GetSize());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId2,
            listNodesResponse.GetNodes(1).GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            listNodesResponse.GetNodes(1).GetSize());

        service.UnlinkNode(headers, RootNodeId, "file1");

        listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("file2", listNodesResponse.GetNames(0));

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId2,
            listNodesResponse.GetNodes(0).GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            listNodesResponse.GetNodes(0).GetSize());

        auto headers1 = headers;
        headers1.FileSystemId = fsConfig.Shard1Id;

        listNodesResponse = service.ListNodes(
            headers1,
            fsConfig.Shard1Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());

        auto headers2 = headers;
        headers2.FileSystemId = fsConfig.Shard2Id;

        listNodesResponse = service.ListNodes(
            headers2,
            fsConfig.Shard2Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());

        auto setXAttrResponse = service.SetNodeXAttr(
            headers,
            fsConfig.FsId,
            nodeId2,
            "user.some_attr",
            "some_value")->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, setXAttrResponse.GetVersion());

        auto getXAttrResponse = service.GetNodeXAttr(
            headers,
            fsConfig.FsId,
            nodeId2,
            "user.some_attr")->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, getXAttrResponse.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL("some_value", getXAttrResponse.GetValue());

        service.SendSetNodeXAttrRequest(
            headers,
            fsConfig.FsId,
            nodeId1,
            "user.some_attr",
            "some_value");

        auto setNodeXAttrResponseEvent = service.RecvSetNodeXAttrResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_FS_NOENT,
            setNodeXAttrResponseEvent->GetStatus(),
            setNodeXAttrResponseEvent->GetErrorReason());
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(ShouldSetHasXAttrsFlagOnFirstSetNodeXAttr)
    {
        for (bool lazyXAttrsEnabled: {false, true}) {
            config.SetLazyXAttrsEnabled(lazyXAttrsEnabled);
            config.SetMultiTabletForwardingEnabled(true);

            TShardedFileSystemConfig fsConfig;
            CREATE_ENV_AND_SHARDED_FILESYSTEM();

            auto counters =
                env.GetCounters()->FindSubgroup("component", "service");

            auto getCount = [&counters](const char* name)
            {
                return counters->FindSubgroup("request", name)
                    ->GetCounter("Count")
                    ->GetAtomic();
            };
            auto getErrorCount = [&counters](const char* name)
            {
                return counters->FindSubgroup("request", name)
                    ->GetCounter("Errors")
                    ->GetAtomic();
            };

            // Create initial session
            THeaders headers;
            auto session =
                service.InitSession(headers, fsConfig.FsId, "client");
            if (lazyXAttrsEnabled) {
                // There should be no XAttrs at the begining if
                // lazyXAttrsEnabled is true
                UNIT_ASSERT(!session->Record.GetFileStore()
                                 .GetFeatures()
                                 .GetHasXAttrs());
            } else {
                // Otherwise it's always true
                UNIT_ASSERT(session->Record.GetFileStore()
                                .GetFeatures()
                                .GetHasXAttrs());
            }

            // Create two files: 'file1', 'file2'
            const auto createNodeResponse1 =
                service
                    .CreateNode(
                        headers,
                        TCreateNodeArgs::File(RootNodeId, "file1"))
                    ->Record;
            const auto nodeId1 = createNodeResponse1.GetNode().GetId();

            const auto createNodeResponse2 =
                service
                    .CreateNode(
                        headers,
                        TCreateNodeArgs::File(RootNodeId, "file2"))
                    ->Record;
            const auto nodeId2 = createNodeResponse2.GetNode().GetId();

            // Check that GetNodeXAttr returns error 'E_FS_NOXATTR' in case
            // 'HasXAttrs' is false
            auto getXAttrResponse = service
                                        .AssertGetNodeXAttrFailed(
                                            headers,
                                            fsConfig.FsId,
                                            nodeId1,
                                            "user.some_attr")
                                        ->Record;
            UNIT_ASSERT_VALUES_EQUAL(
                ui32(NProto::E_FS_NOXATTR),
                STATUS_FROM_CODE(getXAttrResponse.GetError().GetCode()));
            UNIT_ASSERT_VALUES_EQUAL(1, getErrorCount("GetNodeXAttr"));

            auto listXAttrResponse =
                service.ListNodeXAttr(headers, fsConfig.FsId, nodeId1)->Record;
            UNIT_ASSERT(listXAttrResponse.GetNames().empty());
            UNIT_ASSERT_VALUES_EQUAL(1, getCount("ListNodeXAttr"));

            if (lazyXAttrsEnabled) {
                // If lazyXAttrsEnabled is true the first attempt should fail
                // and cause the main tablet to restart
                service.AssertSetNodeXAttrFailed(
                    headers,
                    fsConfig.FsId,
                    nodeId1,
                    "user.some_attr1",
                    "some_value1");

                WaitForTabletStart(service);
                session = service.InitSession(headers, fsConfig.FsId, "client");
                // After the first XAttr is set the flag HasXAttrs should be
                // true
                UNIT_ASSERT(session->Record.GetFileStore()
                                .GetFeatures()
                                .GetHasXAttrs());
            }

            service.SetNodeXAttr(
                headers,
                fsConfig.FsId,
                nodeId1,
                "user.some_attr1",
                "some_value1");

            UNIT_ASSERT_VALUES_EQUAL(1, getCount("SetNodeXAttr"));

            // Check that XAttr was seccessfully set
            auto getXAttrResponse1 = service
                                         .GetNodeXAttr(
                                             headers,
                                             fsConfig.FsId,
                                             nodeId1,
                                             "user.some_attr1")
                                         ->Record;
            UNIT_ASSERT_VALUES_EQUAL(
                "some_value1",
                getXAttrResponse1.GetValue());

            // The same check for 'file2'.
            // As this time the index tablet should not restart, we don't need
            // to create a new session
            service.SetNodeXAttr(
                headers,
                fsConfig.FsId,
                nodeId2,
                "user.some_attr2",
                "some_value2");
            UNIT_ASSERT_VALUES_EQUAL(2, getCount("SetNodeXAttr"));

            auto getXAttrResponse2 = service
                                         .GetNodeXAttr(
                                             headers,
                                             fsConfig.FsId,
                                             nodeId2,
                                             "user.some_attr2")
                                         ->Record;
            UNIT_ASSERT_VALUES_EQUAL(
                "some_value2",
                getXAttrResponse2.GetValue());
            UNIT_ASSERT_VALUES_EQUAL(2, getCount("GetNodeXAttr"));

            service.RemoveNodeXAttr(
                headers,
                fsConfig.FsId,
                nodeId1,
                "user.some_attr1");
            UNIT_ASSERT_VALUES_EQUAL(1, getCount("RemoveNodeXAttr"));

            listXAttrResponse =
                service.ListNodeXAttr(headers, fsConfig.FsId, nodeId2)->Record;
            const auto& names = listXAttrResponse.GetNames();
            UNIT_ASSERT_VALUES_EQUAL(1, names.size());
            UNIT_ASSERT_VALUES_EQUAL("user.some_attr2", names[0]);

            listXAttrResponse =
                service.ListNodeXAttr(headers, fsConfig.FsId, nodeId1)->Record;
            UNIT_ASSERT(names.empty());

            UNIT_ASSERT_VALUES_EQUAL(3, getCount("ListNodeXAttr"));
        }
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(ShouldCreateDirectoryStructureInLeader)
    {
        config.SetMultiTabletForwardingEnabled(true);

        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(RootNodeId, "dir1"))->Record;
        const auto dir1Id = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(0, ExtractShardNo(dir1Id));

        createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(dir1Id, "dir1_1"))->Record;
        const auto dir1_1Id = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(0, ExtractShardNo(dir1_1Id));

        createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(dir1Id, "dir1_2"))->Record;
        const auto dir1_2Id = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(0, ExtractShardNo(dir1_2Id));

        auto createHandleResponse = service.CreateHandle(
            headers,
            fsConfig.FsId,
            dir1_1Id,
            "file1",
            TCreateHandleArgs::CREATE)->Record;

        const auto nodeId1 = createHandleResponse.GetNodeAttr().GetId();
        UNIT_ASSERT_VALUES_EQUAL(ShardedId(2, 1), nodeId1);

        const auto handle1 = createHandleResponse.GetHandle();
        UNIT_ASSERT_C(ExtractShardNo(handle1) == 1, handle1);

        createHandleResponse = service.CreateHandle(
            headers,
            fsConfig.FsId,
            dir1_2Id,
            "file1",
            TCreateHandleArgs::CREATE)->Record;

        const auto nodeId2 = createHandleResponse.GetNodeAttr().GetId();
        UNIT_ASSERT_VALUES_EQUAL(ShardedId(2, 2), nodeId2);

        const auto handle2 = createHandleResponse.GetHandle();
        UNIT_ASSERT_C(ExtractShardNo(handle2) == 2, handle2);

        auto data = GenerateValidateData(256_KB);
        service.WriteData(headers, fsConfig.FsId, nodeId1, handle1, 0, data);
        auto readDataResponse = service.ReadData(
            headers,
            fsConfig.FsId,
            nodeId1,
            handle1,
            0,
            data.size())->Record;
        UNIT_ASSERT_VALUES_EQUAL(data, readDataResponse.GetBuffer());

        data = GenerateValidateData(1_MB);
        service.WriteData(headers, fsConfig.FsId, nodeId2, handle2, 0, data);
        readDataResponse = service.ReadData(
            headers,
            fsConfig.FsId,
            nodeId2,
            handle2,
            0,
            data.size())->Record;
        UNIT_ASSERT_VALUES_EQUAL(data, readDataResponse.GetBuffer());

        service.DestroyHandle(headers, fsConfig.FsId, nodeId1, handle1);
        service.DestroyHandle(headers, fsConfig.FsId, nodeId2, handle2);
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(ShouldNotFailOnLegacyHandlesWithHighBits)
    {
        config.SetMultiTabletForwardingEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);

        // forcing fs to set high bits for handle ids
        {
            NProtoPrivate::TConfigureAsShardRequest request;
            request.SetFileSystemId(fsId);
            request.SetShardNo(111);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("configureasshard", buf);
            NProtoPrivate::TConfigureAsShardResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());
        }

        auto headers = service.InitSession(fsId, "client");

        auto createHandleResponse = service.CreateHandle(
            headers,
            fsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::CREATE_EXL)->Record;

        const auto nodeId1 = createHandleResponse.GetNodeAttr().GetId();
        UNIT_ASSERT_VALUES_EQUAL(111, ExtractShardNo(nodeId1));

        const auto handle1 = createHandleResponse.GetHandle();
        UNIT_ASSERT_VALUES_EQUAL(111, ExtractShardNo(handle1));

        auto data = GenerateValidateData(256_KB);
        service.WriteData(headers, fsId, nodeId1, handle1, 0, data);
        auto readDataResponse = service.ReadData(
            headers,
            fsId,
            nodeId1,
            handle1,
            0,
            data.size())->Record;
        UNIT_ASSERT_VALUES_EQUAL(data, readDataResponse.GetBuffer());
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(
        ShouldHandleCreateNodeErrorFromShardUponCreateHandleViaLeader)
    {
        config.SetMultiTabletForwardingEnabled(true);

        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        auto error = MakeError(E_FAIL);

        env.GetRuntime().SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvCreateNodeResponse: {
                        auto* msg =
                            event->Get<TEvService::TEvCreateNodeResponse>();
                        if (error.GetCode()) {
                            msg->Record.MutableError()->CopyFrom(error);
                        }

                        break;
                    }
                }

                return false;
            });

        const ui64 requestId = 111;

        service.SendCreateHandleRequest(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::CREATE_EXL,
            "",
            requestId);

        auto response = service.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            error.GetCode(),
            response->GetError().GetCode(),
            FormatError(response->GetError()));

        error = {};

        service.SendCreateHandleRequest(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::CREATE_EXL,
            "",
            requestId);

        response = service.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            error.GetCode(),
            response->GetError().GetCode(),
            FormatError(response->GetError()));
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(ShouldNotFailListNodesUponGetAttrENOENT)
    {
        config.SetMultiTabletForwardingEnabled(true);

        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"));
        service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file2"));
        service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file3"));
        service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file4"));

        auto listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(4, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("file1", listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL("file2", listNodesResponse.GetNames(1));
        UNIT_ASSERT_VALUES_EQUAL("file3", listNodesResponse.GetNames(2));
        UNIT_ASSERT_VALUES_EQUAL("file4", listNodesResponse.GetNames(3));
        TVector<std::pair<ui64, TString>> nodes(4);
        for (ui32 i = 0; i < 4; ++i) {
            nodes[i] = {
                listNodesResponse.GetNodes(i).GetId(),
                listNodesResponse.GetNames(i)};
            UNIT_ASSERT_VALUES_UNEQUAL(0, nodes[i].first);
        }

        auto headers1 = headers;
        headers1.FileSystemId = fsConfig.Shard1Id;

        listNodesResponse = service.ListNodes(
            headers1,
            fsConfig.Shard1Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(2, listNodesResponse.NamesSize());
        const auto shard1NodeName1 = listNodesResponse.GetNames(0);
        const auto shard1NodeId1 = listNodesResponse.GetNodes(0).GetId();
        const auto shard1NodeName2 = listNodesResponse.GetNames(1);

        auto headers2 = headers;
        headers2.FileSystemId = fsConfig.Shard2Id;

        listNodesResponse = service.ListNodes(
            headers2,
            fsConfig.Shard2Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(2, listNodesResponse.NamesSize());
        const auto shard2NodeName1 = listNodesResponse.GetNames(0);
        const auto shard2NodeName2 = listNodesResponse.GetNames(1);

        // "breaking" one node - deleting it directly from the shard
        service.UnlinkNode(headers1, RootNodeId, shard1NodeName1);

        EraseIf(nodes, [=] (const auto& node) {
            return node.first == shard1NodeId1;
        });

        // ListNodes should still succeed
        listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            RootNodeId)->Record;

        // unresolved nodes should be removed from the response
        UNIT_ASSERT_VALUES_EQUAL(3, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            nodes[0].second,
            listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL(
            nodes[1].second,
            listNodesResponse.GetNames(1));
        UNIT_ASSERT_VALUES_EQUAL(
            nodes[2].second,
            listNodesResponse.GetNames(2));

        UNIT_ASSERT_VALUES_EQUAL(3, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            nodes[0].first,
            listNodesResponse.GetNodes(0).GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            nodes[1].first,
            listNodesResponse.GetNodes(1).GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            nodes[2].first,
            listNodesResponse.GetNodes(2).GetId());

        const auto counters =
            env.GetCounters()->FindSubgroup("component", "service");
        UNIT_ASSERT(counters);
        const auto counter =
            counters->GetCounter("AppCriticalEvents/NodeNotFoundInShard");
        UNIT_ASSERT_EQUAL(1, counter->GetAtomic());

        auto& runtime = env.GetRuntime();
        // However, ListNodes should not report a critical event in case of
        // a retriable error from the leader
        {
            runtime.SetEventFilter(
                [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
                {
                    switch (event->GetTypeRewrite()) {
                        case TEvService::EvGetNodeAttrResponse: {
                            auto* msg =
                                event
                                    ->Get<TEvService::TEvGetNodeAttrResponse>();
                            if (msg->Record.GetError().GetCode() == E_FS_NOENT)
                            {
                                msg->Record.MutableError()->CopyFrom(
                                    MakeError(E_REJECTED, "error"));
                            }
                            break;
                        }
                    }

                    return false;
                });

            auto response = service.SendAndRecvListNodes(
                headers,
                fsConfig.FsId,
                RootNodeId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetError().GetCode(),
                response->GetErrorReason());

            UNIT_ASSERT_VALUES_EQUAL(1, counter->GetAtomic());

            runtime.SetEventFilter(TTestActorRuntimeBase::DefaultFilterFunc);
        }

        // "breaking" all nodes - ListNodes should fail with E_IO after this
        service.UnlinkNode(headers1, RootNodeId, shard1NodeName2);
        service.UnlinkNode(headers2, RootNodeId, shard2NodeName1);
        service.UnlinkNode(headers2, RootNodeId, shard2NodeName2);

        service.SendListNodesRequest(headers, fsConfig.FsId, RootNodeId);
        auto response = service.RecvListNodesResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_IO,
            response->GetError().GetCode(),
            response->GetErrorReason());
    }

    SERVICE_TEST_SIMPLE(ShouldListMultipleNodesWithGetNodeAttrBatch)
    {
        config.SetMultiTabletForwardingEnabled(true);
        config.SetGetNodeAttrBatchEnabled(true);
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");
        auto headers1 = headers;
        headers1.FileSystemId = fsConfig.Shard1Id;

        TVector<TString> names;
        TVector<ui64> ids;
        for (ui32 i = 10; i < 50; ++i) {
            const auto name = Sprintf("file%u", i);
            const auto id = service.CreateNode(
                headers,
                TCreateNodeArgs::File(RootNodeId, name)
            )->Record.GetNode().GetId();
            names.push_back(name);
            ids.push_back(id);
        }

        auto listNodesResponse = service.ListNodes(
            headers1,
            fsConfig.Shard1Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            names.size() / 2,
            listNodesResponse.NamesSize());
        const ui32 idxToUnlink1 = 13;
        const ui32 idxToUnlink2 = 17;
        const auto shard1NodeName1 = listNodesResponse.GetNames(idxToUnlink1);
        const ui64 unlinkedId1 = listNodesResponse.GetNodes(idxToUnlink1).GetId();
        const auto shard1NodeName2 = listNodesResponse.GetNames(idxToUnlink2);
        const ui64 unlinkedId2 = listNodesResponse.GetNodes(idxToUnlink2).GetId();

        listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(names.size(), listNodesResponse.NamesSize());
        for (ui32 i = 0; i < names.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(names[i], listNodesResponse.GetNames(i));
            UNIT_ASSERT_VALUES_EQUAL(
                ids[i],
                listNodesResponse.GetNodes(i).GetId());
        }

        service.UnlinkNode(headers1, RootNodeId, shard1NodeName1);
        service.UnlinkNode(headers1, RootNodeId, shard1NodeName2);

        listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            RootNodeId)->Record;

        for (auto id: {unlinkedId1, unlinkedId2}) {
            auto idx = Find(ids, id) - ids.begin();
            ids.erase(ids.begin() + idx);
            names.erase(names.begin() + idx);
        }

        UNIT_ASSERT_VALUES_EQUAL(names.size(), listNodesResponse.NamesSize());
        for (ui32 i = 0; i < names.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(names[i], listNodesResponse.GetNames(i));
            UNIT_ASSERT_VALUES_EQUAL(
                ids[i],
                listNodesResponse.GetNodes(i).GetId());
        }
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(ShouldValidateRequestsWithShardId)
    {
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        const auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"))->Record;

        const auto nodeId1 = createNodeResponse.GetNode().GetId();

        auto headers1 = headers;
        headers1.FileSystemId = fsConfig.Shard1Id;

        // a request with ShardId and without Name
        service.SendCreateHandleRequest(
            headers,
            fsConfig.FsId,
            nodeId1,
            "",
            TCreateHandleArgs::RDWR,
            fsConfig.Shard1Id);

        auto createHandleResponseEvent = service.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            createHandleResponseEvent->GetError().GetCode(),
            createHandleResponseEvent->GetErrorReason());
    }

    SERVICE_TEST_SIMPLE(ShouldValidateShardConfiguration)
    {
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        const auto shard3Id = fsConfig.FsId + "-f3";
        service.CreateFileStore(shard3Id, 1'000);

        // ShardNo change not allowed
        {
            NProtoPrivate::TConfigureAsShardRequest request;
            request.SetFileSystemId(fsConfig.Shard1Id);
            request.SetShardNo(2);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("configureasshard", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetError().GetCode(),
                response->GetErrorReason());
        }

        // Shard deletion not allowed
        {
            NProtoPrivate::TConfigureShardsRequest request;
            request.SetFileSystemId(fsConfig.FsId);
            *request.AddShardFileSystemIds() = fsConfig.Shard1Id;

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("configureshards", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetError().GetCode(),
                response->GetErrorReason());
        }

        // Shard reordering not allowed
        {
            NProtoPrivate::TConfigureShardsRequest request;
            request.SetFileSystemId(fsConfig.FsId);
            *request.AddShardFileSystemIds() = fsConfig.Shard2Id;
            *request.AddShardFileSystemIds() = fsConfig.Shard1Id;

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("configureshards", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetError().GetCode(),
                response->GetErrorReason());
        }

        // Shard addition IS allowed
        {
            NProtoPrivate::TConfigureAsShardRequest request;
            request.SetFileSystemId(shard3Id);
            request.SetShardNo(3);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("configureasshard", buf);
            NProtoPrivate::TConfigureAsShardResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());
        }

        {
            NProtoPrivate::TConfigureShardsRequest request;
            request.SetFileSystemId(fsConfig.FsId);
            *request.AddShardFileSystemIds() = fsConfig.Shard1Id;
            *request.AddShardFileSystemIds() = fsConfig.Shard2Id;
            *request.AddShardFileSystemIds() = shard3Id;

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("configureshards", buf);
            NProtoPrivate::TConfigureShardsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());
        }

        // TODO(#1350): leader should check that shards' ShardNos correspond
        // to the shard order in leader's config
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(ShouldRenameExternalNodes)
    {
        config.SetMultiTabletForwardingEnabled(true);
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        // creating 2 files

        auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"))->Record;

        const auto nodeId1 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(nodeId1));

        createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file2"))->Record;

        const auto nodeId2 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(2, ExtractShardNo(nodeId2));

        ui64 handle1 = service.CreateHandle(
            headers,
            fsConfig.FsId,
            nodeId1,
            "",
            TCreateHandleArgs::RDWR)->Record.GetHandle();

        // writing some data to file1 then moving file1 to file3

        auto data1 = GenerateValidateData(256_KB);
        service.WriteData(headers, fsConfig.FsId, nodeId1, handle1, 0, data1);

        auto renameNodeResponse = service.RenameNode(
            headers,
            RootNodeId,
            "file1",
            RootNodeId,
            "file3",
            0);

        // opening file3 for reading

        auto createHandleResponse = service.CreateHandle(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file3",
            TCreateHandleArgs::RDNLY)->Record;

        // checking that file3 refers to the same node as formerly file1

        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            createHandleResponse.GetNodeAttr().GetId());

        auto handle1r = createHandleResponse.GetHandle();

        // checking that we can read the data that we wrote to file1

        auto readDataResponse = service.ReadData(
            headers,
            fsConfig.FsId,
            nodeId1,
            handle1r,
            0,
            data1.size())->Record;
        UNIT_ASSERT_VALUES_EQUAL(data1, readDataResponse.GetBuffer());

        // checking that node listing shows file3 and file2

        auto listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(2, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("file2", listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL("file3", listNodesResponse.GetNames(1));

        UNIT_ASSERT_VALUES_EQUAL(2, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId2,
            listNodesResponse.GetNodes(0).GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            listNodesResponse.GetNodes(1).GetId());

        // checking that move into an existing file (file2) works

        renameNodeResponse = service.RenameNode(
            headers,
            RootNodeId,
            "file3",
            RootNodeId,
            "file2",
            0);

        // checking that we can still read the same data

        createHandleResponse = service.CreateHandle(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file2",
            TCreateHandleArgs::RDNLY)->Record;

        // nodeId should be kept intact

        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            createHandleResponse.GetNodeAttr().GetId());

        handle1r = createHandleResponse.GetHandle();

        readDataResponse = service.ReadData(
            headers,
            fsConfig.FsId,
            nodeId1,
            handle1r,
            0,
            data1.size())->Record;
        UNIT_ASSERT_VALUES_EQUAL(data1, readDataResponse.GetBuffer());

        // listing should show only file2 now

        listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("file2", listNodesResponse.GetNames(0));

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            listNodesResponse.GetNodes(0).GetId());

        // listing in shard2 should show nothing

        auto headers2 = headers;
        headers2.FileSystemId = fsConfig.Shard2Id;

        listNodesResponse = service.ListNodes(
            headers2,
            fsConfig.Shard2Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());

        // now try to move to another subdirectory

        auto subdirId =
            service
                .CreateNode(
                    headers,
                    TCreateNodeArgs::Directory(RootNodeId, "subdir"))
                ->Record.GetNode()
                .GetId();

        renameNodeResponse = service.RenameNode(
            headers,
            RootNodeId,
            "file2",
            subdirId,
            "file2",
            0);

        // listing should show only subdir with file2 in it

        listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("subdir", listNodesResponse.GetNames(0));

        listNodesResponse =
            service.ListNodes(headers, fsConfig.FsId, subdirId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("file2", listNodesResponse.GetNames(0));

        // create 2 files in the same shard

        ui64 nodeId4 =
            service
                .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file4"))
                ->Record.GetNode()
                .GetId();

        // round robin is used for 2 clusters, so we just skip one shard by
        // creating a file in it
        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file5"));

        ui64 nodeId6 =
            service
                .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file6"))
                ->Record.GetNode()
                .GetId();

        UNIT_ASSERT_VALUES_EQUAL(
            ExtractShardNo(nodeId4),
            ExtractShardNo(nodeId6));

        // now move to the same shard

        renameNodeResponse = service.RenameNode(
            headers,
            RootNodeId,
            "file4",
            RootNodeId,
            "file6",
            0);

        // file4 should not be present in the listing

        service.SendGetNodeAttrRequest(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file4");

        auto getNodeAttrResponse = service.RecvGetNodeAttrResponse();
        UNIT_ASSERT(getNodeAttrResponse);
        UNIT_ASSERT_C(
            FAILED(getNodeAttrResponse->GetStatus()),
            getNodeAttrResponse->GetErrorReason().c_str());
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(ShouldPerformLocksForExternalNodes)
    {
        config.SetMultiTabletForwardingEnabled(true);
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        ui64 nodeId = service
            .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
            ->Record.GetNode()
            .GetId();
        ui64 handle = service.CreateHandle(
            headers,
            fsConfig.FsId,
            nodeId,
            "",
            TCreateHandleArgs::RDWR)->Record.GetHandle();

        service.AcquireLock(headers, fsConfig.FsId, handle, 1, 0, 4_KB);

        auto response = service.AssertAcquireLockFailed(
            headers,
            fsConfig.FsId,
            handle,
            2,
            0,
            4_KB);
        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), E_FS_WOULDBLOCK);

        service.ReleaseLock(headers, fsConfig.FsId, handle, 2, 0, 4_KB);
        service.AcquireLock(headers, fsConfig.FsId, handle, 1, 0, 0);

        response = service.AssertAcquireLockFailed(
            headers,
            fsConfig.FsId,
            handle,
            2,
            0,
            4_KB);
        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), E_FS_WOULDBLOCK);

        service.ReleaseLock(headers, fsConfig.FsId, handle, 1, 0, 0);

        // ShouldTrackSharedLocks

        service.AcquireLock(
            headers,
            fsConfig.FsId,
            handle,
            1,
            0,
            4_KB,
            DefaultPid,
            NProto::E_SHARED);
        service.AcquireLock(
            headers,
            fsConfig.FsId,
            handle,
            2,
            0,
            4_KB,
            DefaultPid,
            NProto::E_SHARED);

        response = service.AssertAcquireLockFailed(
            headers,
            fsConfig.FsId,
            handle,
            1,
            0,
            0);
        UNIT_ASSERT_VALUES_EQUAL(
            response->GetError().GetCode(),
            E_FS_WOULDBLOCK);

        response = service.AssertAcquireLockFailed(
            headers,
            fsConfig.FsId,
            handle,
            1,
            0,
            0);
        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), E_FS_WOULDBLOCK);

        service.AcquireLock(
            headers,
            fsConfig.FsId,
            handle,
            3,
            0,
            4_KB,
            DefaultPid,
            NProto::E_SHARED);

        service.DestroyHandle(headers, fsConfig.FsId, nodeId, handle);

        handle = service.CreateHandle(
            headers,
            fsConfig.FsId,
            nodeId,
            "",
            TCreateHandleArgs::WRNLY)->Record.GetHandle();

        service.AssertTestLockFailed(
            headers,
            fsConfig.FsId,
            handle,
            1,
            0,
            4_KB,
            DefaultPid,
            NProto::E_SHARED);
        service.AssertAcquireLockFailed(
            headers,
            fsConfig.FsId,
            handle,
            1,
            0,
            4_KB,
            DefaultPid,
            NProto::E_SHARED);
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(ShouldLinkExternalNodes)
    {
        config.SetMultiTabletForwardingEnabled(true);
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        auto headers1 = headers;
        headers1.FileSystemId = fsConfig.Shard1Id;

        const auto nodeId1 =
            service
                .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file1"))
                ->Record.GetNode()
                .GetId();
        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(nodeId1));

        auto node = service.GetNodeAttr(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file1")->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(1, node.GetLinks());

        auto linkNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::Link(RootNodeId, "file2", nodeId1));
        UNIT_ASSERT(linkNodeResponse);
        UNIT_ASSERT_C(
            SUCCEEDED(linkNodeResponse->GetStatus()),
            linkNodeResponse->GetErrorReason().c_str());

        // attributes of the linked node should be updated
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            linkNodeResponse->Record.GetNode().GetLinks());
        UNIT_ASSERT_VALUES_UNEQUAL(
            0,
            linkNodeResponse->Record.GetNode().GetCTime());

        // validate that the links field is incremented
        auto links = service.GetNodeAttr(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file1")->Record.GetNode().GetLinks();
        UNIT_ASSERT_VALUES_EQUAL(2, links);

        // get node attr should also work with the link
        links = service.GetNodeAttr(headers, fsConfig.FsId, RootNodeId, "file1")
            ->Record.GetNode().GetLinks();
        UNIT_ASSERT_VALUES_EQUAL(2, links);

        // validate that reading from hardlinked file works
        auto data = GenerateValidateData(256_KB);
        ui64 handle = service.CreateHandle(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::RDWR)->Record.GetHandle();
        service.WriteData(headers, fsConfig.FsId, nodeId1, handle, 0, data);

        ui64 handle2 = service.CreateHandle(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file2",
            TCreateHandleArgs::RDWR)->Record.GetHandle();

        auto data2 = service.ReadData(
            headers,
            fsConfig.FsId,
            linkNodeResponse->Record.GetNode().GetId(),
            handle2,
            0,
            data.size())->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL(data, data2);

        // Removal of both the file and a hardlink should remove file from both
        // the shard and a leader

        service.DestroyHandle(headers, fsConfig.FsId, nodeId1, handle);
        service.DestroyHandle(
            headers,
            fsConfig.FsId,
            linkNodeResponse->Record.GetNode().GetId(),
            handle2);

        service.UnlinkNode(headers, RootNodeId, "file1");
        service.UnlinkNode(headers, RootNodeId, "file2");

        // Now listing of the root should show no files

        auto listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            RootNodeId)->Record;
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());

        listNodesResponse = service.ListNodes(
            headers1,
            fsConfig.Shard1Id,
            RootNodeId)->Record;
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());

        // GetNodeAttr should fail as well

        service.AssertGetNodeAttrFailed(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file1");
        service.AssertGetNodeAttrFailed(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file2");

        // Creating hardlinks to non-existing files should fail. It is
        // reasonable to assume that nodeId + 100 is not a valid node id.
        linkNodeResponse = service.AssertCreateNodeFailed(
            headers,
            TCreateNodeArgs::Link(RootNodeId, "file3", nodeId1 + 100));
    }

    SERVICE_TEST_SIMPLE(ShouldAggregateFileSystemMetrics)
    {
        config.SetMultiTabletForwardingEnabled(true);
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        // creating 2 files

        auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"))->Record;

        const auto nodeId1 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(nodeId1));

        createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file2"))->Record;

        const auto nodeId2 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(2, ExtractShardNo(nodeId2));

        ui64 handle1 = service.CreateHandle(
            headers,
            fsConfig.FsId,
            nodeId1,
            "",
            TCreateHandleArgs::RDWR)->Record.GetHandle();

        auto data1 = GenerateValidateData(256_KB);
        service.WriteData(headers, fsConfig.FsId, nodeId1, handle1, 0, data1);

        ui64 handle2 = service.CreateHandle(
            headers,
            fsConfig.FsId,
            nodeId2,
            "",
            TCreateHandleArgs::RDWR)->Record.GetHandle();

        auto data2 = GenerateValidateData(512_KB);
        service.WriteData(headers, fsConfig.FsId, nodeId2, handle2, 0, data2);

        // triggering stats collection from shards
        const auto fsStat =
            service.StatFileStore(headers, fsConfig.FsId)->Record;
        const auto& fileStore = fsStat.GetFileStore();
        UNIT_ASSERT_VALUES_EQUAL(fsConfig.FsId, fileStore.GetFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL(2'000, fileStore.GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(4_KB, fileStore.GetBlockSize());

        const auto& fileStoreStats = fsStat.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(2, fileStoreStats.GetUsedNodesCount());
        UNIT_ASSERT_VALUES_EQUAL(
            768_KB / 4_KB,
            fileStoreStats.GetUsedBlocksCount());

        {
            const auto response =
                GetStorageStats(service, fsConfig.FsId, /*allowCache=*/true);
            const auto& stats = response.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                (data1.size() + data2.size()) / 4_KB,
                stats.GetUsedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                (data1.size() + data2.size()) / 4_KB,
                stats.GetMixedBlocksCount());
        }

        service.WriteData(headers, fsConfig.FsId, nodeId2, handle2, 0, data2);

        // triggering stats collection from shards
        service.StatFileStore(headers, fsConfig.FsId);

        {
            const auto response =
                GetStorageStats(service, fsConfig.FsId, /*allowCache=*/true);
            const auto& stats = response.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                (data1.size() + data2.size()) / 4_KB,
                stats.GetUsedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                (data1.size() + 2 * data2.size()) / 4_KB,
                stats.GetMixedBlocksCount());
        }
    }

    SERVICE_TEST_SIMPLE(
        ShouldGetMultishardedSystemTopologyWithStrictFileSystemSizeEnforcement)
    {
        config.SetMultiTabletForwardingEnabled(true);
        config.SetStrictFileSystemSizeEnforcementEnabled(true);
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        // create 2 files

        auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"))->Record;

        const auto nodeId1 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(nodeId1));

        createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file2"))->Record;

        const auto nodeId2 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(2, ExtractShardNo(nodeId2));

        ui64 handle1 = service.CreateHandle(
            headers,
            fsConfig.FsId,
            nodeId1,
            "",
            TCreateHandleArgs::RDWR)->Record.GetHandle();

        auto data1 = GenerateValidateData(256_KB);
        service.WriteData(headers, fsConfig.FsId, nodeId1, handle1, 0, data1);

        ui64 handle2 = service.CreateHandle(
            headers,
            fsConfig.FsId,
            nodeId2,
            "",
            TCreateHandleArgs::RDWR)->Record.GetHandle();

        auto data2 = GenerateValidateData(512_KB);
        service.WriteData(headers, fsConfig.FsId, nodeId2, handle2, 0, data2);

        auto testFileSystemTopology =
            [&](const ui32 shardNo, const TString& shardId)
        {
            const auto response = GetFileSystemTopology(service, shardId);
            const auto& shardIds = response.GetShardFileSystemIds();
            UNIT_ASSERT_EQUAL(shardNo, response.GetShardNo());
            UNIT_ASSERT_EQUAL(2, shardIds.size());
            UNIT_ASSERT_EQUAL(fsConfig.Shard1Id, shardIds[0]);
            UNIT_ASSERT_EQUAL(fsConfig.Shard2Id, shardIds[1]);
            UNIT_ASSERT(response.GetStrictFileSystemSizeEnforcementEnabled());
        };

        testFileSystemTopology(0, fsConfig.FsId);
        testFileSystemTopology(1, fsConfig.Shard1Id);
        testFileSystemTopology(2, fsConfig.Shard2Id);
    }

    SERVICE_TEST_SIMPLE(ShouldGetStorageStatsInDifferentModes)
    {
        config.SetMultiTabletForwardingEnabled(true);
        config.SetStrictFileSystemSizeEnforcementEnabled(true);
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        // create 2 files

        auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"))->Record;

        const auto nodeId1 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(nodeId1));

        createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file2"))->Record;

        const auto nodeId2 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(2, ExtractShardNo(nodeId2));

        ui64 handle1 = service.CreateHandle(
            headers,
            fsConfig.FsId,
            nodeId1,
            "",
            TCreateHandleArgs::RDWR)->Record.GetHandle();

        auto data1 = GenerateValidateData(256_KB);
        service.WriteData(headers, fsConfig.FsId, nodeId1, handle1, 0, data1);

        ui64 handle2 = service.CreateHandle(
            headers,
            fsConfig.FsId,
            nodeId2,
            "",
            TCreateHandleArgs::RDWR)->Record.GetHandle();

        auto data2 = GenerateValidateData(512_KB);
        service.WriteData(headers, fsConfig.FsId, nodeId2, handle2, 0, data2);

        {
            const auto response = GetStorageStats(service, fsConfig.FsId);
            const auto& stats = response.GetStats();
            // In default mode a request to the main filesystem fetches
            // statistics form shards
            UNIT_ASSERT_VALUES_EQUAL(
                (data1.size() + data2.size()) / 4_KB,
                stats.GetUsedBlocksCount());
        }

        {
            const auto response = GetStorageStats(
                service,
                fsConfig.FsId,
                /*allowCache=*/false,
                NProtoPrivate::STATS_REQUEST_MODE_GET_ONLY_SELF);
            const auto& stats = response.GetStats();
            // No blocks are used by the main filesystem itself, all blocks are
            // used by shards.
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUsedBlocksCount());
        }

        // Shards in default mode report only their own statistics.
        {
            const auto response = GetStorageStats(service, fsConfig.Shard1Id);
            const auto& stats = response.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                data1.size() / 4_KB,
                stats.GetUsedBlocksCount());
        }

        {
            const auto response = GetStorageStats(
                service,
                fsConfig.Shard2Id,
                /*allowCache=*/false);
            const auto& stats = response.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                data2.size() / 4_KB,
                stats.GetUsedBlocksCount());
        }

        // Shards in FORCE_FETCH_SHARDS mode fetch statistics from all the
        // shards.
        {
            const auto response = GetStorageStats(
                service,
                fsConfig.Shard1Id,
                /*allowCache=*/false,
                NProtoPrivate::STATS_REQUEST_MODE_FORCE_FETCH_SHARDS);
            const auto& stats = response.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                (data1.size() + data2.size()) / 4_KB,
                stats.GetUsedBlocksCount());
        }

        {
            const auto response = GetStorageStats(
                service,
                fsConfig.Shard2Id,
                /*allowCache=*/false,
                NProtoPrivate::STATS_REQUEST_MODE_FORCE_FETCH_SHARDS);
            const auto& stats = response.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                (data1.size() + data2.size()) / 4_KB,
                stats.GetUsedBlocksCount());
        }
    }

    SERVICE_TEST_SIMPLE(ShouldEnableStrictFileSystemSizeEnforcement)
    {
        // Create file system with two shards 1000 * 4 * 1024 bytes each
        config.SetMultiTabletForwardingEnabled(true);
        config.SetStrictFileSystemSizeEnforcementEnabled(false);
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();
        auto headers = service.InitSession(fsConfig.FsId, "client");

        const ui64 blockSize = 4_KB;
        const ui64 fsSize =
            fsConfig.ShardIds().size() * fsConfig.ShardBlockCount * blockSize;
        const ui64 filesCount = 8;
        const ui64 fileSize = fsSize / (filesCount * 2);

        auto createFile = [&](const TString& fileName) -> auto
        {
            auto createNodeResponse =
                service
                    .CreateNode(
                        headers,
                        TCreateNodeArgs::File(RootNodeId, fileName))
                    ->Record;
            const auto nodeId = createNodeResponse.GetNode().GetId();
            const ui64 handle = service
                                    .CreateHandle(
                                        headers,
                                        fsConfig.FsId,
                                        nodeId,
                                        "",
                                        TCreateHandleArgs::RDWR)
                                    ->Record.GetHandle();

            return std::pair<ui64, ui64>(nodeId, handle);
        };

        // Write some data to some files
        for (ui64 i = 0; i < filesCount; ++i) {
            const auto file = createFile(TStringBuilder() << "file" << i);
            service.WriteData(
                headers,
                fsConfig.FsId,
                file.first,
                file.second,
                0,
                GenerateValidateData(fileSize, i));
        }

        {
            // check topology before turning strict mode on
            const auto mainFsTopology =
                GetFileSystemTopology(service, fsConfig.FsId);
            UNIT_ASSERT(
                !mainFsTopology.GetStrictFileSystemSizeEnforcementEnabled());
            UNIT_ASSERT_EQUAL(2, mainFsTopology.ShardFileSystemIdsSize());

            const auto shard1Topology =
                GetFileSystemTopology(service, fsConfig.Shard1Id);
            UNIT_ASSERT(
                !shard1Topology.GetStrictFileSystemSizeEnforcementEnabled());
            UNIT_ASSERT_EQUAL(0, shard1Topology.ShardFileSystemIdsSize());

            const auto shard2Topology =
                GetFileSystemTopology(service, fsConfig.Shard2Id);
            UNIT_ASSERT(
                !shard2Topology.GetStrictFileSystemSizeEnforcementEnabled());
            UNIT_ASSERT_EQUAL(0, shard2Topology.ShardFileSystemIdsSize());
        }

        // Resize the filesystem to the same size turinng on
        // StrictFileSystemSizeEnforcementEnabled
        service.ResizeFileStore(
            fsConfig.FsId,
            fsConfig.MainFsBlockCount(),
            /*force=*/false,
            /*shardCount=*/0,
            /*enableStrictSizeMode=*/true);

        {
            // check topology after turning strict mode on
            const auto mainFsTopology =
                GetFileSystemTopology(service, fsConfig.FsId);
            UNIT_ASSERT(
                mainFsTopology.GetStrictFileSystemSizeEnforcementEnabled());
            UNIT_ASSERT_EQUAL(2, mainFsTopology.ShardFileSystemIdsSize());

            const auto shard1Topology =
                GetFileSystemTopology(service, fsConfig.Shard1Id);
            UNIT_ASSERT(
                shard1Topology.GetStrictFileSystemSizeEnforcementEnabled());
            UNIT_ASSERT_EQUAL(2, shard1Topology.ShardFileSystemIdsSize());

            const auto shard2Topology =
                GetFileSystemTopology(service, fsConfig.Shard2Id);
            UNIT_ASSERT(
                shard2Topology.GetStrictFileSystemSizeEnforcementEnabled());
            UNIT_ASSERT_EQUAL(2, shard2Topology.ShardFileSystemIdsSize());
        }

        auto checkShardsSize = [&](const ui64 blocksCount)
        {
            const auto mainStats =
                GetStorageStats(service, fsConfig.FsId, /*allowCache=*/false);
            const auto shard1Stats = GetStorageStats(
                service,
                fsConfig.Shard1Id,
                /*allowCache=*/false,
                NProtoPrivate::STATS_REQUEST_MODE_FORCE_FETCH_SHARDS);
            const auto shard2Stats = GetStorageStats(
                service,
                fsConfig.Shard2Id,
                /*allowCache=*/false,
                NProtoPrivate::STATS_REQUEST_MODE_FORCE_FETCH_SHARDS);
            UNIT_ASSERT_EQUAL(
                blocksCount,
                mainStats.GetStats().GetTotalBlocksCount());
            UNIT_ASSERT_EQUAL(
                blocksCount,
                shard1Stats.GetStats().GetTotalBlocksCount());
            UNIT_ASSERT_EQUAL(
                blocksCount,
                shard2Stats.GetStats().GetTotalBlocksCount());
        };

        // After resizing and turning on StrictFileSystemSizeEnforcement every
        // shard should have the same TotalBytesCount equal to that of the main
        // filesystem
        checkShardsSize(fsConfig.MainFsBlockCount());

        // Downsize the filesystem by force
        const auto newBlocksCount = fsConfig.MainFsBlockCount() / 2;
        service.ResizeFileStore(
            fsConfig.FsId,
            newBlocksCount,
            /*force=*/true);

        checkShardsSize(newBlocksCount);

        // Attempt to write one more file should fail
        const auto file = createFile("notEnoughSpace");
        service.AssertWriteDataFailed(
            headers,
            fsConfig.FsId,
            file.first,
            file.second,
            0,
            GenerateValidateData(fileSize, 0xBADF00D));
    }

    SERVICE_TEST_SIMPLE(ShouldAggregateFileSystemMetricsInBackground)
    {
        config.SetMultiTabletForwardingEnabled(true);
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        // creating 2 files

        auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"))->Record;

        const auto nodeId1 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(nodeId1));

        createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file2"))->Record;

        const auto nodeId2 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(2, ExtractShardNo(nodeId2));

        ui64 handle1 = service.CreateHandle(
            headers,
            fsConfig.FsId,
            nodeId1,
            "",
            TCreateHandleArgs::RDWR)->Record.GetHandle();

        auto data1 = GenerateValidateData(256_KB);
        service.WriteData(headers, fsConfig.FsId, nodeId1, handle1, 0, data1);

        ui64 handle2 = service.CreateHandle(
            headers,
            fsConfig.FsId,
            nodeId2,
            "",
            TCreateHandleArgs::RDWR)->Record.GetHandle();

        auto data2 = GenerateValidateData(512_KB);
        service.WriteData(headers, fsConfig.FsId, nodeId2, handle2, 0, data2);

        // triggering background shard stats collection

        env.GetRuntime().AdvanceCurrentTime(TDuration::Seconds(15));

        {
            using TRequest = TEvIndexTabletPrivate::TEvUpdateCounters;

            env.GetRuntime().Send(
                new IEventHandle(
                    fsInfo.MainTabletActorId, // recipient
                    TActorId(), // sender
                    new TRequest(),
                    0, // flags
                    0),
                0);
        }

        TDispatchOptions options;
        options.FinalEvents = {
            TDispatchOptions::TFinalEventCondition(
                TEvIndexTabletPrivate::EvAggregateStatsCompleted)};
        service.AccessRuntime().DispatchEvents(options);

        {
            const auto response =
                GetStorageStats(service, fsConfig.FsId, /*allowCache=*/true);
            const auto& stats = response.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                (data1.size() + data2.size()) / 4_KB,
                stats.GetUsedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                (data1.size() + data2.size()) / 4_KB,
                stats.GetMixedBlocksCount());
        }

        // writing some more data to change stats

        service.WriteData(headers, fsConfig.FsId, nodeId2, handle2, 0, data2);

        // configuring one time request drop to break a single iteration of
        // background stats collection

        bool dropped = false;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvGetStorageStatsRequest: {
                        if (!dropped
                                && event->Recipient != fsInfo.MainTabletActorId
                                && event->Recipient
                                    != MakeIndexTabletProxyServiceId())
                        {
                            dropped = true;
                            return true;
                        }
                        break;
                    }
                }

                return false;
            });

        // triggering background stats collection

        env.GetRuntime().AdvanceCurrentTime(TDuration::Seconds(15));

        {
            using TRequest = TEvIndexTabletPrivate::TEvUpdateCounters;

            env.GetRuntime().Send(
                new IEventHandle(
                    fsInfo.MainTabletActorId, // recipient
                    TActorId(), // sender
                    new TRequest(),
                    0, // flags
                    0),
                0);
        }

        options.FinalEvents = {
            TDispatchOptions::TFinalEventCondition(
                TEvIndexTablet::EvGetStorageStatsRequest)};
        service.AccessRuntime().DispatchEvents(options);

        const auto counters =
            env.GetCounters()->FindSubgroup("component", "service");
        UNIT_ASSERT(counters);
        const auto counter = counters->GetCounter(
            "AppCriticalEvents/ShardStatsRetrievalTimeout");
        UNIT_ASSERT_EQUAL(0, counter->GetAtomic());

        env.GetRuntime().AdvanceCurrentTime(TDuration::Minutes(15));

        // stats not updated yet

        {
            const auto response =
                GetStorageStats(service, fsConfig.FsId, /*allowCache=*/true);
            const auto& stats = response.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                (data1.size() + data2.size()) / 4_KB,
                stats.GetUsedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                (data1.size() + data2.size()) / 4_KB,
                stats.GetMixedBlocksCount());
        }

        // triggering background stats collection once more - main tablet
        // should notice that previous background request timed out and
        // should send the request again

        {
            using TRequest = TEvIndexTabletPrivate::TEvUpdateCounters;

            env.GetRuntime().Send(
                new IEventHandle(
                    fsInfo.MainTabletActorId, // recipient
                    TActorId(), // sender
                    new TRequest(),
                    0, // flags
                    0),
                0);
        }

        options.FinalEvents = {
            TDispatchOptions::TFinalEventCondition(
                TEvIndexTabletPrivate::EvAggregateStatsCompleted)};
        service.AccessRuntime().DispatchEvents(options);

        // crit event should be raised

        UNIT_ASSERT_EQUAL(1, counter->GetAtomic());

        // stats should be up to date now

        {
            const auto response =
                GetStorageStats(service, fsConfig.FsId, /*allowCache=*/true);
            const auto& stats = response.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(
                (data1.size() + data2.size()) / 4_KB,
                stats.GetUsedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                (data1.size() + 2 * data2.size()) / 4_KB,
                stats.GetMixedBlocksCount());
        }
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(ShouldRetryUnlinkingInShard)
    {
        config.SetMultiTabletForwardingEnabled(true);
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        const auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"))->Record;

        const auto nodeId1 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        TAutoPtr<IEventHandle> shardUnlinkResponse;
        bool intercept = true;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvUnlinkNodeRequest) {
                    const auto* msg =
                        event->Get<TEvService::TEvUnlinkNodeRequest>();
                    if (intercept && msg->Record.GetFileSystemId()
                            == fsConfig.Shard1Id)
                    {
                        auto response = std::make_unique<
                            TEvService::TEvUnlinkNodeResponse>(
                            MakeError(E_REJECTED, "error"));

                        shardUnlinkResponse = new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie);

                        return true;
                    }
                }
                return false;
            });

        const ui64 requestId = 111;
        service.SendUnlinkNodeRequest(
            headers,
            RootNodeId,
            "file1",
            false, // unlinkDirectory
            requestId);

        ui32 iterations = 0;
        while (!shardUnlinkResponse && iterations++ < 100) {
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT(shardUnlinkResponse);
        intercept = false;
        env.GetRuntime().Send(shardUnlinkResponse.Release());

        auto unlinkResponse = service.RecvUnlinkNodeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            unlinkResponse->GetError().GetCode(),
            unlinkResponse->GetError().GetMessage());

        auto listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());

        auto headers1 = headers;
        headers1.FileSystemId = fsConfig.Shard1Id;

        listNodesResponse = service.ListNodes(
            headers1,
            fsConfig.Shard1Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());

        // checking DupCache logic - just in case
        service.SendUnlinkNodeRequest(
            headers,
            RootNodeId,
            "file1",
            false, // unlinkDirectory
            requestId);

        unlinkResponse = service.RecvUnlinkNodeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            unlinkResponse->GetError().GetCode(),
            unlinkResponse->GetError().GetMessage());
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(
        ShouldRetryUnlinkingInShardUponLeaderRestart)
    {
        config.SetMultiTabletForwardingEnabled(true);
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        const auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"))->Record;

        const auto nodeId1 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        bool intercept = true;
        bool intercepted = false;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvUnlinkNodeRequest) {
                    const auto* msg =
                        event->Get<TEvService::TEvUnlinkNodeRequest>();
                    if (intercept && msg->Record.GetFileSystemId()
                            == fsConfig.Shard1Id)
                    {
                        intercepted = true;
                        return true;
                    }
                }
                return false;
            });

        service.SendUnlinkNodeRequest(headers, RootNodeId, "file1");

        ui32 iterations = 0;
        while (!intercepted && iterations++ < 100) {
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT(intercepted);
        intercept = false;

        auto headers1 = headers;
        headers1.FileSystemId = fsConfig.Shard1Id;

        auto listNodesResponse = service.ListNodes(
            headers1,
            fsConfig.Shard1Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            fsInfo.MainTabletId);
        tablet.RebootTablet();

        auto unlinkResponse = service.RecvUnlinkNodeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            unlinkResponse->GetError().GetCode(),
            unlinkResponse->GetError().GetMessage());

        // remaking session since CreateSessionActor doesn't do it by itself
        // because EvWakeup never arrives because Scheduling doesn't work by
        // default and RegistrationObservers get reset after RebootTablet
        headers = service.InitSession(fsConfig.FsId, "client");

        listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());

        listNodesResponse = service.ListNodes(
            headers1,
            fsConfig.Shard1Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(
        ShouldRetryUnlinkingInShardUponLeaderRestartForRenameNode)
    {
        config.SetMultiTabletForwardingEnabled(true);
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        const auto createNodeResponse1 = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"))->Record;

        const auto nodeId1 = createNodeResponse1.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        const auto createNodeResponse2 = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file2"))->Record;

        const auto nodeId2 = createNodeResponse2.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL((2LU << 56U) + 2, nodeId2);

        bool intercept = true;
        bool intercepted = false;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvUnlinkNodeRequest) {
                    const auto* msg =
                        event->Get<TEvService::TEvUnlinkNodeRequest>();
                    if (intercept && msg->Record.GetFileSystemId()
                            == fsConfig.Shard2Id)
                    {
                        intercepted = true;
                        return true;
                    }
                }
                return false;
            });

        service.SendRenameNodeRequest(
            headers,
            RootNodeId,
            "file1",
            RootNodeId,
            "file2",
            0);

        ui32 iterations = 0;
        while (!intercepted && iterations++ < 100) {
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT(intercepted);
        intercept = false;

        auto headers2 = headers;
        headers2.FileSystemId = fsConfig.Shard2Id;

        auto listNodesResponse = service.ListNodes(
            headers2,
            fsConfig.Shard2Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            fsInfo.MainTabletId);
        tablet.RebootTablet();

        auto renameResponse = service.RecvRenameNodeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            renameResponse->GetError().GetCode(),
            renameResponse->GetError().GetMessage());

        // remaking session since CreateSessionActor doesn't do it by itself
        // because EvWakeup never arrives because Scheduling doesn't work by
        // default and RegistrationObservers get reset after RebootTablet
        headers = service.InitSession(fsConfig.FsId, "client");

        listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL("file2", listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            listNodesResponse.GetNodes(0).GetId());

        listNodesResponse = service.ListNodes(
            headers2,
            fsConfig.Shard2Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(ShouldRetryNodeCreationInShard)
    {
        config.SetMultiTabletForwardingEnabled(true);
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        TAutoPtr<IEventHandle> shardCreateResponse;
        bool intercept = true;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvCreateNodeRequest) {
                    const auto* msg =
                        event->Get<TEvService::TEvCreateNodeRequest>();
                    if (intercept && msg->Record.GetFileSystemId()
                            == fsConfig.Shard1Id)
                    {
                        auto response = std::make_unique<
                            TEvService::TEvCreateNodeResponse>(
                            MakeError(E_REJECTED, "error"));

                        shardCreateResponse = new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie);

                        return true;
                    }
                }
                return false;
            });

        const ui64 requestId = 111;
        service.SendCreateNodeRequest(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"),
            requestId);

        ui32 iterations = 0;
        while (!shardCreateResponse && iterations++ < 100) {
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT(shardCreateResponse);
        intercept = false;
        env.GetRuntime().Send(shardCreateResponse.Release());

        auto createResponse = service.RecvCreateNodeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            createResponse->GetError().GetCode(),
            createResponse->GetError().GetMessage());

        const auto nodeId1 = createResponse->Record.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        auto listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL("file1", listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            listNodesResponse.GetNodes(0).GetId());

        // checking DupCache logic
        service.SendCreateNodeRequest(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"),
            requestId);

        createResponse = service.RecvCreateNodeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            createResponse->GetError().GetCode(),
            createResponse->GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            createResponse->Record.GetNode().GetId());
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(
        ShouldRetryNodeCreationInShardUponLeaderRestart)
    {
        config.SetMultiTabletForwardingEnabled(true);
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        bool intercept = true;
        bool intercepted = false;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvCreateNodeRequest) {
                    const auto* msg =
                        event->Get<TEvService::TEvCreateNodeRequest>();
                    if (intercept && msg->Record.GetFileSystemId()
                            == fsConfig.Shard1Id)
                    {
                        intercepted = true;
                        return true;
                    }
                }
                return false;
            });

        const ui64 requestId = 111;
        service.SendCreateNodeRequest(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"),
            requestId);

        ui32 iterations = 0;
        while (!intercepted && iterations++ < 100) {
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT(intercepted);
        intercept = false;

        // TODO listNodes in leader?

        auto headers1 = headers;
        headers1.FileSystemId = fsConfig.Shard1Id;

        auto listNodesResponse = service.ListNodes(
            headers1,
            fsConfig.Shard1Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            fsInfo.MainTabletId);
        tablet.RebootTablet();

        auto createResponse = service.RecvCreateNodeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            createResponse->GetError().GetCode(),
            createResponse->GetError().GetMessage());

        // remaking session since CreateSessionActor doesn't do it by itself
        // because EvWakeup never arrives because Scheduling doesn't work by
        // default and RegistrationObservers get reset after RebootTablet
        // restoreClientSession = true
        headers = service.InitSession(fsConfig.FsId, "client", {}, true);

        listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            RootNodeId)->Record;

        const ui64 nodeId1 = (1LU << 56U) + 2;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL("file1", listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            listNodesResponse.GetNodes(0).GetId());

        listNodesResponse = service.ListNodes(
            headers1,
            fsConfig.Shard1Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());

        // checking DupCache logic
        service.SendCreateNodeRequest(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"),
            requestId);

        createResponse = service.RecvCreateNodeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            createResponse->GetError().GetCode(),
            createResponse->GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            createResponse->Record.GetNode().GetId());
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(
        ShouldRetryNodeCreationInShardUponCreateHandle)
    {
        config.SetMultiTabletForwardingEnabled(true);
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        TAutoPtr<IEventHandle> shardCreateResponse;
        bool intercept = true;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvCreateNodeRequest) {
                    const auto* msg =
                        event->Get<TEvService::TEvCreateNodeRequest>();
                    if (intercept && msg->Record.GetFileSystemId()
                            == fsConfig.Shard1Id)
                    {
                        auto response = std::make_unique<
                            TEvService::TEvCreateNodeResponse>(
                            MakeError(E_REJECTED, "error"));

                        shardCreateResponse = new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie);

                        return true;
                    }
                }
                return false;
            });

        const ui64 requestId = 111;
        service.SendCreateHandleRequest(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::CREATE,
            "", // shardId
            requestId);

        ui32 iterations = 0;
        while (!shardCreateResponse && iterations++ < 100) {
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT(shardCreateResponse);
        intercept = false;
        env.GetRuntime().Send(shardCreateResponse.Release());

        auto createHandleResponse = service.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            createHandleResponse->GetError().GetCode(),
            createHandleResponse->GetError().GetMessage());

        const auto nodeId1 = createHandleResponse->Record.GetNodeAttr().GetId();
        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        auto listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL("file1", listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            listNodesResponse.GetNodes(0).GetId());

        // checking DupCache logic
        service.SendCreateHandleRequest(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::CREATE,
            "", // shardId
            requestId);

        createHandleResponse = service.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            createHandleResponse->GetError().GetCode(),
            createHandleResponse->GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            createHandleResponse->Record.GetNodeAttr().GetId());
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER(
        ShouldRetryNodeCreationInShardUponCreateHandleUponLeaderRestart)
    {
        config.SetMultiTabletForwardingEnabled(true);
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        bool intercept = true;
        bool intercepted = false;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvCreateNodeRequest) {
                    const auto* msg =
                        event->Get<TEvService::TEvCreateNodeRequest>();
                    if (intercept && msg->Record.GetFileSystemId()
                            == fsConfig.Shard1Id)
                    {
                        intercepted = true;
                        return true;
                    }
                }
                return false;
            });

        const ui64 requestId = 111;
        service.SendCreateHandleRequest(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::CREATE,
            "", // shardId
            requestId);

        ui32 iterations = 0;
        while (!intercepted && iterations++ < 100) {
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT(intercepted);
        intercept = false;

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            fsInfo.MainTabletId);
        tablet.RebootTablet();

        auto createHandleResponse = service.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            createHandleResponse->GetError().GetCode(),
            createHandleResponse->GetError().GetMessage());

        const auto nodeId1 = (1LU << 56U) + 2;

        // remaking session since CreateSessionActor doesn't do it by itself
        // because EvWakeup never arrives because Scheduling doesn't work by
        // default and RegistrationObservers get reset after RebootTablet
        // restoreClientSession = true
        headers = service.InitSession(fsConfig.FsId, "client", {}, true);

        auto listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL("file1", listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            listNodesResponse.GetNodes(0).GetId());

        // checking DupCache logic
        service.SendCreateHandleRequest(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::CREATE,
            "", // shardId
            requestId);

        createHandleResponse = service.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            createHandleResponse->GetError().GetCode(),
            createHandleResponse->GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            createHandleResponse->Record.GetNodeAttr().GetId());
    }

    void DoTestShardedFileSystemConfigured(
        const TString& fsId,
        TServiceClient& service,
        TVector<TString> fsList)
    {
        auto listing = service.ListFileStores();
        const auto& fsIds = listing->Record.GetFileStores();
        TVector<TString> ids(fsIds.begin(), fsIds.end());
        Sort(ids);

        UNIT_ASSERT_VALUES_EQUAL(fsList, ids);

        auto headers = service.InitSession(fsId, "client");

        for (const auto& id: ids) {
            NProtoPrivate::TDescribeSessionsRequest request;
            request.SetFileSystemId(id);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("describesessions", buf);
            NProtoPrivate::TDescribeSessionsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());

            const auto& sessions = response.GetSessions();
            UNIT_ASSERT_VALUES_EQUAL(1, sessions.size());

            UNIT_ASSERT_VALUES_EQUAL(
                headers.SessionId,
                sessions[0].GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(
                headers.ClientId,
                sessions[0].GetClientId());
            UNIT_ASSERT_VALUES_EQUAL("", sessions[0].GetSessionState());
        }

        const TString sessionState = "some_state";
        service.ResetSession(headers, sessionState);

        for (const auto& id: ids) {
            NProtoPrivate::TDescribeSessionsRequest request;
            request.SetFileSystemId(id);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("describesessions", buf);
            NProtoPrivate::TDescribeSessionsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());

            const auto& sessions = response.GetSessions();
            UNIT_ASSERT_VALUES_EQUAL(1, sessions.size());

            UNIT_ASSERT_VALUES_EQUAL(
                headers.SessionId,
                sessions[0].GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(
                headers.ClientId,
                sessions[0].GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(
                sessionState,
                sessions[0].GetSessionState());
        }

        service.DestroySession(headers);

        for (const auto& id: ids) {
            NProtoPrivate::TDescribeSessionsRequest request;
            request.SetFileSystemId(id);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("describesessions", buf);
            NProtoPrivate::TDescribeSessionsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());

            const auto& sessions = response.GetSessions();
            UNIT_ASSERT_VALUES_EQUAL(0, sessions.size());
        }
    }

    SERVICE_TEST_SIMPLE(ShouldConfigureShardsAutomatically)
    {
        config.SetAutomaticShardCreationEnabled(true);
        config.SetShardAllocationUnit(1_GB);
        config.SetAutomaticallyCreatedShardSize(2_GB);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 2_GB / 4_KB);

        // waiting for IndexTablet start after the restart triggered by
        // configureshards
        WaitForTabletStart(service);

        DoTestShardedFileSystemConfigured(
            fsId,
            service,
            {fsId, fsId + "_s1", fsId + "_s2"});
    }

    SERVICE_TEST_SIMPLE(
        ShouldNotConfigureShardsAutomaticallyForSmallFileSystems)
    {
        config.SetAutomaticShardCreationEnabled(true);
        config.SetShardAllocationUnit(1_GB);
        config.SetAutomaticallyCreatedShardSize(2_GB);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, (1_GB - 4_KB) / 4_KB);

        DoTestShardedFileSystemConfigured(fsId, service, {fsId});
    }

    SERVICE_TEST_SIMPLE(ShouldHandleErrorsDuringShardedFileSystemCreation)
    {
        config.SetAutomaticShardCreationEnabled(true);
        config.SetShardAllocationUnit(1_GB);
        config.SetAutomaticallyCreatedShardSize(2_GB);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto blockCount = 2_GB / 4_KB;

        TServiceClient service(env.GetRuntime(), nodeIdx);

        TVector<TString> expected = {fsId, fsId + "_s1", fsId + "_s2"};

        NProto::TError createShardError;
        NProto::TError configureShardError;
        NProto::TError configureShardsError;

        TAutoPtr<IEventHandle> toSend;

        env.GetRuntime().SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvCreateFileStoreRequest: {
                        using TRequest = TEvSSProxy::TEvCreateFileStoreRequest;
                        using TResponse =
                            TEvSSProxy::TEvCreateFileStoreResponse;
                        const auto* msg = event->Get<TRequest>();
                        if (msg->Config.GetFileSystemId() != expected[1]) {
                            break;
                        }

                        if (!HasError(createShardError)) {
                            break;
                        }

                        auto response = std::make_unique<TResponse>(
                            createShardError);

                        toSend = new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie);

                        return true;
                    }

                    case TEvIndexTablet::EvConfigureAsShardRequest: {
                        using TRequest =
                            TEvIndexTablet::TEvConfigureAsShardRequest;
                        using TResponse =
                            TEvIndexTablet::TEvConfigureAsShardResponse;
                        const auto* msg = event->Get<TRequest>();
                        if (msg->Record.GetFileSystemId() != expected[1]) {
                            break;
                        }

                        if (!HasError(configureShardError)) {
                            break;
                        }

                        auto response = std::make_unique<TResponse>(
                            configureShardError);

                        toSend = new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie);

                        return true;
                    }

                    case TEvIndexTablet::EvConfigureShardsRequest: {
                        using TResponse =
                            TEvIndexTablet::TEvConfigureShardsResponse;

                        if (!HasError(configureShardsError)) {
                            break;
                        }

                        auto response = std::make_unique<TResponse>(
                            configureShardsError);

                        toSend = new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie);

                        return true;
                    }
                }

                return false;
            });

        createShardError = MakeError(E_REJECTED, "failed to create shard");
        service.SendCreateFileStoreRequest(fsId, blockCount);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT(toSend);
        env.GetRuntime().Send(toSend, nodeIdx);
        {
            auto response = service.RecvCreateFileStoreResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                FormatError(createShardError),
                FormatError(response->GetError()));
        }

        createShardError = {};
        configureShardError =
            MakeError(E_REJECTED, "failed to configure shard");
        service.SendCreateFileStoreRequest(fsId, blockCount);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT(toSend);
        env.GetRuntime().Send(toSend, nodeIdx);
        {
            auto response = service.RecvCreateFileStoreResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                FormatError(configureShardError),
                FormatError(response->GetError()));
        }

        configureShardError = {};
        configureShardsError =
            MakeError(E_REJECTED, "failed to configure shards");
        service.SendCreateFileStoreRequest(fsId, blockCount);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT(toSend);
        env.GetRuntime().Send(toSend, nodeIdx);
        {
            auto response = service.RecvCreateFileStoreResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                FormatError(configureShardsError),
                FormatError(response->GetError()));
        }

        configureShardsError = {};
        service.SendCreateFileStoreRequest(fsId, blockCount);
        {
            auto response = service.RecvCreateFileStoreResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                FormatError(MakeError(S_OK)),
                FormatError(response->GetError()));
        }

        // waiting for IndexTablet start after the restart triggered by
        // configureshards
        WaitForTabletStart(service);

        DoTestShardedFileSystemConfigured(
            fsId,
            service,
            {fsId, fsId + "_s1", fsId + "_s2"});
    }

    SERVICE_TEST_SIMPLE(ShouldDeleteShardsAutomatically)
    {
        config.SetAutomaticShardCreationEnabled(true);
        config.SetShardAllocationUnit(1_GB);
        config.SetAutomaticallyCreatedShardSize(2_GB);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId1 = "test1";
        const TString fsId2 = "test2";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId1, 2_GB / 4_KB);
        service.CreateFileStore(fsId2, 4_GB / 4_KB);

        TVector<TString> expected = {
            fsId1, fsId1 + "_s1", fsId1 + "_s2",
            fsId2, fsId2 + "_s1", fsId2 + "_s2", fsId2 + "_s3", fsId2 + "_s4",
        };

        auto listing = service.ListFileStores();
        auto fsIds = listing->Record.GetFileStores();
        TVector<TString> ids(fsIds.begin(), fsIds.end());
        Sort(ids);
        UNIT_ASSERT_VALUES_EQUAL(expected, ids);

        service.DestroyFileStore(fsId1);

        expected = {
            fsId2, fsId2 + "_s1", fsId2 + "_s2", fsId2 + "_s3", fsId2 + "_s4",
        };

        listing = service.ListFileStores();
        fsIds = listing->Record.GetFileStores();
        ids = TVector<TString>(fsIds.begin(), fsIds.end());
        Sort(ids);
        UNIT_ASSERT_VALUES_EQUAL(expected, ids);

        service.DestroyFileStore(fsId2);

        expected = {};

        listing = service.ListFileStores();
        fsIds = listing->Record.GetFileStores();
        ids = TVector<TString>(fsIds.begin(), fsIds.end());
        Sort(ids);
        UNIT_ASSERT_VALUES_EQUAL(expected, ids);
    }

    SERVICE_TEST_SIMPLE(ShouldHandleErrorsDuringShardedFileSystemDestruction)
    {
        config.SetAutomaticShardCreationEnabled(true);
        config.SetShardAllocationUnit(1_GB);
        config.SetAutomaticallyCreatedShardSize(2_GB);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 2_GB / 4_KB);

        // waiting for IndexTablet start after the restart triggered by
        // configureshards
        WaitForTabletStart(service);

        TVector<TString> expected = {fsId, fsId + "_s1", fsId + "_s2"};
        auto listing = service.ListFileStores();
        auto fsIds = listing->Record.GetFileStores();
        TVector<TString> ids(fsIds.begin(), fsIds.end());
        Sort(ids);
        UNIT_ASSERT_VALUES_EQUAL(expected, ids);

        NProto::TError getTopologyError;
        NProto::TError destroyShardError;

        TAutoPtr<IEventHandle> toSend;

        env.GetRuntime().SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvGetFileSystemTopologyRequest: {
                        using TResponse =
                            TEvIndexTablet::TEvGetFileSystemTopologyResponse;

                        if (!HasError(getTopologyError)) {
                            break;
                        }

                        auto response = std::make_unique<TResponse>(
                            getTopologyError);

                        toSend = new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie);

                        return true;
                    }

                    case TEvSSProxy::EvDestroyFileStoreRequest: {
                        using TRequest = TEvSSProxy::TEvDestroyFileStoreRequest;
                        using TResponse =
                            TEvSSProxy::TEvDestroyFileStoreResponse;
                        const auto* msg = event->Get<TRequest>();
                        if (msg->FileSystemId != expected[1]) {
                            break;
                        }

                        if (!HasError(destroyShardError)) {
                            break;
                        }

                        auto response = std::make_unique<TResponse>(
                            destroyShardError);

                        toSend = new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie);

                        return true;
                    }
                }

                return false;
            });

        getTopologyError = MakeError(E_REJECTED, "failed to get topology");
        service.SendDestroyFileStoreRequest(fsId);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT(toSend);
        env.GetRuntime().Send(toSend, nodeIdx);
        {
            auto response = service.RecvDestroyFileStoreResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                FormatError(getTopologyError),
                FormatError(response->GetError()));
        }

        listing = service.ListFileStores();
        fsIds = listing->Record.GetFileStores();
        ids = TVector<TString>(fsIds.begin(), fsIds.end());
        Sort(ids);
        UNIT_ASSERT_VALUES_EQUAL(expected, ids);

        getTopologyError = {};
        destroyShardError = MakeError(E_REJECTED, "failed to destroy shard");
        service.SendDestroyFileStoreRequest(fsId);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT(toSend);
        env.GetRuntime().Send(toSend, nodeIdx);
        {
            auto response = service.RecvDestroyFileStoreResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                FormatError(destroyShardError),
                FormatError(response->GetError()));
        }

        expected = {fsId, fsId + "_s1"};
        listing = service.ListFileStores();
        fsIds = listing->Record.GetFileStores();
        ids = TVector<TString>(fsIds.begin(), fsIds.end());
        Sort(ids);
        UNIT_ASSERT_VALUES_EQUAL(expected, ids);

        destroyShardError = {};
        service.SendDestroyFileStoreRequest(fsId);
        {
            auto response = service.RecvDestroyFileStoreResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                FormatError(MakeError(S_OK)),
                FormatError(response->GetError()));
        }

        expected = {};
        listing = service.ListFileStores();
        fsIds = listing->Record.GetFileStores();
        ids = TVector<TString>(fsIds.begin(), fsIds.end());
        Sort(ids);
        UNIT_ASSERT_VALUES_EQUAL(expected, ids);
    }

    SERVICE_TEST_SIMPLE(ShouldAddShardsAutomaticallyUponResize)
    {
        config.SetAutomaticShardCreationEnabled(true);
        config.SetShardAllocationUnit(1_GB);
        config.SetAutomaticallyCreatedShardSize(2_GB);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1_GB / 4_KB);

        TVector<TString> expected = {fsId, fsId + "_s1"};
        auto listing = service.ListFileStores();
        auto fsIds = listing->Record.GetFileStores();
        TVector<TString> ids(fsIds.begin(), fsIds.end());
        Sort(ids);
        UNIT_ASSERT_VALUES_EQUAL(expected, ids);

        service.ResizeFileStore(fsId, 3_GB / 4_KB);

        expected = TVector<TString>{
            fsId, fsId + "_s1", fsId + "_s2", fsId + "_s3",
        };
        listing = service.ListFileStores();
        fsIds = listing->Record.GetFileStores();
        ids = TVector<TString>(fsIds.begin(), fsIds.end());
        Sort(ids);
        UNIT_ASSERT_VALUES_EQUAL(expected, ids);

        service.ResizeFileStore(fsId, (4_GB - 4_KB) / 4_KB);

        expected = TVector<TString>{
            fsId, fsId + "_s1", fsId + "_s2", fsId + "_s3", fsId + "_s4"
        };
        listing = service.ListFileStores();
        fsIds = listing->Record.GetFileStores();
        ids = TVector<TString>(fsIds.begin(), fsIds.end());
        Sort(ids);
        UNIT_ASSERT_VALUES_EQUAL(expected, ids);

        service.ResizeFileStore(fsId, 4_GB / 4_KB);

        listing = service.ListFileStores();
        fsIds = listing->Record.GetFileStores();
        ids = TVector<TString>(fsIds.begin(), fsIds.end());
        Sort(ids);
        UNIT_ASSERT_VALUES_EQUAL(expected, ids);
    }

    SERVICE_TEST_SIMPLE(ShouldHandleErrorsDuringShardedFileSystemResize)
    {
        config.SetAutomaticShardCreationEnabled(true);
        config.SetShardAllocationUnit(1_GB);
        config.SetAutomaticallyCreatedShardSize(2_GB);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto newBlockCount = 4_GB / 4_KB;

        TServiceClient service(env.GetRuntime(), nodeIdx);

        TVector<TString> expected = {fsId, fsId + "_s1", fsId + "_s2"};

        service.CreateFileStore(fsId, 2_GB / 4_KB);
        auto listing = service.ListFileStores();
        auto fsIds = listing->Record.GetFileStores();
        TVector<TString> ids(fsIds.begin(), fsIds.end());
        Sort(ids);
        UNIT_ASSERT_VALUES_EQUAL(expected, ids);

        expected = {
            fsId, fsId + "_s1", fsId + "_s2", fsId + "_s3", fsId + "_s4",
        };

        NProto::TError getTopologyError;
        NProto::TError createShardError;
        NProto::TError configureShardError;
        NProto::TError configureShardsError;

        TAutoPtr<IEventHandle> toSend;

        env.GetRuntime().SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvGetFileSystemTopologyRequest: {
                        using TResponse =
                            TEvIndexTablet::TEvGetFileSystemTopologyResponse;

                        if (!HasError(getTopologyError)) {
                            break;
                        }

                        auto response = std::make_unique<TResponse>(
                            getTopologyError);

                        toSend = new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie);

                        return true;
                    }

                    case TEvSSProxy::EvCreateFileStoreRequest: {
                        using TRequest = TEvSSProxy::TEvCreateFileStoreRequest;
                        using TResponse =
                            TEvSSProxy::TEvCreateFileStoreResponse;
                        const auto* msg = event->Get<TRequest>();
                        if (msg->Config.GetFileSystemId() != expected[3]) {
                            break;
                        }

                        if (!HasError(createShardError)) {
                            break;
                        }

                        auto response = std::make_unique<TResponse>(
                            createShardError);

                        toSend = new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie);

                        return true;
                    }

                    case TEvIndexTablet::EvConfigureAsShardRequest: {
                        using TRequest =
                            TEvIndexTablet::TEvConfigureAsShardRequest;
                        using TResponse =
                            TEvIndexTablet::TEvConfigureAsShardResponse;
                        const auto* msg = event->Get<TRequest>();
                        if (msg->Record.GetFileSystemId() != expected[3]) {
                            break;
                        }

                        if (!HasError(configureShardError)) {
                            break;
                        }

                        auto response = std::make_unique<TResponse>(
                            configureShardError);

                        toSend = new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie);

                        return true;
                    }

                    case TEvIndexTablet::EvConfigureShardsRequest: {
                        using TResponse =
                            TEvIndexTablet::TEvConfigureShardsResponse;

                        if (!HasError(configureShardsError)) {
                            break;
                        }

                        auto response = std::make_unique<TResponse>(
                            configureShardsError);

                        toSend = new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie);

                        return true;
                    }
                }

                return false;
            });

        getTopologyError = MakeError(E_REJECTED, "failed to get topology");
        service.SendResizeFileStoreRequest(fsId, newBlockCount);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT(toSend);
        env.GetRuntime().Send(toSend, nodeIdx);
        {
            auto response = service.RecvResizeFileStoreResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                FormatError(getTopologyError),
                FormatError(response->GetError()));
        }

        getTopologyError = {};
        createShardError = MakeError(E_REJECTED, "failed to create shard");
        service.SendResizeFileStoreRequest(fsId, newBlockCount);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT(toSend);
        env.GetRuntime().Send(toSend, nodeIdx);
        {
            auto response = service.RecvResizeFileStoreResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                FormatError(createShardError),
                FormatError(response->GetError()));
        }

        createShardError = {};
        configureShardError =
            MakeError(E_REJECTED, "failed to configure shard");
        service.SendResizeFileStoreRequest(fsId, newBlockCount);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT(toSend);
        env.GetRuntime().Send(toSend, nodeIdx);
        {
            auto response = service.RecvResizeFileStoreResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                FormatError(configureShardError),
                FormatError(response->GetError()));
        }

        configureShardError = {};
        configureShardsError =
            MakeError(E_REJECTED, "failed to configure shards");
        service.SendResizeFileStoreRequest(fsId, newBlockCount);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT(toSend);
        env.GetRuntime().Send(toSend, nodeIdx);
        {
            auto response = service.RecvResizeFileStoreResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                FormatError(configureShardsError),
                FormatError(response->GetError()));
        }

        configureShardsError = {};
        service.SendResizeFileStoreRequest(fsId, newBlockCount);
        {
            auto response = service.RecvResizeFileStoreResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                FormatError(MakeError(S_OK)),
                FormatError(response->GetError()));
        }

        // waiting for IndexTablet start after the restart triggered by
        // configureshards
        WaitForTabletStart(service);

        DoTestShardedFileSystemConfigured(fsId, service, expected);
    }

    SERVICE_TEST_SIMPLE(ShouldValidateShardList)
    {
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ui32 shardNo = 0;
        for (const auto& shardId: {shard1Id, shard2Id}) {
            NProtoPrivate::TConfigureAsShardRequest request;
            request.SetFileSystemId(shardId);
            request.SetShardNo(++shardNo);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("configureasshard", buf);
            NProtoPrivate::TConfigureAsShardResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());
        }

        {
            NProtoPrivate::TConfigureShardsRequest request;
            request.SetFileSystemId(fsId);
            *request.AddShardFileSystemIds() = shard1Id;
            *request.AddShardFileSystemIds() = shard2Id;

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("configureshards", buf);
            NProtoPrivate::TConfigureShardsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());
        }

        // waiting for IndexTablet start after the restart triggered by
        // configureshards
        WaitForTabletStart(service);

        {
            auto response = service.CreateSession(THeaders{fsId, "client", ""});
            const auto& fileStore = response->Record.GetFileStore();
            UNIT_ASSERT_VALUES_EQUAL(2, fileStore.ShardFileSystemIdsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                shard1Id,
                fileStore.GetShardFileSystemIds(0));
            UNIT_ASSERT_VALUES_EQUAL(
                shard2Id,
                fileStore.GetShardFileSystemIds(1));
        }

        // shard order change not allowed
        {
            NProtoPrivate::TConfigureShardsRequest request;
            request.SetFileSystemId(fsId);
            *request.AddShardFileSystemIds() = shard2Id;
            *request.AddShardFileSystemIds() = shard1Id;

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("configureshards", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetStatus(),
                response->GetErrorReason());
        }

        // shard list cropping not allowed
        {
            NProtoPrivate::TConfigureShardsRequest request;
            request.SetFileSystemId(fsId);
            *request.AddShardFileSystemIds() = shard1Id;

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("configureshards", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetStatus(),
                response->GetErrorReason());
        }

        // force flag should override checks
        {
            NProtoPrivate::TConfigureShardsRequest request;
            request.SetFileSystemId(fsId);
            request.SetForce(true);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("configureshards", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }

        // waiting for IndexTablet start after the restart triggered by
        // configureshards
        WaitForTabletStart(service);

        {
            auto response = service.CreateSession(THeaders{fsId, "client", ""});
            const auto& fileStore = response->Record.GetFileStore();
            UNIT_ASSERT_VALUES_EQUAL(0, fileStore.ShardFileSystemIdsSize());
        }

        // configureshards for a shard should fail
        {
            NProtoPrivate::TConfigureShardsRequest request;
            request.SetFileSystemId(shard1Id);
            *request.AddShardFileSystemIds() = shard2Id;

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("configureshards", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_INVALID_STATE,
                response->GetStatus(),
                response->GetErrorReason());

            // configureshards for a shard should work with force flag
            request.SetForce(true);

            buf.clear();
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("configureshards", buf);
            response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }
    }

    SERVICE_TEST_SIMPLE(ShouldNotAutomaticallyAddShardsToShards)
    {
        config.SetAutomaticShardCreationEnabled(true);
        config.SetShardAllocationUnit(1_GB);
        config.SetAutomaticallyCreatedShardSize(2_GB);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const TString shard1Id = fsId + "_s1";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1_GB / 4_KB);

        TVector<TString> expected = {fsId, shard1Id};
        auto listing = service.ListFileStores();
        auto fsIds = listing->Record.GetFileStores();
        TVector<TString> ids(fsIds.begin(), fsIds.end());
        Sort(ids);
        UNIT_ASSERT_VALUES_EQUAL(expected, ids);

        service.ResizeFileStore(shard1Id, 4_GB / 4_KB);

        // subshards shouldn't have been created for shard1
        expected = TVector<TString>{fsId, shard1Id};
        listing = service.ListFileStores();
        fsIds = listing->Record.GetFileStores();
        ids = TVector<TString>(fsIds.begin(), fsIds.end());
        Sort(ids);
        UNIT_ASSERT_VALUES_EQUAL(expected, ids);
    }

    void CheckPendingCreateNodeInShards(bool withCreateHandle, bool withTabletReboot)
    {
        NProto::TStorageConfig config;
        TShardedFileSystemConfig fsConfig;
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        bool createNodeObserved = false;
        bool createHandleObserved = false;
        TAutoPtr<IEventHandle> createNodeOnShard;

        auto& runtime = env.GetRuntime();
        runtime.SetEventFilter(
            [&](auto& runtime, TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvCreateHandleRequest:
                        createHandleObserved = true;
                        break;
                    case TEvService::EvCreateNodeRequest: {
                        const auto* msg =
                            event->Get<TEvService::TEvCreateNodeRequest>();
                        if (msg->Record.GetShardFileSystemId().empty()) {
                            createNodeOnShard = event.Release();
                            return true;
                        }
                        createNodeObserved = true;

                        break;
                    }
                }
                return false;
            });

        auto checkListNodes = [&](auto expectedNames) {
            auto listNodesRsp =
                service.ListNodes(headers, fsConfig.FsId, RootNodeId)->Record;

            UNIT_ASSERT_VALUES_EQUAL(expectedNames.size(), listNodesRsp.NamesSize());
            UNIT_ASSERT_VALUES_EQUAL(expectedNames.size(), listNodesRsp.NodesSize());

            const auto& names = listNodesRsp.GetNames();
            TVector<TString> listedNames(names.begin(), names.end());
            Sort(listedNames);
            UNIT_ASSERT_VALUES_EQUAL(expectedNames, listedNames);
        };

        // send create node/handle, delay response in shard and make sure node is not
        // listed
        if (!withCreateHandle) {
            service.SendCreateNodeRequest(
                headers,
                TCreateNodeArgs::File(
                    RootNodeId,
                    "file1",
                    0,   // mode
                    fsConfig.Shard1Id));
        } else {
            service.SendCreateHandleRequest(
                headers,
                fsConfig.FsId,
                RootNodeId,
                "file1",
                TCreateHandleArgs::CREATE,
                fsConfig.Shard1Id);
        }
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        if (withCreateHandle) {
            UNIT_ASSERT(!createNodeObserved);
            UNIT_ASSERT(createHandleObserved);
        } else {
            UNIT_ASSERT(createNodeObserved);
            UNIT_ASSERT(!createHandleObserved);
        }
        UNIT_ASSERT(createNodeOnShard);

        if (withTabletReboot) {
            TIndexTabletClient tablet(
                env.GetRuntime(),
                nodeIdx,
                fsInfo.MainTabletId);
            tablet.RebootTablet();

            headers = service.InitSession(fsConfig.FsId, "client", {}, true);
        }

        checkListNodes(TVector<TString>{});

        // send delayed response in shard and make sure node is listed
        auto createNodeRsp =
            std::make_unique<TEvService::TEvCreateNodeResponse>();
        runtime.Send(
            new IEventHandle(
                createNodeOnShard->Sender,
                createNodeOnShard->Recipient,
                createNodeRsp.release(),
                0,   // flags
                createNodeOnShard->Cookie),
            nodeIdx);
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        checkListNodes(TVector<TString>{"file1"});
    }

    Y_UNIT_TEST(ShouldNotListPendingCreateNodeInShards)
    {
        CheckPendingCreateNodeInShards(
            false,   // don't create handle
            false    // don't reboot tablet
        );
        CheckPendingCreateNodeInShards(
            false,   // don't create handle
            true     // reboot tablet
        );
    }

    Y_UNIT_TEST(ShouldNotListPendingCreateHandleInShards)
    {
        CheckPendingCreateNodeInShards(
            true,    // create handle
            false    // don't reboot tablet
        );
        CheckPendingCreateNodeInShards(
            true,   // create handle
            true    // reboot tablet
        );
    }

    Y_UNIT_TEST(ShouldBalanceShardsByFreeSpace)
    {
        NProto::TStorageConfig config;
        config.SetShardIdSelectionInLeaderEnabled(true);
        config.SetAutomaticShardCreationEnabled(true);
        config.SetShardAllocationUnit(4_MB);
        config.SetAutomaticallyCreatedShardSize(4_MB);
        config.SetShardBalancerMinFreeSpaceReserve(4_KB);
        config.SetShardBalancerDesiredFreeSpaceReserve(1_MB);
        config.SetMultiTabletForwardingEnabled(true);

        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 20_MB / 4_KB);

        // waiting for IndexTablet start after the restart triggered by
        // configureshards
        WaitForTabletStart(service);

        TVector<TString> expected = {
            fsId,
            fsId + "_s1",
            fsId + "_s2",
            fsId + "_s3",
            fsId + "_s4",
            fsId + "_s5",
        };
        auto listing = service.ListFileStores();
        auto fsIds = listing->Record.GetFileStores();
        TVector<TString> ids(fsIds.begin(), fsIds.end());
        Sort(ids);
        UNIT_ASSERT_VALUES_EQUAL(expected, ids);

        auto headers = service.InitSession(fsId, "client");

        // creating 5 files - we should have round-robin balancing at this
        // point since all shards have enough free space - more than the
        // DesiredFreeSpaceReserve threshold

        TVector<ui64> handles;
        TVector<ui64> nodes;
        TSet<ui32> shards;
        for (ui32 i = 0; i < 5; ++i) {
            auto createHandleResponse = service.CreateHandle(
                headers,
                fsId,
                RootNodeId,
                Sprintf("file%u", i),
                TCreateHandleArgs::CREATE)->Record;

            const auto nodeId = createHandleResponse.GetNodeAttr().GetId();
            shards.insert(ExtractShardNo(nodeId));
            nodes.push_back(nodeId);

            const auto handleId = createHandleResponse.GetHandle();
            handles.push_back(handleId);
        }

        // checking that we indeed created those 5 files in 5 shards

        UNIT_ASSERT_VALUES_EQUAL(5, shards.size());

        // writing some data to 3 of the 5 files to make 3 of the 5 shards have
        // less than DesiredFreeSpaceReserve free space

        service.WriteData(
            headers,
            fsId,
            nodes[0],
            handles[0],
            0,
            TString(3_MB + 4_KB, 'a'));

        service.WriteData(
            headers,
            fsId,
            nodes[2],
            handles[2],
            0,
            TString(3_MB + 4_KB, 'a'));

        service.WriteData(
            headers,
            fsId,
            nodes[4],
            handles[4],
            0,
            TString(3_MB + 4_KB, 'a'));

        // triggering stats collection from shards
        service.StatFileStore(headers, fsId);

        TSet<ui32> emptyShards;
        emptyShards.insert(ExtractShardNo(nodes[1]));
        emptyShards.insert(ExtractShardNo(nodes[3]));

        handles.clear();
        nodes.clear();
        shards.clear();

        // creating 5 new files - these files should be balanced among the
        // 2 remaining shards which still have more than DesiredFreeSpaceReserve
        // free space

        for (ui32 i = 0; i < 5; ++i) {
            auto createHandleResponse = service.CreateHandle(
                headers,
                fsId,
                RootNodeId,
                Sprintf("file%u", 5 + i),
                TCreateHandleArgs::CREATE)->Record;

            const auto nodeId = createHandleResponse.GetNodeAttr().GetId();
            shards.insert(ExtractShardNo(nodeId));
            nodes.push_back(nodeId);

            const auto handleId = createHandleResponse.GetHandle();
            handles.push_back(handleId);
        }

        // checking that the new 5 files were indeed created in those 2 shards

        UNIT_ASSERT_VALUES_EQUAL(2, shards.size());
        auto l = emptyShards.begin();
        auto r = shards.begin();
        while (l != emptyShards.end()) {
            UNIT_ASSERT_VALUES_EQUAL(*l, *r);
            ++l;
            ++r;
        }
    }

    SERVICE_TEST_SIMPLE(ShouldAddExplicitShardCountAutomaticallyUponResize)
    {
        config.SetAutomaticShardCreationEnabled(true);
        config.SetShardAllocationUnit(4_TB);
        config.SetAutomaticallyCreatedShardSize(5_TB);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1_GB / 4_KB);

        TVector<TString> expected = {fsId};
        auto listing = service.ListFileStores();
        auto fsIds = listing->Record.GetFileStores();
        TVector<TString> ids(fsIds.begin(), fsIds.end());
        Sort(ids);
        UNIT_ASSERT_VALUES_EQUAL(expected, ids);

        // explicitly specifying shard count, other params are left intact
        service.ResizeFileStore(fsId, 1_GB / 4_KB, false, 10);

        expected = TVector<TString>{
            fsId,
            fsId + "_s1",
            fsId + "_s10",
            fsId + "_s2",
            fsId + "_s3",
            fsId + "_s4",
            fsId + "_s5",
            fsId + "_s6",
            fsId + "_s7",
            fsId + "_s8",
            fsId + "_s9",
        };
        listing = service.ListFileStores();
        fsIds = listing->Record.GetFileStores();
        ids = TVector<TString>(fsIds.begin(), fsIds.end());
        Sort(ids);
        UNIT_ASSERT_VALUES_EQUAL(expected, ids);
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER_ONLY(ShouldCreateDirectoryStructureInShards)
    {
        config.SetMultiTabletForwardingEnabled(true);
        config.SetDirectoryCreationInShardsEnabled(true);

        TShardedFileSystemConfig fsConfig{.DirectoryCreationInShardsEnabled = true};
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(RootNodeId, "dir1"))->Record;
        const auto dir1Id = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(dir1Id));

        createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(dir1Id, "dir1_1"))->Record;
        const auto dir1_1Id = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(dir1_1Id));

        createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(dir1Id, "dir1_2"))->Record;
        const auto dir1_2Id = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(2, ExtractShardNo(dir1_2Id));

        auto createHandleResponse = service.CreateHandle(
            headers,
            fsConfig.FsId,
            dir1_1Id,
            "file1",
            TCreateHandleArgs::CREATE)->Record;

        const auto nodeId1 = createHandleResponse.GetNodeAttr().GetId();
        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(nodeId1));

        const auto handle1 = createHandleResponse.GetHandle();
        UNIT_ASSERT_VALUES_EQUAL_C(1, ExtractShardNo(handle1), handle1);

        createHandleResponse = service.CreateHandle(
            headers,
            fsConfig.FsId,
            dir1_2Id,
            "file1",
            TCreateHandleArgs::CREATE)->Record;

        const auto nodeId2 = createHandleResponse.GetNodeAttr().GetId();
        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(nodeId2));

        const auto handle2 = createHandleResponse.GetHandle();
        UNIT_ASSERT_VALUES_EQUAL_C(1, ExtractShardNo(handle2), handle2);

        createHandleResponse = service.CreateHandle(
            headers,
            fsConfig.FsId,
            dir1_2Id,
            "file2",
            TCreateHandleArgs::CREATE)->Record;

        const auto nodeId3 = createHandleResponse.GetNodeAttr().GetId();
        UNIT_ASSERT_VALUES_EQUAL(2, ExtractShardNo(nodeId3));

        const auto handle3 = createHandleResponse.GetHandle();
        UNIT_ASSERT_VALUES_EQUAL_C(2, ExtractShardNo(handle3), handle3);

        auto data1 = GenerateValidateData(256_KB, 1);
        service.WriteData(headers, fsConfig.FsId, nodeId1, handle1, 0, data1);

        auto data2 = GenerateValidateData(1_MB, 2);
        service.WriteData(headers, fsConfig.FsId, nodeId2, handle2, 0, data2);

        auto data3 = GenerateValidateData(512_KB, 3);
        service.WriteData(headers, fsConfig.FsId, nodeId3, handle3, 0, data3);

        auto readDataResponse = service.ReadData(
            headers,
            fsConfig.FsId,
            nodeId1,
            handle1,
            0,
            data1.size())->Record;
        UNIT_ASSERT_VALUES_EQUAL(data1, readDataResponse.GetBuffer());

        readDataResponse = service.ReadData(
            headers,
            fsConfig.FsId,
            nodeId2,
            handle2,
            0,
            data2.size())->Record;
        UNIT_ASSERT_VALUES_EQUAL(data2, readDataResponse.GetBuffer());

        readDataResponse = service.ReadData(
            headers,
            fsConfig.FsId,
            nodeId3,
            handle3,
            0,
            data3.size())->Record;
        UNIT_ASSERT_VALUES_EQUAL(data3, readDataResponse.GetBuffer());

        service.DestroyHandle(headers, fsConfig.FsId, nodeId1, handle1);
        service.DestroyHandle(headers, fsConfig.FsId, nodeId2, handle2);
        service.DestroyHandle(headers, fsConfig.FsId, nodeId3, handle3);
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER_ONLY(ShouldCreateHardLinks)
    {
        config.SetMultiTabletForwardingEnabled(true);
        config.SetDirectoryCreationInShardsEnabled(true);

        TShardedFileSystemConfig fsConfig{.DirectoryCreationInShardsEnabled = true};
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        const auto dirId =
            service
                .CreateNode(
                    headers,
                    TCreateNodeArgs::Directory(RootNodeId, "dir"))
                ->Record.GetNode()
                .GetId();

        const auto file1Id = service.CreateNode(
            headers,
            TCreateNodeArgs::File(dirId, "file1"))->Record.GetNode().GetId();

        service.CreateNode(
            headers,
            TCreateNodeArgs::Link(dirId, "link1", file1Id));
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER_ONLY(
        ShouldListNodesAndGetNodeAttrInDirectoryInShard)
    {
        config.SetMultiTabletForwardingEnabled(true);
        config.SetDirectoryCreationInShardsEnabled(true);

        TShardedFileSystemConfig fsConfig{.DirectoryCreationInShardsEnabled = true};
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(RootNodeId, "dir1"))->Record;
        const auto dir1Id = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(dir1Id));

        service.CreateNode(
            headers,
            TCreateNodeArgs::File(dir1Id, "file1"));
        service.CreateNode(
            headers,
            TCreateNodeArgs::File(dir1Id, "file2"));
        service.CreateNode(
            headers,
            TCreateNodeArgs::File(dir1Id, "file3"));
        service.CreateNode(
            headers,
            TCreateNodeArgs::File(dir1Id, "file4"));

        auto listNodesResponse = service.ListNodes(
            headers,
            fsConfig.FsId,
            dir1Id)->Record;

        UNIT_ASSERT_VALUES_EQUAL(4, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("file1", listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL("file2", listNodesResponse.GetNames(1));
        UNIT_ASSERT_VALUES_EQUAL("file3", listNodesResponse.GetNames(2));
        UNIT_ASSERT_VALUES_EQUAL("file4", listNodesResponse.GetNames(3));
        TVector<std::pair<ui64, TString>> nodes(4);
        for (ui32 i = 0; i < 4; ++i) {
            nodes[i] = {
                listNodesResponse.GetNodes(i).GetId(),
                listNodesResponse.GetNames(i)};
            UNIT_ASSERT_VALUES_UNEQUAL(0, nodes[i].first);
        }

        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(nodes[0].first));
        UNIT_ASSERT_VALUES_EQUAL(2, ExtractShardNo(nodes[1].first));
        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(nodes[2].first));
        UNIT_ASSERT_VALUES_EQUAL(2, ExtractShardNo(nodes[3].first));

        auto getAttrResponse = service.GetNodeAttr(
            headers,
            fsConfig.FsId,
            RootNodeId,
            "dir1")->Record;

        UNIT_ASSERT_VALUES_EQUAL(dir1Id, getAttrResponse.GetNode().GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::E_DIRECTORY_NODE),
            getAttrResponse.GetNode().GetType());

        getAttrResponse = service.GetNodeAttr(
            headers,
            fsConfig.FsId,
            dir1Id,
            "file1")->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            nodes[0].first,
            getAttrResponse.GetNode().GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::E_REGULAR_NODE),
            getAttrResponse.GetNode().GetType());

        getAttrResponse = service.GetNodeAttr(
            headers,
            fsConfig.FsId,
            dir1Id,
            "file2")->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            nodes[1].first,
            getAttrResponse.GetNode().GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::E_REGULAR_NODE),
            getAttrResponse.GetNode().GetType());

        getAttrResponse = service.GetNodeAttr(
            headers,
            fsConfig.FsId,
            dir1Id,
            "file3")->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            nodes[2].first,
            getAttrResponse.GetNode().GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::E_REGULAR_NODE),
            getAttrResponse.GetNode().GetType());

        getAttrResponse = service.GetNodeAttr(
            headers,
            fsConfig.FsId,
            dir1Id,
            "file4")->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            nodes[3].first,
            getAttrResponse.GetNode().GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(NProto::E_REGULAR_NODE),
            getAttrResponse.GetNode().GetType());
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER_ONLY(
        ShouldAggregateFileSystemMetricsInBackgroundWithDirectoriesInShards)
    {
        config.SetMultiTabletForwardingEnabled(true);
        config.SetDirectoryCreationInShardsEnabled(true);

        TShardedFileSystemConfig fsConfig{.DirectoryCreationInShardsEnabled = true};
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        // creating 2 files

        auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"))->Record;

        const auto nodeId1 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(nodeId1));

        createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file2"))->Record;

        const auto nodeId2 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(2, ExtractShardNo(nodeId2));

        ui64 handle1 = service.CreateHandle(
            headers,
            fsConfig.FsId,
            nodeId1,
            "",
            TCreateHandleArgs::RDWR)->Record.GetHandle();

        auto data1 = GenerateValidateData(256_KB);
        service.WriteData(headers, fsConfig.FsId, nodeId1, handle1, 0, data1);

        ui64 handle2 = service.CreateHandle(
            headers,
            fsConfig.FsId,
            nodeId2,
            "",
            TCreateHandleArgs::RDWR)->Record.GetHandle();

        auto data2 = GenerateValidateData(512_KB);
        service.WriteData(headers, fsConfig.FsId, nodeId2, handle2, 0, data2);

        // triggering background shard stats collection

        env.GetRuntime().AdvanceCurrentTime(TDuration::Seconds(15));
        env.GetRuntime().DispatchEvents(
            TDispatchOptions{
                .FinalEvents = {TDispatchOptions::TFinalEventCondition(
                    TEvIndexTablet::EvGetStorageStatsResponse,
                    2)}});

        {
            using TRequest = TEvIndexTabletPrivate::TEvUpdateCounters;

            env.GetRuntime().Send(
                new IEventHandle(
                    fsInfo.Shard1ActorId, // recipient
                    TActorId(), // sender
                    new TRequest(),
                    0, // flags
                    0),
                0);
        }

        TDispatchOptions options;
        options.FinalEvents = {
            TDispatchOptions::TFinalEventCondition(
                TEvIndexTabletPrivate::EvAggregateStatsCompleted)};
        service.AccessRuntime().DispatchEvents(options);

        {
            auto response = GetStorageStats(service, fsConfig.Shard1Id);
            auto stats = response.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.ShardStatsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                data1.size() / 4_KB,
                stats.GetUsedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                data1.size() / 4_KB,
                stats.GetMixedBlocksCount());

            response = GetStorageStats(
                service,
                fsConfig.Shard1Id,
                true /*allowCache*/,
                NProtoPrivate::STATS_REQUEST_MODE_FORCE_FETCH_SHARDS);
            stats = response.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(2, stats.ShardStatsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                64,
                stats.GetShardStats(0).GetUsedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                1000,
                stats.GetShardStats(0).GetTotalBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                128,
                stats.GetShardStats(1).GetUsedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                1000,
                stats.GetShardStats(1).GetTotalBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                (data1.size() + data2.size()) / 4_KB,
                stats.GetUsedBlocksCount());
            UNIT_ASSERT_VALUES_EQUAL(
                (data1.size() + data2.size()) / 4_KB,
                stats.GetMixedBlocksCount());
        }
    }

    struct TEventHelper
    {
        TTestEnv& Env;
        TAutoPtr<IEventHandle> Event;
        bool ShouldIntercept = true;

        void CatchEvent(ui32 eventType)
        {
            Env.GetRuntime().SetEventFilter(
                [&, eventType] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                    Y_UNUSED(runtime);
                    if (ShouldIntercept
                            && event->GetTypeRewrite() == eventType)
                    {
                        Event = event.Release();
                        return true;
                    }
                    return false;
                });
        }
    };

    SERVICE_TEST_SID_SELECT_IN_LEADER_ONLY(
        ShouldUnlinkNodeWithDirectoriesInShards)
    {
        config.SetMultiTabletForwardingEnabled(true);
        config.SetDirectoryCreationInShardsEnabled(true);

        TShardedFileSystemConfig fsConfig{.DirectoryCreationInShardsEnabled = true};
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        // /
        // └── dir              (shard 1)
        //     ├── file         (shard 1)
        //     └── subdir       (shard 2)
        //         └── file     (shard 1)

        auto dirId = service
                         .CreateNode(
                             headers,
                             TCreateNodeArgs::Directory(RootNodeId, "dir"))
                         ->Record.GetNode()
                         .GetId();
        Cerr << "dirId: " << dirId << Endl;
        service.CreateNode(headers, TCreateNodeArgs::File(dirId, "file"));
        auto subdirId = service
                            .CreateNode(
                                headers,
                                TCreateNodeArgs::Directory(dirId, "subdir"))
                            ->Record.GetNode()
                            .GetId();
        Cerr << "subdirId: " << subdirId << Endl;
        UNIT_ASSERT(ExtractShardNo(dirId) != ExtractShardNo(subdirId));
        auto fileId =
            service
                .CreateNode(headers, TCreateNodeArgs::File(subdirId, "file"))
                ->Record.GetNode()
                .GetId();
        Cerr << "fileId: " << fileId << Endl;
        Y_UNUSED(fileId);

        auto unlinkResponse =
            service.SendAndRecvUnlinkNode(headers, dirId, "subdir", true);

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_FS_NOTEMPTY,
            unlinkResponse->GetError().GetCode(),
            unlinkResponse->GetError().GetMessage());

        service.GetNodeAttr(headers, fsConfig.FsId, dirId, "subdir");

        // unlinking the file in subdir
        service.UnlinkNode(headers, subdirId, "file", false);

        // unlinking subdir should now succeed
        service.UnlinkNode(headers, dirId, "subdir", true);

        // creating the subdir again
        subdirId = service
                       .CreateNode(
                           headers,
                           TCreateNodeArgs::Directory(dirId, "subdir"))
                       ->Record.GetNode()
                       .GetId();
        // unlinking the subdir should now succeed
        service.UnlinkNode(headers, dirId, "subdir", true);
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER_ONLY(
        ShouldRetryRenameNodeInShardUponLeaderRestartWithDirectoriesInShards)
    {
        config.SetMultiTabletForwardingEnabled(true);
        config.SetDirectoryCreationInShardsEnabled(true);

        TShardedFileSystemConfig fsConfig{.DirectoryCreationInShardsEnabled = true};
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        // creating 2 dirs - we'll move a file from one dir to another

        ui64 dir1Id = service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(RootNodeId, "dir1")
        )->Record.GetNode().GetId();

        ui64 dir2Id = service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(RootNodeId, "dir2")
        )->Record.GetNode().GetId();

        UNIT_ASSERT_VALUES_UNEQUAL(0, ExtractShardNo(dir1Id));
        UNIT_ASSERT_VALUES_UNEQUAL(0, ExtractShardNo(dir2Id));
        UNIT_ASSERT_VALUES_UNEQUAL(
            ExtractShardNo(dir1Id),
            ExtractShardNo(dir2Id));

        // creating a file which we will move

        ui64 node1Id = service.CreateNode(
            headers,
            TCreateNodeArgs::File(dir1Id, "file1")
        )->Record.GetNode().GetId();

        // configuring an interceptor to make RenameNode get stuck in the shard
        // in charge of dir1

        TEventHelper evHelper(env);
        evHelper.CatchEvent(TEvIndexTablet::EvRenameNodeInDestinationRequest);

        service.SendRenameNodeRequest(
            headers,
            dir1Id,
            "file1",
            dir2Id,
            "file2",
            0);

        ui32 iterations = 0;
        while (!evHelper.Event && iterations++ < 100) {
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT(evHelper.Event);
        evHelper.ShouldIntercept = false;

        // listing the nodes in dir1 and dir2 - no changes should be visible
        // at this point

        {
            auto listNodesResponse = service.ListNodes(
                headers,
                fsConfig.FsId,
                dir1Id)->Record;

            UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
            UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
        }

        {
            auto listNodesResponse = service.ListNodes(
                headers,
                fsConfig.FsId,
                dir2Id)->Record;

            UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());
        }

        // rebooting the shard in charge of dir1 to trigger OpLog replay which
        // should rerun the part of the RenameNode operation that got stuck

        const ui64 tabletId = ExtractShardNo(dir1Id) == 1
            ? fsInfo.Shard1TabletId : fsInfo.Shard2TabletId;
        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.RebootTablet();

        {
            auto renameResponse = service.RecvRenameNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                renameResponse->GetError().GetCode(),
                renameResponse->GetError().GetMessage());
        }

        // remaking session since CreateSessionActor doesn't do it by itself
        // because EvWakeup never arrives because Scheduling doesn't work by
        // default and RegistrationObservers get reset after RebootTablet
        headers = service.InitSession(fsConfig.FsId, "client");

        // after shard reboot our RenameNode operation should have completed
        // checking it via directory listing

        {
            auto listNodesResponse = service.ListNodes(
                headers,
                fsConfig.FsId,
                dir1Id)->Record;

            UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());
        }

        {
            auto listNodesResponse = service.ListNodes(
                headers,
                fsConfig.FsId,
                dir2Id)->Record;

            UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
            UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
            UNIT_ASSERT_VALUES_EQUAL("file2", listNodesResponse.GetNames(0));
            UNIT_ASSERT_VALUES_EQUAL(
                node1Id,
                listNodesResponse.GetNodes(0).GetId());
        }
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER_ONLY(
        ShouldLockSourceNodeRefsUponRenameNodeWithDirectoriesInShards)
    {
        config.SetMultiTabletForwardingEnabled(true);
        config.SetDirectoryCreationInShardsEnabled(true);

        TShardedFileSystemConfig fsConfig{.DirectoryCreationInShardsEnabled = true};
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        // creating 2 dirs - we'll move a file from one dir to another

        ui64 dir1Id = service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(RootNodeId, "dir1")
        )->Record.GetNode().GetId();

        ui64 dir2Id = service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(RootNodeId, "dir2")
        )->Record.GetNode().GetId();

        UNIT_ASSERT_VALUES_UNEQUAL(0, ExtractShardNo(dir1Id));
        UNIT_ASSERT_VALUES_UNEQUAL(0, ExtractShardNo(dir2Id));
        UNIT_ASSERT_VALUES_UNEQUAL(
            ExtractShardNo(dir1Id),
            ExtractShardNo(dir2Id));

        // creating a file which we will move

        ui64 node1Id = service.CreateNode(
            headers,
            TCreateNodeArgs::File(dir1Id, "file1")
        )->Record.GetNode().GetId();

        // configuring an interceptor to make RenameNode get stuck in the shard
        // in charge of dir1

        TEventHelper evHelper(env);
        evHelper.CatchEvent(TEvIndexTablet::EvRenameNodeInDestinationRequest);

        service.SendRenameNodeRequest(
            headers,
            dir1Id,
            "file1",
            dir2Id,
            "file2",
            0);

        ui32 iterations = 0;
        while (!evHelper.Event && iterations++ < 100) {
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT(evHelper.Event);
        evHelper.ShouldIntercept = false;

        // listing dir1 - at this point our file should still be seen there

        {
            auto listNodesResponse = service.ListNodes(
                headers,
                fsConfig.FsId,
                dir1Id)->Record;

            UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
            UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
            UNIT_ASSERT_VALUES_EQUAL("file1", listNodesResponse.GetNames(0));
        }

        // unlinking of that file should be unallowed since we have an inflight
        // RenameNode operation

        service.SendUnlinkNodeRequest(headers, dir1Id, "file1");
        {
            auto unlinkResponse = service.RecvUnlinkNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                unlinkResponse->GetError().GetCode(),
                unlinkResponse->GetError().GetMessage());
        }

        // renaming should not be allowed as well

        service.SendRenameNodeRequest(
            headers,
            dir1Id,
            "file1",
            dir1Id,
            "file1_moved",
            0);

        {
            auto renameResponse = service.RecvRenameNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                renameResponse->GetError().GetCode(),
                renameResponse->GetError().GetMessage());
        }

        // listing dir1 - our file should still be there

        {
            auto listNodesResponse = service.ListNodes(
                headers,
                fsConfig.FsId,
                dir1Id)->Record;

            UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
            UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
            UNIT_ASSERT_VALUES_EQUAL("file1", listNodesResponse.GetNames(0));
        }

        // unblocking the inflight RenameNode op

        env.GetRuntime().Send(evHelper.Event.Release());

        {
            auto renameResponse = service.RecvRenameNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                renameResponse->GetError().GetCode(),
                renameResponse->GetError().GetMessage());
        }

        // now the file should finally be moved

        {
            auto listNodesResponse = service.ListNodes(
                headers,
                fsConfig.FsId,
                dir1Id)->Record;

            UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
            UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());
        }

        {
            auto listNodesResponse = service.ListNodes(
                headers,
                fsConfig.FsId,
                dir2Id)->Record;

            UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
            UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
            UNIT_ASSERT_VALUES_EQUAL("file2", listNodesResponse.GetNames(0));
            UNIT_ASSERT_VALUES_EQUAL(
                node1Id,
                listNodesResponse.GetNodes(0).GetId());
        }

        // RenameNode and UnlinkNode ops should work again

        ui64 node2Id = service.CreateNode(
            headers,
            TCreateNodeArgs::File(dir1Id, "file1")
        )->Record.GetNode().GetId();
        UNIT_ASSERT_VALUES_UNEQUAL(node1Id, node2Id);

        service.SendRenameNodeRequest(
            headers,
            dir1Id,
            "file1",
            dir1Id,
            "file1_moved",
            0);

        {
            auto renameResponse = service.RecvRenameNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                renameResponse->GetError().GetCode(),
                renameResponse->GetError().GetMessage());
        }

        ui64 node3Id = service.CreateNode(
            headers,
            TCreateNodeArgs::File(dir1Id, "file1")
        )->Record.GetNode().GetId();
        UNIT_ASSERT_VALUES_UNEQUAL(node1Id, node3Id);
        UNIT_ASSERT_VALUES_UNEQUAL(node2Id, node3Id);

        service.SendUnlinkNodeRequest(headers, dir1Id, "file1");
        {
            auto unlinkResponse = service.RecvUnlinkNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                unlinkResponse->GetError().GetCode(),
                unlinkResponse->GetError().GetMessage());
        }

        {
            auto listNodesResponse = service.ListNodes(
                headers,
                fsConfig.FsId,
                dir1Id)->Record;

            UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
            UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
            UNIT_ASSERT_VALUES_EQUAL("file1_moved", listNodesResponse.GetNames(0));
            UNIT_ASSERT_VALUES_EQUAL(
                node2Id,
                listNodesResponse.GetNodes(0).GetId());
        }
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER_ONLY(
        ShouldRestoreNodeRefLocksUponLeaderRestartWithDirectoriesInShards)
    {
        config.SetMultiTabletForwardingEnabled(true);
        config.SetDirectoryCreationInShardsEnabled(true);

        TShardedFileSystemConfig fsConfig{.DirectoryCreationInShardsEnabled = true};
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        // creating 2 dirs - we'll move a file from one dir to another

        ui64 dir1Id = service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(RootNodeId, "dir1")
        )->Record.GetNode().GetId();

        ui64 dir2Id = service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(RootNodeId, "dir2")
        )->Record.GetNode().GetId();

        UNIT_ASSERT_VALUES_UNEQUAL(0, ExtractShardNo(dir1Id));
        UNIT_ASSERT_VALUES_UNEQUAL(0, ExtractShardNo(dir2Id));
        UNIT_ASSERT_VALUES_UNEQUAL(
            ExtractShardNo(dir1Id),
            ExtractShardNo(dir2Id));

        // creating a file which we will move

        service.CreateNode(headers, TCreateNodeArgs::File(dir1Id, "file1"));

        // configuring an interceptor to make RenameNode get stuck in the shard
        // in charge of dir1

        TEventHelper evHelper(env);
        evHelper.CatchEvent(TEvIndexTablet::EvRenameNodeInDestinationRequest);

        service.SendRenameNodeRequest(
            headers,
            dir1Id,
            "file1",
            dir2Id,
            "file2",
            0);

        ui32 iterations = 0;
        while (!evHelper.Event && iterations++ < 100) {
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT(evHelper.Event);

        // rebooting the shard in charge of dir1 to trigger OpLog replay which
        // should lock the affected NodeRef

        const ui64 tabletId = ExtractShardNo(dir1Id) == 1
            ? fsInfo.Shard1TabletId : fsInfo.Shard2TabletId;
        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.RebootTablet();

        {
            auto renameResponse = service.RecvRenameNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                renameResponse->GetError().GetCode(),
                renameResponse->GetError().GetMessage());
        }

        // remaking session since CreateSessionActor doesn't do it by itself
        // because EvWakeup never arrives because Scheduling doesn't work by
        // default and RegistrationObservers get reset after RebootTablet
        headers = service.InitSession(fsConfig.FsId, "client");

        // unlinking of that file should be unallowed since we have an inflight
        // RenameNode operation

        service.SendUnlinkNodeRequest(headers, dir1Id, "file1");
        {
            auto unlinkResponse = service.RecvUnlinkNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                unlinkResponse->GetError().GetCode(),
                unlinkResponse->GetError().GetMessage());
        }

        // renaming should not be allowed as well

        service.SendRenameNodeRequest(
            headers,
            dir1Id,
            "file1",
            dir1Id,
            "file1_moved",
            0);

        {
            auto renameResponse = service.RecvRenameNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                renameResponse->GetError().GetCode(),
                renameResponse->GetError().GetMessage());
        }
    }

    SERVICE_TEST_SID_SELECT_IN_LEADER_ONLY(
        ShouldProcessRenameNodeInDestinationErrorWithDirectoriesInShards)
    {
        config.SetMultiTabletForwardingEnabled(true);
        config.SetDirectoryCreationInShardsEnabled(true);

        TShardedFileSystemConfig fsConfig{.DirectoryCreationInShardsEnabled = true};
        CREATE_ENV_AND_SHARDED_FILESYSTEM();

        auto headers = service.InitSession(fsConfig.FsId, "client");

        // creating 2 dirs - we'll move a file from one dir to another

        ui64 dir1Id = service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(RootNodeId, "dir1")
        )->Record.GetNode().GetId();

        ui64 dir2Id = service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(RootNodeId, "dir2")
        )->Record.GetNode().GetId();

        UNIT_ASSERT_VALUES_UNEQUAL(0, ExtractShardNo(dir1Id));
        UNIT_ASSERT_VALUES_UNEQUAL(0, ExtractShardNo(dir2Id));
        UNIT_ASSERT_VALUES_UNEQUAL(
            ExtractShardNo(dir1Id),
            ExtractShardNo(dir2Id));

        // creating a file which we will try move

        ui64 node1Id = service.CreateNode(
            headers,
            TCreateNodeArgs::File(dir1Id, "file1")
        )->Record.GetNode().GetId();

        // creating a file which will prevent the move from succeeding

        ui64 node2Id = service.CreateNode(
            headers,
            TCreateNodeArgs::File(dir2Id, "file2")
        )->Record.GetNode().GetId();

        UNIT_ASSERT_VALUES_UNEQUAL(node1Id, node2Id);

        service.SendRenameNodeRequest(
            headers,
            dir1Id,
            "file1",
            dir2Id,
            "file2",
            ProtoFlag(NProto::TRenameNodeRequest::F_NOREPLACE));

        {
            auto renameResponse = service.RecvRenameNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_EXIST,
                renameResponse->GetError().GetCode(),
                renameResponse->GetError().GetMessage());
        }

        // listing the nodes in dir1 and dir2 - no changes should be done

        {
            auto listNodesResponse = service.ListNodes(
                headers,
                fsConfig.FsId,
                dir1Id)->Record;

            UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
            UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                node1Id,
                listNodesResponse.GetNodes(0).GetId());
            UNIT_ASSERT_VALUES_EQUAL("file1", listNodesResponse.GetNames(0));
        }

        {
            auto listNodesResponse = service.ListNodes(
                headers,
                fsConfig.FsId,
                dir2Id)->Record;

            UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
            UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                node2Id,
                listNodesResponse.GetNodes(0).GetId());
            UNIT_ASSERT_VALUES_EQUAL("file2", listNodesResponse.GetNames(0));
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
