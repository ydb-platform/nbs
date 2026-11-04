#include "service.h"
#include "service_ut_sharding.h"

#include <cloud/filestore/libs/service/filesystem_event.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/model/utils.h>
#include <cloud/filestore/libs/storage/testlib/service_client.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestFileSystemEventHandler final
    : public IFileSystemEventHandler
{
    TVector<NProto::TFileSystemEvent> Events;
    ui32 DisconnectCount = 0;

    void OnEvent(const NProto::TFileSystemEvent& event) override
    {
        Events.push_back(event);
    }

    void OnDisconnect(ui64 tabletId) override
    {
        Y_UNUSED(tabletId);
        ++DisconnectCount;
    }

    bool HasInvalidateNode(ui64 nodeId) const
    {
        for (const auto& event: Events) {
            if (event.HasInvalidateNode()
                    && event.GetInvalidateNode().GetNodeId() == nodeId)
            {
                return true;
            }
        }
        return false;
    }

    bool HasInvalidateNodeRef(ui64 nodeId, const TString& name) const
    {
        for (const auto& event: Events) {
            const auto& invalidate = event.GetInvalidateNodeRef();
            if (event.HasInvalidateNodeRef()
                    && invalidate.GetNodeId() == nodeId
                    && invalidate.GetName() == name)
            {
                return true;
            }
        }
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

TString MakeGenerateFileSystemEventInput(
    const TString& fsId,
    const NProto::TFileSystemEvent& event)
{
    NProtoPrivate::TGenerateFileSystemEventRequest request;
    request.SetFileSystemId(fsId);
    *request.MutableEvent() = event;

    TString buf;
    auto status = google::protobuf::util::MessageToJsonString(request, &buf);
    UNIT_ASSERT_C(status.ok(), status.ToString());
    return buf;
}

NProtoPrivate::TGenerateFileSystemEventResponse GenerateFileSystemEvent(
    TServiceClient& service,
    const TString& fsId,
    const NProto::TFileSystemEvent& event)
{
    auto actionResponse = service.ExecuteAction(
        "generatefilesystemevent",
        MakeGenerateFileSystemEventInput(fsId, event));

    NProtoPrivate::TGenerateFileSystemEventResponse response;
    auto status = google::protobuf::util::JsonStringToMessage(
        actionResponse->Record.GetOutput(),
        &response);
    UNIT_ASSERT_C(status.ok(), status.ToString());
    return response;
}

NProto::TFileSystemEvent MakeEvent(ui64 nodeId)
{
    NProto::TFileSystemEvent event;
    event.MutableInvalidateNode()->SetNodeId(nodeId);
    return event;
}

NProto::TFileSystemEvent MakeEvent(ui64 parentNodeId, const TString& name)
{
    NProto::TFileSystemEvent event;
    auto* invalidate = event.MutableInvalidateNodeRef();
    invalidate->SetNodeId(parentNodeId);
    invalidate->SetName(name);
    return event;
}

void CheckClientRegisteredUponListNodesInShards(bool useListNodesInternal)
{
    TShardedFileSystemConfig fsConfig;

    NProto::TStorageConfig config;
    config.SetAutomaticShardCreationEnabled(true);
    config.SetAutomaticallyCreatedShardSize(fsConfig.ShardBlockCount * 4_KB);
    config.SetShardAllocationUnit(fsConfig.ShardBlockCount * 4_KB);
    config.SetUseListNodesInternal(useListNodesInternal);
    config.SetFileSystemEventsEnabled(true);
    config.SetFileSystemListingEventsEnabled(true);
    TTestEnv env({}, config);
    ui32 nodeIdx = env.AddDynamicNode();

    auto handler = std::make_shared<TTestFileSystemEventHandler>();
    env.GetMultiFileSystemEventHandler()->Register(fsConfig.FsId, handler);

    TServiceClient service(env.GetRuntime(), nodeIdx);
    CreateFileSystem(service, fsConfig);

    auto headers = service.InitSession(fsConfig.FsId, "client");
    TVector<ui64> nodeIds;
    for (ui32 i = 0; i < 4; ++i) {
        nodeIds.push_back(
            service
                .CreateNode(
                    headers,
                    TCreateNodeArgs::File(RootNodeId, Sprintf("f%u", i)))
                ->Record.GetNode()
                .GetId());
    }

    //
    // CreateNode doesn't subscribe the client in the main tablet or in the
    // shards.
    //

    const auto shardIds = fsConfig.ShardIds();
    auto shardIdOf = [&] (ui64 nodeId) {
        const ui32 shardNo = ExtractShardNo(nodeId);
        UNIT_ASSERT_LT(0, shardNo);
        return shardIds[shardNo - 1];
    };

    for (ui32 i = 0; i < nodeIds.size(); ++i) {
        auto response = GenerateFileSystemEvent(
            service,
            fsConfig.FsId,
            MakeEvent(RootNodeId, Sprintf("f%u", i)));
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetClientCount());

        response = GenerateFileSystemEvent(
            service,
            shardIdOf(nodeIds[i]),
            MakeEvent(nodeIds[i]));
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetClientCount());
    }

    //
    // ListNodes (or ListNodesInternal) goes to the main tablet and subscribes
    // the client to the listed entries. The attrs of the shard-resident nodes
    // are fetched via GetNodeAttrBatch, which subscribes the client to the
    // nodes in the shards.
    //

    const auto listNodesResponse = service.ListNodes(headers, RootNodeId);
    UNIT_ASSERT_VALUES_EQUAL(4, listNodesResponse->Record.NodesSize());
    const ui32 cacheTTL =
        (TDuration::Minutes(30) - TDuration::Minutes(1)).MilliSeconds();
    UNIT_ASSERT_VALUES_EQUAL(
        cacheTTL,
        listNodesResponse->Record.GetNodeCacheTTL());
    UNIT_ASSERT_VALUES_EQUAL(
        cacheTTL,
        listNodesResponse->Record.GetNodeRefCacheTTL());

    for (ui32 i = 0; i < nodeIds.size(); ++i) {
        auto response = GenerateFileSystemEvent(
            service,
            fsConfig.FsId,
            MakeEvent(RootNodeId, Sprintf("f%u", i)));
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetClientCount());

        response = GenerateFileSystemEvent(
            service,
            shardIdOf(nodeIds[i]),
            MakeEvent(nodeIds[i]));
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetClientCount());
    }

    UNIT_ASSERT_VALUES_EQUAL(2 * nodeIds.size(), handler->Events.size());
    for (const auto& event: handler->Events) {
        UNIT_ASSERT_VALUES_EQUAL(fsConfig.FsId, event.GetFileSystemId());
    }

    //
    // GetNodeAttr of a shard-resident node: the NodeRef comes from the main
    // tablet, the Node - from the shard.
    //

    const auto getNodeAttrResponse =
        service.GetNodeAttr(headers, fsConfig.FsId, RootNodeId, "f0");
    UNIT_ASSERT_VALUES_EQUAL(
        nodeIds[0],
        getNodeAttrResponse->Record.GetNode().GetId());
    UNIT_ASSERT_VALUES_EQUAL(
        cacheTTL,
        getNodeAttrResponse->Record.GetNodeCacheTTL());
    UNIT_ASSERT_VALUES_EQUAL(
        cacheTTL,
        getNodeAttrResponse->Record.GetNodeRefCacheTTL());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStorageServiceFileSystemEventsTest)
{
    Y_UNIT_TEST(ShouldRegisterClientUponListNodesInShards)
    {
        CheckClientRegisteredUponListNodesInShards(
            false /* useListNodesInternal */);
    }

    Y_UNIT_TEST(ShouldRegisterClientUponListNodesInternalInShards)
    {
        CheckClientRegisteredUponListNodesInShards(
            true /* useListNodesInternal */);
    }

    Y_UNIT_TEST(ShouldDeliverGeneratedFileSystemEvent)
    {
        NProto::TStorageConfig config;
        config.SetFileSystemEventsEnabled(true);
        TTestEnv env({}, config);
        ui32 nodeIdx = env.AddDynamicNode();

        auto handler = std::make_shared<TTestFileSystemEventHandler>();
        env.GetMultiFileSystemEventHandler()->Register("test", handler);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);
        auto headers = service.InitSession("test", "client");

        //
        // Nobody has read the entry yet.
        //

        auto response =
            GenerateFileSystemEvent(
                service,
                "test",
                MakeEvent(RootNodeId, "f"));
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetClientCount());
        UNIT_ASSERT_VALUES_EQUAL(0, handler->Events.size());

        //
        // CreateNode doesn't subscribe the client. GetNodeAttr subscribes it
        // to the entry and the node.
        //

        const auto nodeId =
            service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "f"))
                ->Record.GetNode()
                .GetId();
        UNIT_ASSERT_VALUES_EQUAL(0, handler->Events.size());

        response = GenerateFileSystemEvent(service, "test", MakeEvent(nodeId));
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetClientCount());

        const auto getNodeAttrResponse =
            service.GetNodeAttr(headers, "test", RootNodeId, "f");
        const ui32 cacheTTL =
            (TDuration::Minutes(30) - TDuration::Minutes(1)).MilliSeconds();
        UNIT_ASSERT_VALUES_EQUAL(
            cacheTTL,
            getNodeAttrResponse->Record.GetNodeCacheTTL());
        UNIT_ASSERT_VALUES_EQUAL(
            cacheTTL,
            getNodeAttrResponse->Record.GetNodeRefCacheTTL());

        response = GenerateFileSystemEvent(service, "test", MakeEvent(nodeId));
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetClientCount());
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Events.size());

        const auto& event = handler->Events[0];
        UNIT_ASSERT_VALUES_EQUAL("test", event.GetFileSystemId());
        UNIT_ASSERT(event.HasInvalidateNode());
        UNIT_ASSERT(!event.HasInvalidateNodeRef());
        UNIT_ASSERT_VALUES_EQUAL(nodeId, event.GetInvalidateNode().GetNodeId());

        response =
            GenerateFileSystemEvent(
                service,
                "test",
                MakeEvent(RootNodeId, "f"));
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetClientCount());
        UNIT_ASSERT_VALUES_EQUAL(2, handler->Events.size());
        UNIT_ASSERT(handler->HasInvalidateNodeRef(RootNodeId, "f"));
        UNIT_ASSERT(!handler->Events[1].HasInvalidateNode());

        //
        // Subscriptions are one-shot.
        //

        response = GenerateFileSystemEvent(service, "test", MakeEvent(nodeId));
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetClientCount());
        UNIT_ASSERT_VALUES_EQUAL(2, handler->Events.size());

        //
        // Handlers registered for other filesystems get nothing.
        //

        auto otherHandler = std::make_shared<TTestFileSystemEventHandler>();
        env.GetMultiFileSystemEventHandler()->Register(
            "other",
            otherHandler);
        service.GetNodeAttr(headers, "test", nodeId, "");
        GenerateFileSystemEvent(service, "test", MakeEvent(nodeId));
        UNIT_ASSERT_VALUES_EQUAL(3, handler->Events.size());
        UNIT_ASSERT_VALUES_EQUAL(0, otherHandler->Events.size());

        //
        // Unregistered handlers get nothing.
        //

        env.GetMultiFileSystemEventHandler()->Unregister("test", handler);
        service.GetNodeAttr(headers, "test", nodeId, "");
        GenerateFileSystemEvent(service, "test", MakeEvent(nodeId));
        UNIT_ASSERT_VALUES_EQUAL(3, handler->Events.size());
    }

    Y_UNIT_TEST(ShouldRejectInvalidFileSystemEvent)
    {
        TTestEnv env;
        ui32 nodeIdx = env.AddDynamicNode();

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);

        auto response = service.AssertExecuteActionFailed(
            "generatefilesystemevent",
            MakeGenerateFileSystemEventInput("test", {}));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response->GetError().GetCode(),
            FormatError(response->GetError()));

        auto event = MakeEvent(42);
        *event.MutableInvalidateNodeRef() =
            MakeEvent(1, "a").GetInvalidateNodeRef();
        response = service.AssertExecuteActionFailed(
            "generatefilesystemevent",
            MakeGenerateFileSystemEventInput("test", event));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response->GetError().GetCode(),
            FormatError(response->GetError()));

        event = MakeEvent(42);
        auto* invalidate = event.MutableInvalidateNodeXAttr();
        invalidate->SetNodeId(42);
        invalidate->SetName("user.a");
        response = service.AssertExecuteActionFailed(
            "generatefilesystemevent",
            MakeGenerateFileSystemEventInput("test", event));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response->GetError().GetCode(),
            FormatError(response->GetError()));
    }

    Y_UNIT_TEST(ShouldGenerateFileSystemEventsUponNodeChanges)
    {
        NProto::TStorageConfig config;
        config.SetFileSystemEventsEnabled(true);
        TTestEnv env({}, config);
        ui32 nodeIdx = env.AddDynamicNode();

        auto handler = std::make_shared<TTestFileSystemEventHandler>();
        env.GetMultiFileSystemEventHandler()->Register("test", handler);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);
        auto headers = service.InitSession("test", "client");

        const auto nodeId =
            service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "f"))
                ->Record.GetNode()
                .GetId();

        //
        // Nobody had read the entry before it was created - the event isn't
        // sent to anyone.
        //

        UNIT_ASSERT_VALUES_EQUAL(0, handler->Events.size());

        service.GetNodeAttr(headers, "test", RootNodeId, "f");
        service.UnlinkNode(headers, RootNodeId, "f");
        for (const auto& event: handler->Events) {
            UNIT_ASSERT_VALUES_EQUAL("test", event.GetFileSystemId());
        }

        UNIT_ASSERT(handler->HasInvalidateNodeRef(RootNodeId, "f"));
        UNIT_ASSERT(handler->HasInvalidateNode(nodeId));
    }

    Y_UNIT_TEST(ShouldGenerateInvalidateNodeUponWriteOnlyIfSizeChanges)
    {
        NProto::TStorageConfig config;
        config.SetFileSystemEventsEnabled(true);
        TTestEnv env({}, config);
        ui32 nodeIdx = env.AddDynamicNode();

        auto handler = std::make_shared<TTestFileSystemEventHandler>();
        env.GetMultiFileSystemEventHandler()->Register("test", handler);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);
        auto headers = service.InitSession("test", "client");

        const auto nodeId =
            service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "f"))
                ->Record.GetNode()
                .GetId();
        const auto handle =
            service
                .CreateHandle(
                    headers,
                    "test",
                    nodeId,
                    "",
                    TCreateHandleArgs::RDWR)
                ->Record.GetHandle();

        //
        // Extends the file - InvalidateNode is expected.
        //

        handler->Events.clear();
        service.GetNodeAttr(headers, "test", nodeId, "");
        service.WriteData(headers, "test", nodeId, handle, 0, TString(10, 'a'));
        UNIT_ASSERT(handler->HasInvalidateNode(nodeId));

        //
        // Overwrites the data within the file size - only MTime changes,
        // no InvalidateNode is expected. The subscription has been consumed
        // by the previous event, so the client reads the node again.
        //

        handler->Events.clear();
        service.GetNodeAttr(headers, "test", nodeId, "");
        service.WriteData(headers, "test", nodeId, handle, 0, TString(5, 'b'));
        UNIT_ASSERT(!handler->HasInvalidateNode(nodeId));

        //
        // Explicit time change via SetNodeAttr - InvalidateNode is expected.
        //

        handler->Events.clear();
        service.GetNodeAttr(headers, "test", nodeId, "");
        service.SetNodeAttr(
            headers,
            "test",
            TSetNodeAttrArgs(nodeId).SetMTime(42));
        UNIT_ASSERT(handler->HasInvalidateNode(nodeId));
    }

    Y_UNIT_TEST(ShouldNotGenerateFileSystemEventsIfDisabled)
    {
        TTestEnv env;
        ui32 nodeIdx = env.AddDynamicNode();

        auto handler = std::make_shared<TTestFileSystemEventHandler>();
        env.GetMultiFileSystemEventHandler()->Register("test", handler);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);
        auto headers = service.InitSession("test", "client");

        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "f"));
        service.UnlinkNode(headers, RootNodeId, "f");

        UNIT_ASSERT_VALUES_EQUAL(0, handler->Events.size());
    }

    Y_UNIT_TEST(ShouldNotifyHandlerUponPipeDisconnect)
    {
        NProto::TStorageConfig config;
        config.SetFileSystemEventsEnabled(true);
        TTestEnv env({}, config);
        ui32 nodeIdx = env.AddDynamicNode();
        auto& runtime = env.GetRuntime();

        ui64 tabletId = -1;
        runtime.SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                if (event->GetTypeRewrite()
                        == TEvSSProxy::EvDescribeFileStoreResponse)
                {
                    using TResponse = TEvSSProxy::TEvDescribeFileStoreResponse;
                    const auto* msg = event->Get<TResponse>();
                    const auto& desc =
                        msg->PathDescription.GetFileStoreDescription();
                    tabletId = desc.GetIndexTabletId();
                }
                return false;
            });

        auto handler = std::make_shared<TTestFileSystemEventHandler>();
        env.GetMultiFileSystemEventHandler()->Register("test", handler);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);
        auto headers = service.InitSession("test", "client");
        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "f"));

        UNIT_ASSERT_VALUES_UNEQUAL(-1, tabletId);
        UNIT_ASSERT_VALUES_EQUAL(0, handler->DisconnectCount);

        TIndexTabletClient tablet(runtime, nodeIdx, tabletId);
        tablet.RebootTablet();

        UNIT_ASSERT_LE(1, handler->DisconnectCount);

        //
        // The rebooted tablet has no clients and subscriptions until the
        // client reads the entry again via a new pipe.
        //

        auto response =
            GenerateFileSystemEvent(
                service,
                "test",
                MakeEvent(RootNodeId, "f"));
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetClientCount());

        headers = service.InitSession(
            "test",
            "client",
            {} /* checkpointId */,
            true /* restoreClientSession */);
        service.GetNodeAttr(headers, "test", RootNodeId, "f");

        response =
            GenerateFileSystemEvent(
                service,
                "test",
                MakeEvent(RootNodeId, "f"));
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetClientCount());
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Events.size());
    }

    Y_UNIT_TEST(ShouldDeliverFileSystemEventsFromShards)
    {
        TShardedFileSystemConfig fsConfig;

        NProto::TStorageConfig config;
        config.SetAutomaticShardCreationEnabled(true);
        config.SetAutomaticallyCreatedShardSize(
            fsConfig.ShardBlockCount * 4_KB);
        config.SetShardAllocationUnit(fsConfig.ShardBlockCount * 4_KB);
        config.SetFileSystemEventsEnabled(true);
        TTestEnv env({}, config);
        ui32 nodeIdx = env.AddDynamicNode();

        auto handler = std::make_shared<TTestFileSystemEventHandler>();
        env.GetMultiFileSystemEventHandler()->Register(
            fsConfig.FsId,
            handler);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        CreateFileSystem(service, fsConfig);

        auto headers = service.InitSession(fsConfig.FsId, "client");
        auto shardHeaders = headers;
        shardHeaders.FileSystemId = fsConfig.Shard1Id;
        service.GetNodeAttr(shardHeaders, fsConfig.Shard1Id, RootNodeId, "");

        auto response = GenerateFileSystemEvent(
            service,
            fsConfig.Shard1Id,
            MakeEvent(RootNodeId));
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetClientCount());
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Events.size());

        //
        // Shards report the main FileSystemId so that the handlers do not
        // need to know about the shards.
        //

        for (const auto& event: handler->Events) {
            UNIT_ASSERT_VALUES_EQUAL(fsConfig.FsId, event.GetFileSystemId());
        }
        UNIT_ASSERT(handler->HasInvalidateNode(RootNodeId));
    }
}

}   // namespace NCloud::NFileStore::NStorage
