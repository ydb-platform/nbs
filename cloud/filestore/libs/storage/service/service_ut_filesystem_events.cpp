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
    TTestEnv env({}, config);
    ui32 nodeIdx = env.AddDynamicNode();

    auto handler = std::make_shared<TTestFileSystemEventHandler>();
    env.GetMultiFileSystemEventHandler()->Register(fsConfig.FsId, handler);

    TServiceClient service(env.GetRuntime(), nodeIdx);
    const auto fsInfo = CreateFileSystem(service, fsConfig);

    auto headers = service.InitSession(fsConfig.FsId, "client");
    for (ui32 i = 0; i < 4; ++i) {
        service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, Sprintf("f%u", i)));
    }

    //
    // Rebooting the tablets drops all their clients: CreateNode has
    // registered the client both in the main tablet and in the shards.
    //

    const TVector<ui64> tabletIds = {
        fsInfo.MainTabletId,
        fsInfo.Shard1TabletId,
        fsInfo.Shard2TabletId};
    for (const ui64 tabletId: tabletIds) {
        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.RebootTablet();
    }

    for (const auto& fsId: fsConfig.MainAndShardIds()) {
        const auto response =
            GenerateFileSystemEvent(service, fsId, MakeEvent(42));
        UNIT_ASSERT_VALUES_EQUAL_C(0, response.GetClientCount(), fsId);
    }

    headers = service.InitSession(
        fsConfig.FsId,
        "client",
        {} /* checkpointId */,
        true /* restoreClientSession */);

    //
    // ListNodes (or ListNodesInternal) goes to the main tablet, the attrs of
    // the shard-resident nodes are fetched via GetNodeAttrBatch.
    //

    const auto listNodesResponse = service.ListNodes(headers, RootNodeId);
    const auto& nodes = listNodesResponse->Record.GetNodes();
    UNIT_ASSERT_VALUES_EQUAL(4, nodes.size());

    THashSet<ui32> shardNos;
    for (const auto& node: nodes) {
        shardNos.insert(ExtractShardNo(node.GetId()));
    }
    UNIT_ASSERT(!shardNos.contains(0));

    auto response =
        GenerateFileSystemEvent(service, fsConfig.FsId, MakeEvent(42));
    UNIT_ASSERT_VALUES_EQUAL(1, response.GetClientCount());

    const auto shardIds = fsConfig.ShardIds();
    for (const ui32 shardNo: shardNos) {
        const auto& shardId = shardIds[shardNo - 1];
        response = GenerateFileSystemEvent(service, shardId, MakeEvent(42));
        UNIT_ASSERT_VALUES_EQUAL_C(1, response.GetClientCount(), shardId);
    }

    UNIT_ASSERT_VALUES_EQUAL(1 + shardNos.size(), handler->Events.size());
    for (const auto& event: handler->Events) {
        UNIT_ASSERT_VALUES_EQUAL(fsConfig.FsId, event.GetFileSystemId());
    }
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
        TTestEnv env;
        ui32 nodeIdx = env.AddDynamicNode();

        auto handler = std::make_shared<TTestFileSystemEventHandler>();
        env.GetMultiFileSystemEventHandler()->Register("test", handler);

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);
        auto headers = service.InitSession("test", "client");

        //
        // No clients yet: the tablet has not received any of the requests
        // that register a client.
        //

        auto response =
            GenerateFileSystemEvent(service, "test", MakeEvent(42));
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetClientCount());
        UNIT_ASSERT_VALUES_EQUAL(0, handler->Events.size());

        //
        // CreateNode registers the IndexTabletProxy as a client. The flag
        // is disabled, so CreateNode itself generates no events.
        //

        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "f"));
        UNIT_ASSERT_VALUES_EQUAL(0, handler->Events.size());

        response =
            GenerateFileSystemEvent(service, "test", MakeEvent(42));
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetClientCount());
        UNIT_ASSERT_VALUES_EQUAL(1, handler->Events.size());

        const auto& event = handler->Events[0];
        UNIT_ASSERT_VALUES_EQUAL("test", event.GetFileSystemId());
        UNIT_ASSERT(event.HasInvalidateNode());
        UNIT_ASSERT(!event.HasInvalidateNodeRef());
        UNIT_ASSERT_VALUES_EQUAL(42, event.GetInvalidateNode().GetNodeId());

        response =
            GenerateFileSystemEvent(service, "test", MakeEvent(1, "a"));
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetClientCount());
        UNIT_ASSERT_VALUES_EQUAL(2, handler->Events.size());
        UNIT_ASSERT(handler->HasInvalidateNodeRef(1, "a"));
        UNIT_ASSERT(!handler->Events[1].HasInvalidateNode());

        //
        // Handlers registered for other filesystems get nothing.
        //

        auto otherHandler = std::make_shared<TTestFileSystemEventHandler>();
        env.GetMultiFileSystemEventHandler()->Register(
            "other",
            otherHandler);
        GenerateFileSystemEvent(service, "test", MakeEvent(42));
        UNIT_ASSERT_VALUES_EQUAL(3, handler->Events.size());
        UNIT_ASSERT_VALUES_EQUAL(0, otherHandler->Events.size());

        //
        // Unregistered handlers get nothing.
        //

        env.GetMultiFileSystemEventHandler()->Unregister("test", handler);
        GenerateFileSystemEvent(service, "test", MakeEvent(42));
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

        UNIT_ASSERT(handler->HasInvalidateNodeRef(RootNodeId, "f"));
        UNIT_ASSERT(!handler->HasInvalidateNode(nodeId));
        for (const auto& event: handler->Events) {
            UNIT_ASSERT_VALUES_EQUAL("test", event.GetFileSystemId());
        }

        handler->Events.clear();

        service.UnlinkNode(headers, RootNodeId, "f");

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
        service.WriteData(headers, "test", nodeId, handle, 0, TString(10, 'a'));
        UNIT_ASSERT(handler->HasInvalidateNode(nodeId));

        //
        // Overwrites the data within the file size - only MTime changes,
        // no InvalidateNode is expected.
        //

        handler->Events.clear();
        service.WriteData(headers, "test", nodeId, handle, 0, TString(5, 'b'));
        UNIT_ASSERT(!handler->HasInvalidateNode(nodeId));

        //
        // Explicit time change via SetNodeAttr - InvalidateNode is expected.
        //

        handler->Events.clear();
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
        TTestEnv env;
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
        // The rebooted tablet has no clients until it receives one of the
        // registering requests via a new pipe.
        //

        auto response =
            GenerateFileSystemEvent(service, "test", MakeEvent(42));
        UNIT_ASSERT_VALUES_EQUAL(0, response.GetClientCount());

        headers = service.InitSession(
            "test",
            "client",
            {} /* checkpointId */,
            true /* restoreClientSession */);
        service.GetNodeAttr(headers, "test", RootNodeId, "f");

        response =
            GenerateFileSystemEvent(service, "test", MakeEvent(42));
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
            MakeEvent(42));
        UNIT_ASSERT_LE(1, response.GetClientCount());
        UNIT_ASSERT_LE(1, handler->Events.size());

        //
        // Shards report the main FileSystemId so that the handlers do not
        // need to know about the shards.
        //

        for (const auto& event: handler->Events) {
            UNIT_ASSERT_VALUES_EQUAL(fsConfig.FsId, event.GetFileSystemId());
        }
        UNIT_ASSERT(handler->HasInvalidateNode(42));
    }
}

}   // namespace NCloud::NFileStore::NStorage
