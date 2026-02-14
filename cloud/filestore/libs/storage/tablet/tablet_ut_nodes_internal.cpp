#include "tablet.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_NodesInternal)
{
    // TODO(#2674):
    // * check dupcache - retry an already completed op

    void OverrideDescribeFileStore(
        TTestActorRuntime& runtime,
        ui32 nodeIdx,
        ui64 tabletId,
        TTestActorRuntime::TEventFilter subFilter = {})
    {
        runtime.SetEventFilter([=] (auto& runtime, auto& event) {
            switch (event->GetTypeRewrite()) {
                case TEvSSProxy::EvDescribeFileStoreResponse: {
                    using TResponse = TEvSSProxy::TEvDescribeFileStoreResponse;
                    NKikimrSchemeOp::TPathDescription pd;
                    pd.MutableFileStoreDescription()->SetIndexTabletId(tabletId);
                    auto response = std::make_unique<TResponse>(
                        "some_path",
                        std::move(pd));

                    runtime.Send(new IEventHandle(
                        event->Recipient,
                        event->Sender,
                        response.release(),
                        0, // flags
                        event->Cookie), nodeIdx);

                    return true;
                }
            }

            if (subFilter) {
                return subFilter(runtime, event);
            }

            return false;
        });
    }

    TABLET_TEST_4K_ONLY(ShouldPrepareUnlinkDirectoryNodeInShard)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        const ui32 nodeIdx = env.CreateNode("nfs");
        const ui64 tabletId = env.BootIndexTablet(nodeIdx);
        OverrideDescribeFileStore(env.GetRuntime(), nodeIdx, tabletId);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        const TString uuid1 = CreateGuidAsString();
        const TString uuid2 = CreateGuidAsString();

        const ui64 dirId =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, uuid1));
        CreateNode(tablet, TCreateNodeArgs::File(dirId, uuid2));

        //
        // dir is not empty - prepare-unlink should fail.
        //

        NProtoPrivate::TRenameNodeInDestinationRequest originalRequest;
        tablet.SendPrepareUnlinkDirectoryNodeInShardRequest(
            dirId,
            originalRequest);
        {
            auto response =
                tablet.RecvPrepareUnlinkDirectoryNodeInShardResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOTEMPTY,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        //
        // dir shouldn't be locked for node-ref addition.
        //

        const TString uuid3 = CreateGuidAsString();
        tablet.SendCreateNodeRequest(TCreateNodeArgs::File(dirId, uuid3));
        {
            auto response = tablet.RecvCreateNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        //
        // Let's make the dir empty - after that prepare-unlink should succeed.
        //

        tablet.UnlinkNode(dirId, uuid2, false /* unlinkDirectory */);
        tablet.UnlinkNode(dirId, uuid3, false /* unlinkDirectory */);

        tablet.SendPrepareUnlinkDirectoryNodeInShardRequest(
            dirId,
            originalRequest);
        {
            auto response =
                tablet.RecvPrepareUnlinkDirectoryNodeInShardResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        //
        // Now dir should be locked for new node-ref addition.
        //
        // We should return retriable errors because there's a chance that the
        // unlink op will be aborted.
        //

        const TString uuid4 = CreateGuidAsString();
        tablet.SendCreateNodeRequest(TCreateNodeArgs::File(dirId, uuid4));
        {
            auto response = tablet.RecvCreateNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        const TString uuid5 = CreateGuidAsString();
        tablet.SendCreateHandleRequest(dirId, uuid5, TCreateHandleArgs::CREATE);
        {
            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        const TString shardId6 = "shard6";
        const TString uuid6 = CreateGuidAsString();
        CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, uuid6));

        const TString uuid7 = CreateGuidAsString();
        tablet.SendRenameNodeRequest(RootNodeId, uuid6, dirId, uuid7);
        {
            auto response = tablet.RecvRenameNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        const TString uuid8 = CreateGuidAsString();
        tablet.SendRenameNodeInDestinationRequest(
            dirId,
            uuid8,
            shardId6,
            uuid6);
        {
            auto response = tablet.RecvRenameNodeInDestinationResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        //
        // It should be possible to unlink dir and after that node creation
        // attempts should start returning E_FS_NOENT instead of E_REJECTED.
        //

        tablet.UnlinkNode(RootNodeId, uuid1, true /* unlinkDirectory */);

        const TString uuid9 = CreateGuidAsString();
        tablet.SendCreateNodeRequest(TCreateNodeArgs::File(dirId, uuid9));
        {
            auto response = tablet.RecvCreateNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOENT,
                response->GetStatus(),
                FormatError(response->GetError()));
        }
    }

    TABLET_TEST_4K_ONLY(ShouldAbortUnlinkDirectoryNodeInShard)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        const ui32 nodeIdx = env.CreateNode("nfs");
        const ui64 tabletId = env.BootIndexTablet(nodeIdx);
        OverrideDescribeFileStore(env.GetRuntime(), nodeIdx, tabletId);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        const TString uuid1 = CreateGuidAsString();

        const ui64 dirId =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, uuid1));

        NProtoPrivate::TRenameNodeInDestinationRequest originalRequest;
        tablet.SendPrepareUnlinkDirectoryNodeInShardRequest(
            dirId,
            originalRequest);
        {
            auto response =
                tablet.RecvPrepareUnlinkDirectoryNodeInShardResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        //
        // Now dir should be locked for new node-ref addition.
        //

        const TString uuid2 = CreateGuidAsString();
        tablet.SendCreateNodeRequest(TCreateNodeArgs::File(dirId, uuid2));
        {
            auto response = tablet.RecvCreateNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        //
        // Aborting unlink - after this the dir should become unlocked.
        //

        tablet.SendAbortUnlinkDirectoryNodeInShardRequest(
            dirId,
            originalRequest);
        {
            auto response =
                tablet.RecvAbortUnlinkDirectoryNodeInShardResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        const TString uuid3 = CreateGuidAsString();
        tablet.SendCreateNodeRequest(TCreateNodeArgs::File(dirId, uuid3));
        {
            auto response = tablet.RecvCreateNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                FormatError(response->GetError()));
        }
    }

    void DoTestShouldReturnErrorUponRenameNodeForFileToDirOp(
        const TFileSystemConfig& tabletConfig,
        bool useRenameInDestination)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetDirectoryCreationInShardsEnabled(true);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        const ui32 nodeIdx = env.CreateNode("nfs");
        const ui64 tabletId = env.BootIndexTablet(nodeIdx);
        OverrideDescribeFileStore(env.GetRuntime(), nodeIdx, tabletId);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.ConfigureShards(true);
        tablet.ReconnectPipe();
        tablet.WaitReady();
        tablet.InitSession("client", "session");

        //
        //  Scenario:
        //
        //  uuid1 -> id1 (file)
        //  name1 -> uuid1
        //
        //  uuid2 -> id2 (dir)
        //  name2 -> uuid2
        //
        //  move name1 to name2 -> fail
        //  move name2 to name1 -> ok
        //

        const TString shardId1 = "shard1";
        const TString name1 = "name1";
        const TString uuid1 = CreateGuidAsString();

        // NodeRefs should be managed by the same shard for the RenameNode test
        const TString shardId2 = useRenameInDestination ? "shard2" : shardId1;
        const TString name2 = "name2";
        const TString uuid2 = CreateGuidAsString();

        CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, uuid1));
        CreateExternalRef(tablet, RootNodeId, name1, shardId1, uuid1);

        CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, uuid2));
        CreateExternalRef(tablet, RootNodeId, name2, shardId2, uuid2);

        if (useRenameInDestination) {
            tablet.SendRenameNodeInDestinationRequest(
                RootNodeId,
                name2,
                shardId1,
                uuid1);
            auto response = tablet.RecvRenameNodeInDestinationResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_ISDIR,
                response->GetStatus(),
                FormatError(response->GetError()));

            tablet.SendRenameNodeInDestinationRequest(
                RootNodeId,
                name1,
                shardId2,
                uuid2);
            response = tablet.RecvRenameNodeInDestinationResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                FormatError(response->GetError()));
        } else {
            tablet.SendRenameNodeRequest(RootNodeId, name1, RootNodeId, name2);
            auto response = tablet.RecvRenameNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_ISDIR,
                response->GetStatus(),
                FormatError(response->GetError()));

            tablet.SendRenameNodeRequest(RootNodeId, name2, RootNodeId, name1);
            response = tablet.RecvRenameNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                FormatError(response->GetError()));
        }
    }

    TABLET_TEST_4K_ONLY(
        ShouldReturnErrorUponRenameNodeInDestinationForFileToDirOp)
    {
        DoTestShouldReturnErrorUponRenameNodeForFileToDirOp(
            tabletConfig,
            true /* useRenameInDestination */);
    }

    TABLET_TEST_4K_ONLY(ShouldReturnErrorUponRenameNodeForFileToDirOp)
    {
        DoTestShouldReturnErrorUponRenameNodeForFileToDirOp(
            tabletConfig,
            false /* useRenameInDestination */);
    }

    void DoTestShouldCheckDstEmptinessUponRenameNodeForDirToDirOp(
        const TFileSystemConfig& tabletConfig,
        bool useRenameInDestination)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetDirectoryCreationInShardsEnabled(true);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        const ui32 nodeIdx = env.CreateNode("nfs");
        const ui64 tabletId = env.BootIndexTablet(nodeIdx);
        OverrideDescribeFileStore(env.GetRuntime(), nodeIdx, tabletId);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.ConfigureShards(true);
        tablet.ReconnectPipe();
        tablet.WaitReady();
        tablet.InitSession("client", "session");

        //
        //  Scenario:
        //
        //  uuid1 -> id1 (dir)
        //  name1 -> uuid1
        //
        //  uuid2 -> id2 (dir)
        //  name2 -> uuid2
        //
        //  uuid3 -> id3 (file)
        //  name2/name3 -> uuid3
        //
        //  move name1 to name2 -> fail
        //
        //  unlink name2/name3
        //
        //  move name1 to name2 -> success
        //

        const TString shardId1 = "shard1";
        const TString name1 = "name1";
        const TString uuid1 = CreateGuidAsString();

        // NodeRefs should be managed by the same shard for the RenameNode test
        const TString shardId2 = useRenameInDestination ? "shard2" : shardId1;
        const TString name2 = "name2";
        const TString uuid2 = CreateGuidAsString();

        const TString shardId3 = "shard3";
        const TString name3 = "name3";
        const TString uuid3 = CreateGuidAsString();

        CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, uuid1));
        CreateExternalRef(tablet, RootNodeId, name1, shardId1, uuid1);

        const ui64 dir2 =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, uuid2));
        CreateExternalRef(tablet, RootNodeId, name2, shardId2, uuid2);

        CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, uuid3));
        CreateExternalRef(tablet, dir2, name3, shardId3, uuid3);

        if (useRenameInDestination) {
            tablet.SendRenameNodeInDestinationRequest(
                RootNodeId,
                name2,
                shardId1,
                uuid1);
            auto response = tablet.RecvRenameNodeInDestinationResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOTEMPTY,
                response->GetStatus(),
                FormatError(response->GetError()));
        } else {
            tablet.SendRenameNodeRequest(
                RootNodeId,
                name1,
                RootNodeId,
                name2);
            auto response = tablet.RecvRenameNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOTEMPTY,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        DeleteRef(tablet, dir2, name3);

        if (useRenameInDestination) {
            tablet.SendRenameNodeInDestinationRequest(
                RootNodeId,
                name2,
                shardId1,
                uuid1);
            auto response = tablet.RecvRenameNodeInDestinationResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                FormatError(response->GetError()));
        } else {
            tablet.SendRenameNodeRequest(RootNodeId, name1, RootNodeId, name2);
            auto response = tablet.RecvRenameNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        const auto nodeRef = tablet.UnsafeGetNodeRef(RootNodeId, name2)->Record;
        UNIT_ASSERT_VALUES_EQUAL(shardId1, nodeRef.GetShardId());
        UNIT_ASSERT_VALUES_EQUAL(uuid1, nodeRef.GetShardNodeName());
    }

    TABLET_TEST_4K_ONLY(
        ShouldCheckDstEmptinessUponRenameNodeInDestinationForDirToDirOp)
    {
        DoTestShouldCheckDstEmptinessUponRenameNodeForDirToDirOp(
            tabletConfig,
            true /* useRenameInDestination */);
    }

    TABLET_TEST_4K_ONLY(ShouldCheckDstEmptinessUponRenameNodeForDirToDirOp)
    {
        DoTestShouldCheckDstEmptinessUponRenameNodeForDirToDirOp(
            tabletConfig,
            false /* useRenameInDestination */);
    }

    void DoTestShouldAbortUnlinkUponRenameNodeForDirToDirOp(
        const TFileSystemConfig& tabletConfig,
        bool useRenameInDestination)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetDirectoryCreationInShardsEnabled(true);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        const ui32 nodeIdx = env.CreateNode("nfs");
        const ui64 tabletId = env.BootIndexTablet(nodeIdx);

        IEventHandlePtr abortRequest;
        bool shouldIntercept = true;
        ui64 opLogEntryId = 0;
        auto interceptor = [&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvIndexTablet::EvAbortUnlinkDirectoryNodeInShardRequest: {
                    if (shouldIntercept) {
                        abortRequest.reset(event.Release());
                        return true;
                    }
                    break;
                }

                case TEvIndexTabletPrivate::EvWriteOpLogEntryResponse: {
                    using TResponse =
                        TEvIndexTabletPrivate::TEvWriteOpLogEntryResponse;
                    opLogEntryId =
                        event->template Get<TResponse>()->OpLogEntryId;
                    break;
                }
            }

            return false;
        };

        OverrideDescribeFileStore(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            interceptor);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.ConfigureShards(true);
        tablet.ReconnectPipe();
        tablet.WaitReady();
        tablet.InitSession("client", "session");

        //
        //  Scenario:
        //
        //  uuid1 -> id1 (file)
        //  name1 -> uuid1
        //
        //  uuid2 -> id2 (dir)
        //  name2 -> uuid2
        //
        //  move name1 to name2 -> fail, abort initiated
        //
        //  check that no files can be created under dir
        //  check that the dir itself can't be moved
        //
        //  abort completes, now files can be created
        //
        //  create file under dir -> success
        //

        const TString shardId1 = "shard1";
        const TString name1 = "name1";
        const TString uuid1 = CreateGuidAsString();

        // NodeRefs should be managed by the same shard for the RenameNode test
        const TString shardId2 = useRenameInDestination ? "shard2" : shardId1;
        const TString name2 = "name2";
        const TString uuid2 = CreateGuidAsString();

        CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, uuid1));
        CreateExternalRef(tablet, RootNodeId, name1, shardId1, uuid1);

        const ui64 dirId =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, uuid2));
        CreateExternalRef(tablet, RootNodeId, name2, shardId2, uuid2);

        if (useRenameInDestination) {
            tablet.SendRenameNodeInDestinationRequest(
                RootNodeId,
                name2,
                shardId1,
                uuid1);
        } else {
            tablet.SendRenameNodeRequest(RootNodeId, name1, RootNodeId, name2);
        }

        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(1));
        UNIT_ASSERT(abortRequest);

        //
        // Now dir should be locked for new node-ref addition and locked for
        // RenameNode.
        //

        const TString uuid3 = CreateGuidAsString();
        tablet.SendCreateNodeRequest(TCreateNodeArgs::File(dirId, uuid3));
        {
            auto response = tablet.RecvCreateNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        tablet.SendRenameNodeRequest(
            RootNodeId,
            name2,
            RootNodeId,
            name2 + ".moved");
        {
            auto response = tablet.RecvRenameNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        {
            auto opLogEntryResponse = tablet.GetOpLogEntry(opLogEntryId);
            UNIT_ASSERT_C(
                opLogEntryResponse->OpLogEntry.Defined(),
                opLogEntryId);
            const auto& entry = *opLogEntryResponse->OpLogEntry;
            const auto& abortRequest =
                entry.GetAbortUnlinkDirectoryNodeInShardRequest();
            UNIT_ASSERT_VALUES_EQUAL(dirId, abortRequest.GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL(shardId2, abortRequest.GetFileSystemId());
        }

        //
        // Releasing the intercepted event.
        //

        shouldIntercept = false;
        env.GetRuntime().Send(abortRequest.release(), nodeIdx);

        if (useRenameInDestination) {
            auto response = tablet.RecvRenameNodeInDestinationResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_ISDIR,
                response->GetStatus(),
                FormatError(response->GetError()));
        } else {
            auto response = tablet.RecvRenameNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_ISDIR,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        //
        // Abort OpLogEntry should be deleted.
        //

        {
            auto opLogEntryResponse = tablet.GetOpLogEntry(opLogEntryId);
            UNIT_ASSERT_C(
                !opLogEntryResponse->OpLogEntry.Defined(),
                opLogEntryResponse->OpLogEntry->ShortUtf8DebugString());
        }

        //
        // Now dir should be unlocked.
        //

        tablet.SendCreateNodeRequest(TCreateNodeArgs::File(dirId, uuid3));
        {
            auto response = tablet.RecvCreateNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                FormatError(response->GetError()));
        }
    }

    TABLET_TEST_4K_ONLY(
        ShouldAbortUnlinkUponRenameNodeInDestinationForDirToDirOp)
    {
        DoTestShouldAbortUnlinkUponRenameNodeForDirToDirOp(
            tabletConfig,
            true /* useRenameInDestination */);
    }

    TABLET_TEST_4K_ONLY(ShouldAbortUnlinkUponRenameNodeForDirToDirOp)
    {
        DoTestShouldAbortUnlinkUponRenameNodeForDirToDirOp(
            tabletConfig,
            false /* useRenameInDestination */);
    }

    void DoTestShouldAbortUnlinkUponRenameNodeForDirToDirOpAfterReboot(
        const TFileSystemConfig& tabletConfig,
        bool useRenameInDestination)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetDirectoryCreationInShardsEnabled(true);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        const ui32 nodeIdx = env.CreateNode("nfs");
        const ui64 tabletId = env.BootIndexTablet(nodeIdx);

        IEventHandlePtr abortRequest;
        bool shouldIntercept = true;
        ui64 opLogEntryId = 0;
        auto interceptor = [&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvIndexTablet::EvAbortUnlinkDirectoryNodeInShardRequest: {
                    if (shouldIntercept) {
                        abortRequest.reset(event.Release());
                        return true;
                    }
                    break;
                }

                case TEvIndexTabletPrivate::EvWriteOpLogEntryResponse: {
                    using TResponse =
                        TEvIndexTabletPrivate::TEvWriteOpLogEntryResponse;
                    opLogEntryId =
                        event->template Get<TResponse>()->OpLogEntryId;
                    break;
                }
            }

            return false;
        };

        OverrideDescribeFileStore(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            interceptor);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.ConfigureShards(true);
        tablet.ReconnectPipe();
        tablet.WaitReady();
        tablet.InitSession("client", "session");

        //
        //  Scenario:
        //
        //  uuid1 -> id1 (file)
        //  name1 -> uuid1
        //
        //  uuid2 -> id2 (dir)
        //  name2 -> uuid2
        //
        //  move name1 to name2 -> fail, abort initiated
        //
        //  check that no files can be created under dir
        //  check that the dir itself can't be moved
        //
        //  reboot tablet
        //
        //  abort should complete, now files can be created
        //
        //  create file under dir -> success
        //

        const TString shardId1 = "shard1";
        const TString name1 = "name1";
        const TString uuid1 = CreateGuidAsString();

        // NodeRefs should be managed by the same shard for the RenameNode test
        const TString shardId2 = useRenameInDestination ? "shard2" : shardId1;
        const TString name2 = "name2";
        const TString uuid2 = CreateGuidAsString();

        CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, uuid1));
        CreateExternalRef(tablet, RootNodeId, name1, shardId1, uuid1);

        const ui64 dirId =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, uuid2));
        CreateExternalRef(tablet, RootNodeId, name2, shardId2, uuid2);

        if (useRenameInDestination) {
            tablet.SendRenameNodeInDestinationRequest(
                RootNodeId,
                name2,
                shardId1,
                uuid1);
        } else {
            tablet.SendRenameNodeRequest(
                RootNodeId,
                name1,
                RootNodeId,
                name2);
        }

        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(1));
        UNIT_ASSERT(abortRequest);

        //
        // Now dir should be locked for new node-ref addition and locked for
        // RenameNode.
        //

        const TString uuid3 = CreateGuidAsString();
        tablet.SendCreateNodeRequest(TCreateNodeArgs::File(dirId, uuid3));
        {
            auto response = tablet.RecvCreateNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        tablet.SendRenameNodeRequest(
            RootNodeId,
            name2,
            RootNodeId,
            name2 + ".moved");
        {
            auto response = tablet.RecvRenameNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        {
            auto opLogEntryResponse = tablet.GetOpLogEntry(opLogEntryId);
            UNIT_ASSERT_C(
                opLogEntryResponse->OpLogEntry.Defined(),
                opLogEntryId);
            const auto& entry = *opLogEntryResponse->OpLogEntry;
            const auto& abortRequest =
                entry.GetAbortUnlinkDirectoryNodeInShardRequest();
            UNIT_ASSERT_VALUES_EQUAL(dirId, abortRequest.GetNodeId());
            UNIT_ASSERT_VALUES_EQUAL(shardId2, abortRequest.GetFileSystemId());
        }

        //
        // Rebooting - Abort request should be re-sent upon tablet start.
        //

        shouldIntercept = false;
        tablet.RebootTablet();
        tablet.ReconnectPipe();
        tablet.WaitReady();
        tablet.RecoverSession();

        //
        // E_REJECTED is expected because tablet actor died.
        //

        if (useRenameInDestination) {
            auto response = tablet.RecvRenameNodeInDestinationResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                FormatError(response->GetError()));
        } else {
            auto response = tablet.RecvRenameNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                FormatError(response->GetError()));
        }

        //
        // Abort OpLogEntry should be deleted.
        //

        {
            auto opLogEntryResponse = tablet.GetOpLogEntry(opLogEntryId);
            UNIT_ASSERT_C(
                !opLogEntryResponse->OpLogEntry.Defined(),
                opLogEntryResponse->OpLogEntry->ShortUtf8DebugString());
        }

        //
        // Now dir should be unlocked.
        //

        tablet.SendCreateNodeRequest(TCreateNodeArgs::File(dirId, uuid3));
        {
            auto response = tablet.RecvCreateNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                FormatError(response->GetError()));
        }
    }

    TABLET_TEST_4K_ONLY(
        ShouldAbortUnlinkUponRenameNodeInDestinationForDirToDirOpAfterReboot)
    {
        DoTestShouldAbortUnlinkUponRenameNodeForDirToDirOpAfterReboot(
            tabletConfig,
            true /* useRenameInDestination */);
    }

    TABLET_TEST_4K_ONLY(ShouldAbortUnlinkUponRenameNodeForDirToDirOpAfterReboot)
    {
        DoTestShouldAbortUnlinkUponRenameNodeForDirToDirOpAfterReboot(
            tabletConfig,
            false /* useRenameInDestination */);
    }

    TABLET_TEST_4K_ONLY(
        ShouldReturnErrorUponRenameNodeInDestinationForLocalNode)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        const ui32 nodeIdx = env.CreateNode("nfs");

        const ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "file"));

        tablet.SendRenameNodeInDestinationRequest(
            RootNodeId,
            "file",
            "shard",
            CreateGuidAsString());
        auto response = tablet.RecvRenameNodeInDestinationResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response->GetStatus(),
            FormatError(response->GetError()));
    }

    TABLET_TEST_4K_ONLY(ShouldHandleCommitIdOverflowUponRenameNodeInDestination)
    {
        const ui32 maxTabletStep = 5;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxTabletStep(maxTabletStep);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        const ui32 nodeIdx = env.CreateNode("nfs");

        TTabletRebootTracker rebootTracker;
        env.GetRuntime().SetEventFilter(rebootTracker.GetEventFilter());

        const ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto dir =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "dir"));

        auto reconnectIfNeeded = [&]()
        {
            if (rebootTracker.IsPipeDestroyed()) {
                tablet.ReconnectPipe();
                tablet.WaitReady();
                tablet.RecoverSession();
                rebootTracker.ClearPipeDestroyed();
            }
        };

        ui32 failures = 0;

        TVector<TString> names;
        TVector<NProto::TNodeAttr> nodes;

        for (ui32 i = 0; i < 10; ++i) {
            auto fileName = TStringBuilder() << "file" << i;
            auto shardId = TStringBuilder() << "shard" << i;
            auto shardNodeName = CreateGuidAsString();

            tablet.SendRenameNodeInDestinationRequest(
                dir,
                fileName,
                shardId,
                shardNodeName);
            auto response = tablet.RecvRenameNodeInDestinationResponse();
            reconnectIfNeeded();

            if (HasError(response->GetError())) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_REJECTED,
                    response->GetStatus(),
                    FormatError(response->GetError()));
                ++failures;
                continue;
            }

            names.push_back(std::move(fileName));
            NProto::TNodeAttr node;
            node.SetShardFileSystemId(shardId);
            node.SetShardNodeName(shardNodeName);
            nodes.push_back(std::move(node));
        }

        auto response = tablet.ListNodes(dir);
        const auto& listedNames = response->Record.GetNames();
        const auto& listedNodes = response->Record.GetNodes();
        UNIT_ASSERT_VALUES_EQUAL(names.size(), listedNames.size());
        UNIT_ASSERT_VALUES_EQUAL(nodes.size(), listedNodes.size());
        for (ui32 i = 0; i < names.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(names[i], listedNames[i]);
            UNIT_ASSERT_VALUES_EQUAL(
                nodes[i].ShortUtf8DebugString(),
                listedNodes[i].ShortUtf8DebugString());
        }

        UNIT_ASSERT_C(
            rebootTracker.GetGenerationCount() >= 2,
            "Expected at least 2 different generations due to tablet reboot, "
            "got "
                << rebootTracker.GetGenerationCount());

        UNIT_ASSERT_GT(failures, 0);
    }

    TABLET_TEST_4K_ONLY(
        ShouldHandleCommitIdOverflowUponPrepareRenameNodeInSource)
    {
        const ui32 maxTabletStep = 5;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxTabletStep(maxTabletStep);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        const ui32 nodeIdx = env.CreateNode("nfs");

        TTabletRebootTracker rebootTracker;
        env.GetRuntime().SetEventFilter(rebootTracker.GetEventFilter());

        const ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto dir =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "dir"));

        const ui64 dstDir = 11111;

        auto reconnectIfNeeded = [&]()
        {
            if (rebootTracker.IsPipeDestroyed()) {
                tablet.ReconnectPipe();
                tablet.WaitReady();
                tablet.RecoverSession();
                rebootTracker.ClearPipeDestroyed();
            }
        };

        ui32 failures = 0;

        const TString shardId = "shard";
        const TString shardNodeName = CreateGuidAsString();

        //
        // Pre-creating N fake node-refs to apply the rename op to.
        //

        const ui32 iterations = 10;
        TVector<TString> fileNames;

        for (ui32 i = 0; i < iterations;) {
            auto fileName = TStringBuilder() << "file" << i;
            tablet.SendUnsafeCreateNodeRefRequest(
                dir,
                fileName,
                0 /* childId */,
                shardId,
                shardNodeName);
            auto response = tablet.RecvUnsafeCreateNodeRefResponse();
            reconnectIfNeeded();
            if (HasError(response->GetError())) {
                continue;
            }

            fileNames.push_back(fileName);
            ++i;
        }

        //
        // Calling PrepareRenameNodeInSource for each of those fake node-refs.
        // Comparing the response to the expected values as well, not just
        // checking CommitIdOverflow processing logic.
        //

        for (ui32 i = 0; i < iterations; ++i) {
            auto newFileName = TStringBuilder() << "new-file" << i;
            auto dstShardId = TStringBuilder() << "shard" << i;

            auto renameNodeRequest = tablet.CreateRenameNodeRequest(
                dir,
                fileNames[i],
                dstDir,
                newFileName);

            tablet.SendPrepareRenameNodeInSourceRequest(
                renameNodeRequest->Record,
                dstShardId);
            auto response = tablet.RecvPrepareRenameNodeInSourceResponse();
            reconnectIfNeeded();

            if (HasError(response->GetError())) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_REJECTED,
                    response->GetStatus(),
                    FormatError(response->GetError()));
                ++failures;
                continue;
            }

            auto opLogEntryResponse = tablet.GetOpLogEntry(response->OpLogEntryId);
            UNIT_ASSERT_C(
                opLogEntryResponse->OpLogEntry.Defined(),
                response->OpLogEntryId);
            const auto& entry = *opLogEntryResponse->OpLogEntry;
            const auto& dstRequest = entry.GetRenameNodeInDestinationRequest();
            UNIT_ASSERT_VALUES_EQUAL(
                renameNodeRequest->Record.GetNewParentId(),
                dstRequest.GetNewParentId());
            UNIT_ASSERT_VALUES_EQUAL(
                renameNodeRequest->Record.GetNewName(),
                dstRequest.GetNewName());
            UNIT_ASSERT_VALUES_EQUAL(dstShardId, dstRequest.GetFileSystemId());
            UNIT_ASSERT_VALUES_EQUAL(
                shardId,
                dstRequest.GetSourceNodeShardId());
            UNIT_ASSERT_VALUES_EQUAL(
                shardNodeName,
                dstRequest.GetSourceNodeShardNodeName());

            //
            // Deleting OpLogEntry so that the tablet doesn't try to replay it
            // upon restart. The request can't be replayed because dst shard
            // doesn't exist.
            //

            tablet.DeleteOpLogEntry(response->OpLogEntryId);
        }

        UNIT_ASSERT_C(
            rebootTracker.GetGenerationCount() >= 2,
            "Expected at least 2 different generations due to tablet reboot, "
            "got "
                << rebootTracker.GetGenerationCount());

        UNIT_ASSERT_GT(failures, 0);
    }

    Y_UNIT_TEST(ShouldHandleCommitIdOverflowInUnsafeNodeOperations)
    {
        const ui32 maxTabletStep = 4;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxTabletStep(maxTabletStep);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TTabletRebootTracker rebootTracker;
        env.GetRuntime().SetEventFilter(rebootTracker.GetEventFilter());

        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        auto reconnectIfNeeded = [&]()
        {
            if (rebootTracker.IsPipeDestroyed()) {
                tablet.ReconnectPipe();
                tablet.WaitReady();
                tablet.RecoverSession();
                rebootTracker.ClearPipeDestroyed();
            }
        };

        tablet.InitSession("client", "session");

        TVector<ui64> nodeIds;

        bool createNodeFailed = false;

        while (nodeIds.size() < 5) {
            const ui64 nodeId = 2 + nodeIds.size();
            tablet.SendUnsafeCreateNodeRequest(nodeId, nodeIds.size() * 100);
            auto response = tablet.RecvUnsafeCreateNodeResponse();
            reconnectIfNeeded();

            if (HasError(response->GetError())) {
                UNIT_ASSERT_VALUES_EQUAL(
                    E_REJECTED,
                    response->GetError().GetCode());
                createNodeFailed = true;
                continue;
            }

            auto getResponse = tablet.GetNodeAttr(nodeId);
            UNIT_ASSERT(getResponse);
            UNIT_ASSERT_VALUES_EQUAL(
                nodeIds.size() * 100,
                getResponse->Record.GetNode().GetSize());
            nodeIds.push_back(nodeId);
        }

        UNIT_ASSERT(createNodeFailed);

        bool updateNodeFailed = false;

        for (ui64 i = 0; i < nodeIds.size();) {
            tablet.SendUnsafeUpdateNodeRequest(nodeIds[i], i * 200);
            auto response = tablet.RecvUnsafeUpdateNodeResponse();
            reconnectIfNeeded();

            if (HasError(response->GetError())) {
                UNIT_ASSERT_VALUES_EQUAL(
                    E_REJECTED,
                    response->GetError().GetCode());
                updateNodeFailed = true;
            } else {
                auto getResponse = tablet.GetNodeAttr(nodeIds[i]);
                UNIT_ASSERT(getResponse);
                UNIT_ASSERT_VALUES_EQUAL(
                    i * 200,
                    getResponse->Record.GetNode().GetSize());
                ++i;
            }
        }

        UNIT_ASSERT(updateNodeFailed);

        bool deleteNodeFailed = false;

        for (ui64 i = 0; i < nodeIds.size(); ++i) {
            tablet.SendUnsafeDeleteNodeRequest(nodeIds[i]);
            auto deleteResponse = tablet.RecvUnsafeDeleteNodeResponse();
            reconnectIfNeeded();

            if (!HasError(deleteResponse->GetError())) {
                tablet.SendUnsafeGetNodeRequest(nodeIds[i]);
                auto getResponse = tablet.RecvUnsafeGetNodeResponse();
                UNIT_ASSERT(HasError(getResponse->GetError()));
            } else {
                deleteNodeFailed = true;
            }
        }

        UNIT_ASSERT(deleteNodeFailed);

        UNIT_ASSERT_C(
            rebootTracker.GetGenerationCount() >= 3,
            "Expected at least 3 different generations due to tablet reboots");
    }

    TABLET_TEST_4K_ONLY(
        ShouldHandleCommitIdOverflowUponCommitRenameNodeInSource)
    {
        const ui32 maxTabletStep = 5;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxTabletStep(maxTabletStep);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        const ui32 nodeIdx = env.CreateNode("nfs");

        TTabletRebootTracker rebootTracker;
        env.GetRuntime().SetEventFilter(rebootTracker.GetEventFilter());

        const ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto dir =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "dir"));

        const ui64 dstDir = 11111;

        auto reconnectIfNeeded = [&]()
        {
            if (rebootTracker.IsPipeDestroyed()) {
                tablet.ReconnectPipe();
                tablet.WaitReady();
                tablet.RecoverSession();
                rebootTracker.ClearPipeDestroyed();
            }
        };

        ui32 failures = 0;

        const TString shardId = "shard";
        const TString shardNodeName = CreateGuidAsString();
        const ui64 opLogEntryId = Max<ui64>();

        //
        // Pre-creating N fake node-refs to apply the rename op to.
        //

        const ui32 iterations = 10;
        TVector<TString> fileNames;

        for (ui32 i = 0; i < iterations;) {
            auto fileName = TStringBuilder() << "file" << i;
            tablet.SendUnsafeCreateNodeRefRequest(
                dir,
                fileName,
                0 /* childId */,
                shardId,
                shardNodeName);
            auto response = tablet.RecvUnsafeCreateNodeRefResponse();
            reconnectIfNeeded();
            if (HasError(response->GetError())) {
                continue;
            }

            fileNames.push_back(fileName);
            ++i;
        }

        //
        // Calling CommitRenameNodeInSource for each of those fake node-refs.
        // Comparing the response to the expected values as well, not just
        // checking CommitIdOverflow processing logic.
        //

        for (ui32 i = 0; i < iterations; ++i) {
            auto newFileName = TStringBuilder() << "new-file" << i;
            auto dstShardId = TStringBuilder() << "shard" << i;

            auto renameNodeRequest = tablet.CreateRenameNodeRequest(
                dir,
                fileNames[i],
                dstDir,
                newFileName);

            NProtoPrivate::TRenameNodeInDestinationResponse subResponse;

            tablet.SendCommitRenameNodeInSourceRequest(
                renameNodeRequest->Record,
                subResponse,
                opLogEntryId);
            auto response = tablet.RecvCommitRenameNodeInSourceResponse();
            reconnectIfNeeded();

            if (HasError(response->GetError())) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_REJECTED,
                    response->GetStatus(),
                    FormatError(response->GetError()));
                ++failures;
                continue;
            }

            tablet.SendUnsafeGetNodeRefRequest(dir, fileNames[i]);
            auto getNodeRefResponse = tablet.RecvUnsafeGetNodeRefResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOENT,
                getNodeRefResponse->GetStatus(),
                FormatError(getNodeRefResponse->GetError()));
        }

        UNIT_ASSERT_C(
            rebootTracker.GetGenerationCount() >= 2,
            "Expected at least 2 different generations due to tablet reboot, "
            "got "
                << rebootTracker.GetGenerationCount());

        UNIT_ASSERT_GT(failures, 0);
    }

    TABLET_TEST_4K_ONLY(ShouldHandleCommitIdOverflowUponCompleteUnlinkNode)
    {
        const ui32 maxTabletStep = 5;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxTabletStep(maxTabletStep);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        const ui32 nodeIdx = env.CreateNode("nfs");

        TTabletRebootTracker rebootTracker;
        env.GetRuntime().SetEventFilter(rebootTracker.GetEventFilter());

        const ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto dir =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "dir"));

        auto reconnectIfNeeded = [&]()
        {
            if (rebootTracker.IsPipeDestroyed()) {
                tablet.ReconnectPipe();
                tablet.WaitReady();
                tablet.RecoverSession();
                rebootTracker.ClearPipeDestroyed();
            }
        };

        ui32 failures = 0;

        const TString shardId = "shard";
        const ui64 opLogEntryId = Max<ui64>();

        //
        // Pre-creating N fake external node-refs to apply the unlink op to.
        //

        const ui32 iterations = 5;
        TVector<TString> fileNames;

        for (ui32 i = 0; i < iterations;) {
            auto fileName = TStringBuilder() << "file" << i;
            auto shardNodeName = CreateGuidAsString();
            tablet.SendUnsafeCreateNodeRefRequest(
                dir,
                fileName,
                0 /* childId */,
                shardId,
                shardNodeName);
            auto response = tablet.RecvUnsafeCreateNodeRefResponse();
            reconnectIfNeeded();
            if (HasError(response->GetError())) {
                continue;
            }

            fileNames.push_back(fileName);
            ++i;
        }

        for (ui32 i = 0; i < iterations; ++i) {
            auto unlinkNodeRequest = tablet.CreateUnlinkNodeRequest(
                dir,
                fileNames[i],
                false /* unlinkDirectory */);

            NProto::TUnlinkNodeResponse unlinkResponse;

            tablet.SendCompleteUnlinkNodeRequest(
                unlinkNodeRequest->Record,
                unlinkResponse,
                opLogEntryId);
            auto response = tablet.RecvCompleteUnlinkNodeResponse();
            reconnectIfNeeded();

            if (HasError(response->GetError())) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_REJECTED,
                    response->GetStatus(),
                    FormatError(response->GetError()));
                ++failures;
                continue;
            }

            tablet.SendUnsafeGetNodeRefRequest(dir, fileNames[i]);
            auto getNodeRefResponse = tablet.RecvUnsafeGetNodeRefResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOENT,
                getNodeRefResponse->GetStatus(),
                FormatError(getNodeRefResponse->GetError()));
        }

        UNIT_ASSERT_C(
            rebootTracker.GetGenerationCount() >= 3,
            "Expected at least 3 different generations due to tablet reboot, "
            "got "
                << rebootTracker.GetGenerationCount());

        UNIT_ASSERT_GT(failures, 0);
    }
}

}   // namespace NCloud::NFileStore::NStorage
