#include "service_ut_sharding.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/model/utils.h>
#include <cloud/filestore/libs/storage/testlib/service_client.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>
#include <cloud/filestore/private/api/protos/actions.pb.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

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

Y_UNIT_TEST_SUITE(TStorageServiceParentlessTest)
{
    Y_UNIT_TEST(ShouldCreateAndUnlinkParentlessNodesInSimpleFilestore)
    {
        NProto::TStorageConfig config;
        config.SetParentlessFilesOnly(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const TString fs = "test";
        service.CreateFileStore(fs, 1000);

        auto headers = service.InitSession(fs, "client");
        ui64 nodeId =
            service
                .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();

        // Listing the root node should not return the created file
        {
            auto response = service.ListNodes(headers, RootNodeId);
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.NodesSize());
        }

        // Creation of a directory in the parentless filestore should fail
        {
            auto response = service.AssertCreateNodeFailed(
                headers,
                TCreateNodeArgs::Directory(RootNodeId, "dir"));
            UNIT_ASSERT_C(
                response->GetError().GetCode() == E_FS_NOTSUPP,
                response->GetError().GetMessage());
        }

        ui64 handle = InvalidHandle;
        // It should be possible to create a handle for the parentless file and
        // read/write to it
        {
            // Create handle using parent + name should fail
            service.AssertCreateHandleFailed(
                headers,
                fs,
                RootNodeId,
                "file",
                TCreateHandleArgs::RDWR);
            // Create handle using nodeId should succeed
            handle = service
                         .CreateHandle(
                             headers,
                             fs,
                             nodeId,
                             "",
                             TCreateHandleArgs::RDWR)
                         ->Record.GetHandle();

            const ui64 dataSize = 100;
            const TString data = GenerateValidateData(dataSize);

            service.WriteData(headers, fs, nodeId, handle, 0, data);

            auto readResponse =
                service.ReadData(headers, fs, nodeId, handle, 0, dataSize);
            UNIT_ASSERT_VALUES_EQUAL(
                dataSize,
                readResponse->Record.GetBuffer().size());
            UNIT_ASSERT_VALUES_EQUAL(data, readResponse->Record.GetBuffer());
        }

        // Destroyign the handle should also work
        service.DestroyHandle(headers, fs, nodeId, handle);

        // Nodes count should have been incremented upon the creation of the
        // parentless file
        {
            auto stats = service.StatFileStore(headers, fs);
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                stats->Record.GetStats().GetUsedNodesCount());
        }

        // Unlinking the parentless file should succeed
        {
            auto unlinkResponse = service.UnlinkNode(headers, nodeId, "");
            UNIT_ASSERT_C(
                !unlinkResponse->GetError().GetCode(),
                unlinkResponse->GetError().GetMessage());
        }

        // Stats should be updated after unlinking the file
        {
            auto stats = service.StatFileStore(headers, fs);
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                stats->Record.GetStats().GetUsedNodesCount());
        }

        // Creating multiple parentless files should work
        const ui32 fileCount = 321;
        for (ui32 i = 0; i < fileCount; ++i) {
            service.CreateNode(
                headers,
                TCreateNodeArgs::File(RootNodeId, Sprintf("file%03d", i)));
        }

        // Stats should be updated after creating multiple files
        {
            auto stats = service.StatFileStore(headers, fs);
            UNIT_ASSERT_VALUES_EQUAL(
                fileCount,
                stats->Record.GetStats().GetUsedNodesCount());
        }

        // Listing the root node should not return any parentless files though
        {
            auto response = service.ListNodes(headers, RootNodeId);
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.NodesSize());
        }
    }

    Y_UNIT_TEST(ShouldCreateAndUnlinkParentlessNodesInShardedFilestore)
    {
        NProto::TStorageConfig config;
        TShardedFileSystemConfig fsConfig;

        config.SetMultiTabletForwardingEnabled(true);
        config.SetParentlessFilesOnly(true);
        config.SetAutomaticShardCreationEnabled(true);
        config.SetAutomaticallyCreatedShardSize(
            fsConfig.ShardBlockCount * 4_KB);
        config.SetShardAllocationUnit(fsConfig.ShardBlockCount * 4_KB);

        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        CreateFileSystem(service, fsConfig);

        auto headers = service.InitSession(fsConfig.FsId, "client");
        auto createNodeResponse =
            service
                .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record;
        Cerr << "CreateNodeResponse: " << createNodeResponse.ShortDebugString()
             << Endl;
        ui64 nodeId = createNodeResponse.GetNode().GetId();
        // Ensure that the node is created in one of the shards
        UNIT_ASSERT_VALUES_UNEQUAL(0, ExtractShardNo(nodeId));

        auto headers1 = headers;
        headers1.FileSystemId = fsConfig.Shard1Id;
        auto headers2 = headers;
        headers2.FileSystemId = fsConfig.Shard2Id;

        Cerr << "Shard1 contents: "
             << service.ListNodes(headers1, RootNodeId)
                    ->Record.ShortDebugString()
             << Endl;

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            service.ListNodes(headers, RootNodeId)->Record.NodesSize());

        service.AssertCreateNodeFailed(
            headers,
            TCreateNodeArgs::Directory(RootNodeId, "dir"));

        ui64 handle = service
                          .CreateHandle(
                              headers,
                              fsConfig.FsId,
                              nodeId,
                              "",
                              TCreateHandleArgs::RDWR)
                          ->Record.GetHandle();

        const ui64 dataSize = 100;
        const TString data = GenerateValidateData(dataSize);

        service.WriteData(headers, fsConfig.FsId, nodeId, handle, 0, data);
        auto readResponse =
            service
                .ReadData(headers, fsConfig.FsId, nodeId, handle, 0, dataSize);
        UNIT_ASSERT_VALUES_EQUAL(
            dataSize,
            readResponse->Record.GetBuffer().size());
        UNIT_ASSERT_VALUES_EQUAL(data, readResponse->Record.GetBuffer());

        service.DestroyHandle(headers, fsConfig.FsId, nodeId, handle);

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            service.StatFileStore(headers, fsConfig.FsId)
                ->Record.GetStats()
                .GetUsedNodesCount());

        // The first shard should contain the created node, but it should not be
        // accessible via listnodes method. In the main filesystem though there
        // will be no nodes, but the stats will account for all the shards
        {
            auto response = service.ListNodes(headers1, RootNodeId);
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.NodesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                service.StatFileStore(headers1, fsConfig.Shard1Id)
                    ->Record.GetStats()
                    .GetUsedNodesCount());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                service.StatFileStore(headers, fsConfig.FsId)
                    ->Record.GetStats()
                    .GetUsedNodesCount());
        }

        service.UnlinkNode(headers, nodeId, "");
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            service.StatFileStore(headers, fsConfig.FsId)
                ->Record.GetStats()
                .GetUsedNodesCount());

        const ui32 fileCount = 321;
        TVector<ui64> nodeIds;
        for (ui32 i = 0; i < fileCount; ++i) {
            nodeIds.push_back(service
                                  .CreateNode(
                                      headers,
                                      TCreateNodeArgs::File(
                                          RootNodeId,
                                          Sprintf("file%03d", i)))
                                  ->Record.GetNode()
                                  .GetId());
        }
        UNIT_ASSERT_VALUES_EQUAL(
            fileCount,
            service.StatFileStore(headers, fsConfig.FsId)
                ->Record.GetStats()
                .GetUsedNodesCount());

        // Each shard should contain at least one node
        UNIT_ASSERT_LT(
            0,
            service.StatFileStore(headers1, fsConfig.Shard1Id)
                ->Record.GetStats()
                .GetUsedNodesCount());
        UNIT_ASSERT_LT(
            0,
            service.StatFileStore(headers2, fsConfig.Shard2Id)
                ->Record.GetStats()
                .GetUsedNodesCount());

        for (ui64 nodeId: nodeIds) {
            service.UnlinkNode(headers, nodeId, "");
        }
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            service.StatFileStore(headers, fsConfig.FsId)
                ->Record.GetStats()
                .GetUsedNodesCount());
    }
}

}   // namespace NCloud::NFileStore::NStorage
