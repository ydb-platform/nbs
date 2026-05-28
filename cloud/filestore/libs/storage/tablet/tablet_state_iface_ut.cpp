#include "tablet_state_iface.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>

namespace NCloud::NFileStore::NStorage {

Y_UNIT_TEST_SUITE(IIndexTabletDatabaseTest)
{
    Y_UNIT_TEST(ShouldCompressAndDecompressNodeRefs)
    {
        constexpr ui64 commitId = 1;
        constexpr ui64 nodeId = 1;
        constexpr ui64 childNodeId = 2542354;

        const TVector<TString> mainFsIds = {
            "testfilesystem-testfilesystem1234id",
            "_s_s123X",
            "_s",
            "nfs_shared"};

        auto cmpNodeRefs = [](const IIndexTabletDatabase::TNodeRef& ref1,
                              const IIndexTabletDatabase::TNodeRef& ref2)
        {
            UNIT_ASSERT_EQUAL(ref1.ShardId, ref2.ShardId);
            UNIT_ASSERT_EQUAL(ref1.ShardNodeName, ref2.ShardNodeName);
        };

        // Compress/decompress wellformed node refs
        for (const auto& mainFsId: mainFsIds) {
            const TVector<TString> shardIds = {
                mainFsId,
                mainFsId + "_s1",
                mainFsId + "_s32",
                mainFsId + "_s475"};
            for (const auto& shardId: shardIds) {
                IIndexTabletDatabase::TNodeRef ref = {
                    .NodeId = nodeId,
                    .Name = "some_name",
                    .ChildNodeId = childNodeId,
                    .ShardId = shardId,
                    .ShardNodeName = CreateGuidAsString(),
                    .MinCommitId = commitId,
                    .MaxCommitId = InvalidCommitId
                };

                IIndexTabletDatabase::TNodeRef refCopy(ref);
                UNIT_ASSERT(refCopy.TryToEncodeShardId(mainFsId));
                UNIT_ASSERT(refCopy.TryToDecodeShardId(mainFsId));
                cmpNodeRefs(ref, refCopy);
            }
        }

        // Try to compress illformed node refs
        {
            TVector<std::pair<TString, TString>> malformedShardIds = {
                {"abcd_s0", CreateGuidAsString()},
                {"abcd_s65536", CreateGuidAsString()},
                {"abcd_s655366553665536", CreateGuidAsString()},
                {"abcd_sabcedf", CreateGuidAsString()},
                {"abcd_s123", "not a guid"}};

            for (const auto& shardId: malformedShardIds) {
                IIndexTabletDatabase::TNodeRef ref = {
                    .NodeId = nodeId,
                    .Name = "some_name",
                    .ChildNodeId = childNodeId,
                    .ShardId = shardId.first,
                    .ShardNodeName = shardId.second,
                    .MinCommitId = commitId,
                    .MaxCommitId = InvalidCommitId
                };

                UNIT_ASSERT(!ref.TryToEncodeShardId("abcd"));
            }
        }

        {
            const TString binaryGuid(sizeof(TGUID::dw), ' ');
            const TString malformedGuid(sizeof(TGUID::dw) - 3, ' ');

            TVector<std::pair<TString, TString>> malformedShardIds = {
                {"\x03\x01\xFF", binaryGuid},
                {"\x01\x01", binaryGuid},
                {"\x01\x01\x02", malformedGuid}};

            for (const auto& shardId: malformedShardIds) {
                IIndexTabletDatabase::TNodeRef ref = {
                    .NodeId = nodeId,
                    .Name = "some_name",
                    .ChildNodeId = childNodeId,
                    .ShardId = shardId.first,
                    .ShardNodeName = shardId.second,
                    .MinCommitId = commitId,
                    .MaxCommitId = InvalidCommitId
                };

                UNIT_ASSERT(!ref.TryToDecodeShardId("some_fs"));
            }
        }
    }
}

}
