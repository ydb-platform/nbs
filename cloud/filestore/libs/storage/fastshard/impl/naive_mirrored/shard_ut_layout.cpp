#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>
#include <cloud/filestore/libs/storage/fastshard/impl/naive_mirrored/shard.h>
#include <cloud/filestore/private/api/unsafe_protos/unsafe.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/size_literals.h>
#include <util/stream/str.h>

#include <gtest/gtest.h>

using namespace NCloud;
using namespace NFileStore;
using namespace NStorage::NFastShard;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 ShardNo = 1;
constexpr size_t NodesPerGroup = 64;

////////////////////////////////////////////////////////////////////////////////
// The layout dump does no IO, so a storage group whose methods do
// nothing is enough.

struct TNullStorageGroup: IStorageGroup
{
    NCloud::NProto::TError AcquireDevices() override
    {
        return {};
    }

    NCloud::NProto::TError ReleaseDevices() override
    {
        return {};
    }

    NCloud::NProto::TError WriteLogRecord(
        NCloud::NProto::TDeviceRequestHeaders headers,
        TVector<TPageGroup> pageGroups) override
    {
        Y_UNUSED(headers, pageGroups);

        return {};
    }

    NCloud::NProto::TError ReadPages(
        NCloud::NProto::TDeviceRequestHeaders headers,
        const TVector<TPageGroupRef>& pageGroupRefs,
        TVector<TPageGroup>* pageGroups) override
    {
        Y_UNUSED(headers, pageGroupRefs, pageGroups);

        return {};
    }
};

struct TNullStorageGroupFactory: IStorageGroupFactory
{
    IStorageGroupPtr MakeStorageGroup(
        const NProtoPrivate::TPersistentFastShardConfig& config) override
    {
        Y_UNUSED(config);

        return std::make_shared<TNullStorageGroup>();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TStorageFixture
{
    NProtoPrivate::TPersistentFastShardConfig Config;
    std::shared_ptr<TNullStorageGroupFactory> Factory =
        std::make_shared<TNullStorageGroupFactory>();

    TStorageFixture()
    {
        Config.SetNodesPerGroup(NodesPerGroup);
        Config.SetExpectedGroupCapacity(64_MB);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(NaiveMirroredShardLayoutTest, DumpsLayout)
{
    TStorageFixture fx;

    auto shard =
        CreateNaiveMirroredFileSystemShard(ShardNo, fx.Factory, fx.Config);

    TStringStream json;
    shard->DumpLayoutJson(json);

    NJson::TJsonValue parsed;
    ASSERT_TRUE(NJson::ReadJsonTree(json.Str(), &parsed)) << json.Str();
    const auto& components = parsed["components"].GetArray();
    ASSERT_EQ(6u, components.size()) << json.Str();

    //
    // The components must cover the group contiguously in the
    // documented order, starting at offset 0.
    //

    const TVector<TString> expectedNames = {
        "NodeTable",
        "NameTable",
        "HandleTable",
        "PageIndex",
        "PageAllocatorBitmap",
        "DataPages",
    };

    ui64 expectedOffset = 0;
    for (ui32 i = 0; i < components.size(); ++i) {
        const auto& c = components[i];
        EXPECT_EQ(expectedNames[i], c["name"].GetString());
        EXPECT_EQ(expectedOffset, c["offsetBytes"].GetUInteger())
            << c["name"].GetString();
        EXPECT_GT(c["sizeBytes"].GetUInteger(), 0u)
            << c["name"].GetString();
        EXPECT_GT(c["slotCount"].GetUInteger(), 0u)
            << c["name"].GetString();
        expectedOffset += c["sizeBytes"].GetUInteger();
    }

    TStringStream html;
    shard->DumpLayoutHtml(html);

    EXPECT_TRUE(html.Str().Contains("Fast Shard Layout")) << html.Str();
    for (const auto& name: expectedNames) {
        EXPECT_TRUE(html.Str().Contains("<td>" + name + "</td>"))
            << html.Str();
    }

    //
    // Every template variable must have been substituted.
    //

    EXPECT_FALSE(html.Str().Contains("{{")) << html.Str();
}
