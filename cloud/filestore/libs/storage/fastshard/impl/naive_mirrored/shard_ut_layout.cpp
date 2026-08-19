#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>
#include <cloud/filestore/libs/storage/fastshard/impl/naive_mirrored/shard.h>
#include <cloud/filestore/private/api/unsafe_protos/unsafe.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/size_literals.h>
#include <util/stream/str.h>
#include <util/string/cast.h>

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

        //
        // The null storage group factory ignores the devices; they are
        // here so that the layout dump has something to display.
        //

        auto* group = Config.AddStorageGroups();
        group->SetType(NProtoPrivate::TStorageGroup::E_SG_MIRROR);
        for (ui32 i = 1; i <= 3; ++i) {
            auto* device = group->AddDevices();
            device->SetHost("host-" + ToString(i));
            device->SetPort(29900 + i);
            device->SetDeviceId("device-" + ToString(i));
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(NaiveMirroredShardLayoutTest, DumpsLayout)
{
    TStorageFixture fx;

    auto shard =
        CreateNaiveMirroredFileSystemShard(ShardNo, fx.Factory, fx.Config);

    //
    // Json test.
    //

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

    {
        const auto& c = components[0];
        EXPECT_EQ("NodeTable", c["name"].GetString());
        EXPECT_EQ(0ULL, c["offsetBytes"].GetUInteger());
        EXPECT_EQ(8_KB, c["sizeBytes"].GetUInteger());
        EXPECT_EQ(84ULL, c["slotCount"].GetUInteger());
    }

    {
        const auto& c = components[1];
        EXPECT_EQ("NameTable", c["name"].GetString());
        EXPECT_EQ(8_KB, c["offsetBytes"].GetUInteger());
        EXPECT_EQ(4_KB, c["sizeBytes"].GetUInteger());
        EXPECT_EQ(85ULL, c["slotCount"].GetUInteger());
    }

    {
        const auto& c = components[2];
        EXPECT_EQ("HandleTable", c["name"].GetString());
        EXPECT_EQ(12_KB, c["offsetBytes"].GetUInteger());
        EXPECT_EQ(12_KB, c["sizeBytes"].GetUInteger());
        EXPECT_EQ(768ULL, c["slotCount"].GetUInteger());
    }

    {
        const auto& c = components[3];
        EXPECT_EQ("PageIndex", c["name"].GetString());
        EXPECT_EQ(24_KB, c["offsetBytes"].GetUInteger());
        EXPECT_EQ(52_KB, c["sizeBytes"].GetUInteger());
        EXPECT_EQ(2210ULL, c["slotCount"].GetUInteger());
    }

    {
        const auto& c = components[4];
        EXPECT_EQ("PageAllocatorBitmap", c["name"].GetString());
        EXPECT_EQ(76_KB, c["offsetBytes"].GetUInteger());
        EXPECT_EQ(4_KB, c["sizeBytes"].GetUInteger());
        EXPECT_EQ(32768ULL, c["slotCount"].GetUInteger());
    }

    {
        const auto& c = components[5];
        EXPECT_EQ("DataPages", c["name"].GetString());
        EXPECT_EQ(96_KB, c["offsetBytes"].GetUInteger());
        EXPECT_EQ(1_GB, c["sizeBytes"].GetUInteger());
        EXPECT_EQ(32768ULL, c["slotCount"].GetUInteger());
    }

    //
    // Storage groups mirror the config, with every device field.
    //

    {
        const auto& groups = parsed["storageGroups"].GetArray();
        ASSERT_EQ(1u, groups.size()) << json.Str();
        EXPECT_EQ("E_SG_MIRROR", groups[0]["type"].GetString());

        const auto& devices = groups[0]["devices"].GetArray();
        ASSERT_EQ(3u, devices.size()) << json.Str();
        for (ui32 i = 1; i <= 3; ++i) {
            const auto& d = devices[i - 1];
            EXPECT_EQ("host-" + ToString(i), d["host"].GetString());
            EXPECT_EQ(29900 + i, d["port"].GetUInteger());
            EXPECT_EQ("device-" + ToString(i), d["deviceId"].GetString());
        }
    }

    //
    // Html test.
    //

    const TVector<TString> expectedNames = {
        "NodeTable",
        "NameTable",
        "HandleTable",
        "PageIndex",
        "PageAllocatorBitmap",
        "DataPages",
    };

    TStringStream html;
    shard->DumpLayoutHtml(html);

    EXPECT_TRUE(html.Str().Contains("Fast Shard Layout")) << html.Str();
    for (const auto& name: expectedNames) {
        EXPECT_TRUE(html.Str().Contains("<td>" + name + "</td>"))
            << html.Str();
    }

    EXPECT_TRUE(html.Str().Contains("Storage Groups")) << html.Str();
    EXPECT_TRUE(html.Str().Contains("<td>E_SG_MIRROR</td>")) << html.Str();
    for (ui32 i = 1; i <= 3; ++i) {
        EXPECT_TRUE(
            html.Str().Contains("<td>host-" + ToString(i) + "</td>"))
            << html.Str();
        EXPECT_TRUE(
            html.Str().Contains("<td>" + ToString(29900 + i) + "</td>"))
            << html.Str();
        EXPECT_TRUE(
            html.Str().Contains("<td>device-" + ToString(i) + "</td>"))
            << html.Str();
    }

    //
    // Every template variable must have been substituted.
    //

    EXPECT_FALSE(html.Str().Contains("{{")) << html.Str();
}
