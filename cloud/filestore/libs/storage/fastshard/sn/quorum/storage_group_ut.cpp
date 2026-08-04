#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/sn/quorum/storage_group.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/fake_storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/silk_env.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <gtest/gtest.h>

using namespace NCloud;
using namespace NFileStore::NStorage::NFastShard;
using silk::FiberScheduler;

namespace {

////////////////////////////////////////////////////////////////////////////////

[[maybe_unused]] auto* const gEnv =
    ::testing::AddGlobalTestEnvironment(MakeSilkTestEnv());

const NProto::TDeviceRequestHeaders defaultHeaders;

////////////////////////////////////////////////////////////////////////////////
// Test fixture: builds a set of fake storage nodes. And that's it.

struct TStorageFixture
{
    static constexpr ui32 NodeCount = 3;
    TVector<std::shared_ptr<TFakeStorageNode>> StorageNodes;
    const TVector<TString> DeviceUUIDs = {
        "dev-a",
        "dev-b",
        "dev-c",
    };
    IStorageGroupPtr Group;

    TStorageFixture()
        : StorageNodes(NodeCount)
    {
        TVector<TStorageDevice> devices(NodeCount);
        for (ui32 i = 0; i < NodeCount; ++i) {
            StorageNodes[i] = std::make_shared<TFakeStorageNode>();
            devices[i] = {
                .Node = StorageNodes[i],
                .DeviceUUID = DeviceUUIDs[i],
            };
        }

        Group = CreateNaiveMirroredStorageGroup(std::move(devices));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(NaiveGroupTest, MirrorsAcquireReleaseRequests)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TStorageFixture fx;

            {
                auto error = fx.Group->AcquireDevices();
                EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
            }

            //
            // We expect naive group impl to do dumb mirroring for acquire and
            // release requests.
            //

            for (ui32 i = 0; i < TStorageFixture::NodeCount; ++i) {
                auto& sn = fx.StorageNodes[i];
                EXPECT_EQ(1U, sn->AcquireCalls.size());
                EXPECT_EQ(1U, sn->AcquireCalls[0].DeviceUUIDsSize());
                EXPECT_EQ(
                    fx.DeviceUUIDs[i],
                    sn->AcquireCalls[0].GetDeviceUUIDs(0));
            }

            {
                auto error = fx.Group->ReleaseDevices();
                EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
            }

            //
            // We expect naive group impl to do dumb mirroring for acquire and
            // release requests.
            //

            for (ui32 i = 0; i < TStorageFixture::NodeCount; ++i) {
                auto& sn = fx.StorageNodes[i];
                EXPECT_EQ(1U, sn->ReleaseCalls.size());
                EXPECT_EQ(1U, sn->ReleaseCalls[0].DeviceUUIDsSize());
                EXPECT_EQ(
                    fx.DeviceUUIDs[i],
                    sn->ReleaseCalls[0].GetDeviceUUIDs(0));
            }

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveGroupTest, MirrorsWrites)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TStorageFixture fx;

            {
                TVector<TPageGroup> pageGroups = {{
                    .FirstPageNo = 111,
                    .Content = {"page1", "page2"},
                }};

                auto error = fx.Group->WriteLogRecord(
                    defaultHeaders,
                    std::move(pageGroups));
                EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
            }

            for (ui64 i = 0; i < TStorageFixture::NodeCount; ++i) {
                auto& sn = fx.StorageNodes[i];
                EXPECT_EQ(1U, sn->WriteCalls.size());
                EXPECT_EQ(fx.DeviceUUIDs[i], sn->WriteCalls[0].GetDeviceUUID());
                EXPECT_EQ(1U, sn->WriteCalls[0].PageGroupsSize());
                const auto& pg = sn->WriteCalls[0].GetPageGroups(0);
                EXPECT_EQ(111U, pg.GetFirstPageNo());
                EXPECT_EQ(2U, pg.ContentSize());
                EXPECT_EQ("page1", pg.GetContent(0));
                EXPECT_EQ("page2", pg.GetContent(1));
            }

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveGroupTest, RoundRobinsRead)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TStorageFixture fx;

            TVector<NProto::TReadPagesResponse> readResponses(
                TStorageFixture::NodeCount);
            for (ui64 i = 0; i < TStorageFixture::NodeCount; ++i) {
                auto* pg = readResponses[i].AddPageGroups();
                pg->SetFirstPageNo(111);
                pg->AddContent(TStringBuilder() << "aaa" << i);
                fx.StorageNodes[i]->ReadResp = readResponses[i];
            }

            for (ui64 i = 0; i < 10 * TStorageFixture::NodeCount; ++i) {
                const ui32 snIndex = i % TStorageFixture::NodeCount;

                {
                    TVector<TPageGroupRef> pageGroupRefs = {{
                        .FirstPageNo = 111,
                        .PageCount = 100,
                        .PageSize = 4_KB,
                    }};

                    TVector<TPageGroup> pageGroups;

                    auto error = fx.Group->ReadPages(
                        defaultHeaders,
                        pageGroupRefs,
                        &pageGroups);
                    EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
                    EXPECT_EQ(
                        readResponses[snIndex].PageGroupsSize(),
                        pageGroups.size());
                    for (ui64 j = 0; j < pageGroups.size(); ++j) {
                        const auto& epg =
                            readResponses[snIndex].GetPageGroups(j);
                        EXPECT_EQ(
                            epg.GetFirstPageNo(),
                            pageGroups[j].FirstPageNo);
                        EXPECT_EQ(
                            epg.ContentSize(),
                            pageGroups[j].Content.size());
                        for (ui64 k = 0; k < pageGroups[j].Content.size(); ++k)
                        {
                            EXPECT_STREQ(
                                epg.GetContent(k).c_str(),
                                pageGroups[j].Content[k].c_str());
                        }
                    }
                }

                auto& sn = fx.StorageNodes[snIndex];
                const ui32 cnt = 1 + i / TStorageFixture::NodeCount;
                EXPECT_EQ(cnt, sn->ReadCalls.size());
                EXPECT_EQ(
                    fx.DeviceUUIDs[snIndex],
                    sn->ReadCalls[cnt - 1].GetDeviceUUID());
                EXPECT_EQ(1U, sn->ReadCalls[cnt - 1].PageGroupRefsSize());
                const auto& pg = sn->ReadCalls[cnt - 1].GetPageGroupRefs(0);
                EXPECT_EQ(4_KB, pg.GetPageSize());
                EXPECT_EQ(100U, pg.GetPageCount());
                EXPECT_EQ(111U, pg.GetFirstPageNo());
            }

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}
