#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/sn/quorum/storage_group.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/fake_storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/silk_env.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <gtest/gtest.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

using namespace NCloud;
using namespace NFileStore::NStorage::NFastShard;
using silk::FiberScheduler;

namespace {

////////////////////////////////////////////////////////////////////////////////

[[maybe_unused]] auto* const gEnv =
    ::testing::AddGlobalTestEnvironment(MakeSilkTestEnv());

////////////////////////////////////////////////////////////////////////////////
// Test fixture: builds a set of fake storage nodes. And that's it.

struct TStorageFixture
{
    static constexpr ui32 NodeCount = 3;
    TVector<std::shared_ptr<TFakeStorageNode>> StorageNodes;
    IStorageGroupPtr Group;

    TStorageFixture()
        : StorageNodes(NodeCount)
    {
        TVector<IStorageNodePtr> nodes(NodeCount);
        for (ui32 i = 0; i < NodeCount; ++i) {
            StorageNodes[i] = std::make_shared<TFakeStorageNode>();
            nodes[i] = StorageNodes[i];
        }

        Group = CreateNaiveMirroredStorageGroup(std::move(nodes));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(NaiveGroupTest, MirrorsAcquireReleaseRequests)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int {
            TStorageFixture fx;

            {
                NProto::TAcquireDevicesRequest request;
                request.AddDeviceUUIDs("dev-a");
                request.AddDeviceUUIDs("dev-b");

                auto response = fx.Group->AcquireDevices(request);
                EXPECT_EQ(S_OK, response.GetError().GetCode())
                    << response.GetError().GetMessage();
            }

            //
            // We expect naive group impl to do dumb mirroring for acquire and
            // release requests.
            //

            for (auto& sn: fx.StorageNodes) {
                EXPECT_EQ(1U, sn->AcquireCalls.size());
                EXPECT_EQ(2U, sn->AcquireCalls[0].DeviceUUIDsSize());
                EXPECT_EQ("dev-a", sn->AcquireCalls[0].GetDeviceUUIDs(0));
                EXPECT_EQ("dev-b", sn->AcquireCalls[0].GetDeviceUUIDs(1));
            }

            {
                NProto::TReleaseDevicesRequest request;
                request.AddDeviceUUIDs("dev-a");
                request.AddDeviceUUIDs("dev-b");

                auto response = fx.Group->ReleaseDevices(request);
                EXPECT_EQ(S_OK, response.GetError().GetCode())
                    << response.GetError().GetMessage();
            }

            //
            // We expect naive group impl to do dumb mirroring for acquire and
            // release requests.
            //

            for (auto& sn: fx.StorageNodes) {
                EXPECT_EQ(1U, sn->ReleaseCalls.size());
                EXPECT_EQ(2U, sn->ReleaseCalls[0].DeviceUUIDsSize());
                EXPECT_EQ("dev-a", sn->ReleaseCalls[0].GetDeviceUUIDs(0));
                EXPECT_EQ("dev-b", sn->ReleaseCalls[0].GetDeviceUUIDs(1));
            }

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveGroupTest, MirrorsWrites)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int {
            TStorageFixture fx;

            {
                NProto::TWriteLogRecordRequest request;
                request.SetDeviceUUID("dev");
                auto* pg = request.AddPageGroups();
                pg->AddContent("page1");
                pg->AddContent("page2");
                pg->SetFirstPageNo(111);

                auto response = fx.Group->WriteLogRecord(request);
                EXPECT_EQ(S_OK, response.GetError().GetCode())
                    << response.GetError().GetMessage();
            }

            for (auto& sn: fx.StorageNodes) {
                EXPECT_EQ(1U, sn->WriteCalls.size());
                EXPECT_EQ("dev", sn->WriteCalls[0].GetDeviceUUID());
                EXPECT_EQ(1U, sn->WriteCalls[0].PageGroupsSize());
                const auto& pg = sn->WriteCalls[0].GetPageGroups(0);
                EXPECT_EQ(111U, pg.GetFirstPageNo());
                EXPECT_EQ(2U, pg.ContentSize());
                EXPECT_EQ("page1", pg.GetContent(0));
                EXPECT_EQ("page2", pg.GetContent(1));
            }

            {
                NProto::TReleaseDevicesRequest request;
                request.AddDeviceUUIDs("dev-a");
                request.AddDeviceUUIDs("dev-b");

                auto response = fx.Group->ReleaseDevices(request);
                EXPECT_EQ(S_OK, response.GetError().GetCode())
                    << response.GetError().GetMessage();
            }

            //
            // We expect naive group impl to do dumb mirroring for acquire and
            // release requests.
            //

            for (auto& sn: fx.StorageNodes) {
                EXPECT_EQ(1U, sn->ReleaseCalls.size());
                EXPECT_EQ(2U, sn->ReleaseCalls[0].DeviceUUIDsSize());
                EXPECT_EQ("dev-a", sn->ReleaseCalls[0].GetDeviceUUIDs(0));
                EXPECT_EQ("dev-b", sn->ReleaseCalls[0].GetDeviceUUIDs(1));
            }

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveGroupTest, RoundRobinsRead)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int {
            TStorageFixture fx;

            TVector<NProto::TReadPagesResponse> readResponses(
                TStorageFixture::NodeCount);
            for (ui32 i = 0; i < TStorageFixture::NodeCount; ++i) {
                auto* pg = readResponses[i].AddPageGroups();
                pg->SetFirstPageNo(111);
                pg->AddContent(TStringBuilder() << "aaa" << i);
                fx.StorageNodes[i]->ReadResp = readResponses[i];
            }

            for (ui32 i = 0; i < 10 * TStorageFixture::NodeCount; ++i) {
                const ui32 snIndex = i % TStorageFixture::NodeCount;

                {
                    NProto::TReadPagesRequest request;
                    request.SetDeviceUUID("dev");

                    auto* pg = request.AddPageGroupRefs();
                    pg->SetPageSize(4_KB);
                    pg->SetPageCount(100);
                    pg->SetFirstPageNo(111);

                    auto response = fx.Group->ReadPages(request);
                    EXPECT_EQ(S_OK, response.GetError().GetCode())
                        << response.GetError().GetMessage();
                    EXPECT_STREQ(
                        readResponses[snIndex].ShortUtf8DebugString().c_str(),
                        response.ShortUtf8DebugString().c_str());
                }

                auto& sn = fx.StorageNodes[snIndex];
                const ui32 cnt = 1 + i / TStorageFixture::NodeCount;
                EXPECT_EQ(cnt, sn->ReadCalls.size());
                EXPECT_EQ("dev", sn->ReadCalls[cnt - 1].GetDeviceUUID());
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
