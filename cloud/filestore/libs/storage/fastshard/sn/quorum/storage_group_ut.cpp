#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/sn/quorum/storage_group.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/fake_storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/silk_env.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer_test.h>
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
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
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

        Group = CreateNaiveMirroredStorageGroup(
            std::move(devices),
            TStorageGroupRetryPolicy{},
            Timer);
    }
};

////////////////////////////////////////////////////////////////////////////////

//
// The tests below use the default retry policy (300s total timeout, 0.5s
// backoff increment) against the fake TTestTimer, which advances its clock
// by the requested duration upon each Sleep() call. For a permanently
// failing request the k-th error therefore happens at
// t = 0.25 * k * (k + 1) seconds and the first k with t >= 300s is 35. So
// such a request makes 36 attempts and sleeps 35 times: 0.5s, 1.0s, ...,
// 17.5s.
//

constexpr ui32 ExpectedTimeoutAttempts = 36;
constexpr ui32 ExpectedTimeoutBackoffs = 35;

const TDuration DefaultBackoffIncrement = TDuration::MilliSeconds(500);

void CheckBackoffDurations(
    const TVector<TDuration>& sleeps,
    ui32 expectedCount)
{
    ASSERT_EQ(expectedCount, sleeps.size());
    for (ui32 i = 0; i < sleeps.size(); ++i) {
        EXPECT_EQ(DefaultBackoffIncrement * (i + 1), sleeps[i]);
    }
}

NProto::TWriteLogRecordResponse WriteErrorResponse(ui32 code)
{
    NProto::TWriteLogRecordResponse resp;
    *resp.MutableError() = MakeError(code, "scripted error");
    return resp;
}

NProto::TReadPagesResponse ReadErrorResponse(ui32 code)
{
    NProto::TReadPagesResponse resp;
    *resp.MutableError() = MakeError(code, "scripted error");
    return resp;
}

NProto::TError WriteSomething(IStorageGroup& group)
{
    TPageGroup pageGroup{.FirstPageNo = 111};
    pageGroup.Content.emplace_back("page1", 5U /* len */);
    TVector<TPageGroup> pageGroups;
    pageGroups.push_back(std::move(pageGroup));

    return group.WriteLogRecord(
        defaultHeaders,
        std::move(pageGroups),
        1234 /* lsn */);
}

NProto::TError ReadSomething(
    IStorageGroup& group,
    TVector<TPageGroup>* pageGroups)
{
    TVector<TPageGroupRef> pageGroupRefs = {{
        .FirstPageNo = 111,
        .PageCount = 1,
        .PageSize = 4_KB,
    }};

    return group.ReadPages(defaultHeaders, pageGroupRefs, pageGroups);
}

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
                TPageGroup pageGroup{.FirstPageNo = 111};
                pageGroup.Content.emplace_back("page1", 5U /* len */);
                pageGroup.Content.emplace_back("page2", 5U /* len */);
                TVector<TPageGroup> pageGroups;
                pageGroups.push_back(std::move(pageGroup));

                auto error = fx.Group->WriteLogRecord(
                    defaultHeaders,
                    std::move(pageGroups),
                    1234 /* lsn */);
                EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
            }

            for (ui64 i = 0; i < TStorageFixture::NodeCount; ++i) {
                auto& sn = fx.StorageNodes[i];
                EXPECT_EQ(1U, sn->WriteCalls.size());
                EXPECT_EQ(fx.DeviceUUIDs[i], sn->WriteCalls[0].GetDeviceUUID());
                EXPECT_EQ(1U, sn->WriteCalls[0].PageGroupsSize());
                EXPECT_EQ(1234U, sn->WriteCalls[0].GetLogSequenceNumber());
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
                            const auto& c = pageGroups[j].Content[k];
                            EXPECT_EQ(
                                epg.GetContent(k),
                                TString(c.Data(), c.Size()));
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

TEST(NaiveGroupTest, RetriesRetriableWriteErrors)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TStorageFixture fx;

            auto& flaky = *fx.StorageNodes[1];
            flaky.WriteRespQueue.push_back(WriteErrorResponse(E_REJECTED));
            flaky.WriteRespQueue.push_back(WriteErrorResponse(E_TIMEOUT));

            auto error = WriteSomething(*fx.Group);
            EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();

            //
            // The healthy nodes should see exactly one attempt, the flaky one
            // should see two failed attempts and the successful third one. All
            // attempts should carry the same request. Two backoffs should
            // have been applied: 0.5s after the first error and 1s after the
            // second one.
            //

            EXPECT_EQ(1U, fx.StorageNodes[0]->WriteCalls.size());
            EXPECT_EQ(3U, flaky.WriteCalls.size());
            EXPECT_EQ(1U, fx.StorageNodes[2]->WriteCalls.size());

            for (const auto& call: flaky.WriteCalls) {
                EXPECT_EQ(fx.DeviceUUIDs[1], call.GetDeviceUUID());
                EXPECT_EQ(1234U, call.GetLogSequenceNumber());
                EXPECT_EQ(1U, call.PageGroupsSize());
                EXPECT_EQ("page1", call.GetPageGroups(0).GetContent(0));
            }

            CheckBackoffDurations(
                fx.Timer->GetSleepDurations(),
                2 /* expectedCount */);

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveGroupTest, DoesNotRetryNonRetriableWriteErrors)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TStorageFixture fx;

            auto& broken = *fx.StorageNodes[1];
            broken.WriteRespQueue.push_back(WriteErrorResponse(E_ARGUMENT));

            auto error = WriteSomething(*fx.Group);
            EXPECT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();

            for (ui32 i = 0; i < TStorageFixture::NodeCount; ++i) {
                EXPECT_EQ(1U, fx.StorageNodes[i]->WriteCalls.size());
            }

            EXPECT_TRUE(fx.Timer->GetSleepDurations().empty());

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveGroupTest, PropagatesWriteErrorUponRetryTimeout)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TStorageFixture fx;

            //
            // The canned response makes the node fail every attempt, so the
            // request should burn through the whole retry budget with the
            // exact default backoff schedule and propagate the last error.
            //

            auto& broken = *fx.StorageNodes[1];
            broken.WriteResp = WriteErrorResponse(E_REJECTED);

            auto error = WriteSomething(*fx.Group);
            EXPECT_EQ(E_REJECTED, error.GetCode()) << error.GetMessage();

            EXPECT_EQ(1U, fx.StorageNodes[0]->WriteCalls.size());
            EXPECT_EQ(
                ExpectedTimeoutAttempts,
                broken.WriteCalls.size());
            EXPECT_EQ(1U, fx.StorageNodes[2]->WriteCalls.size());

            CheckBackoffDurations(
                fx.Timer->GetSleepDurations(),
                ExpectedTimeoutBackoffs);

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveGroupTest, RetriesReadsOnAnotherDevice)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TStorageFixture fx;

            auto& flaky = *fx.StorageNodes[0];
            flaky.ReadRespQueue.push_back(ReadErrorResponse(E_REJECTED));

            NProto::TReadPagesResponse goodResp;
            auto* rpg = goodResp.AddPageGroups();
            rpg->SetFirstPageNo(111);
            rpg->AddContent("bbb");
            fx.StorageNodes[1]->ReadResp = goodResp;

            TVector<TPageGroup> pageGroups;
            auto error = ReadSomething(*fx.Group, &pageGroups);
            EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();

            //
            // The first attempt should hit node 0 and fail, the retry should
            // advance the round-robin selector and read from node 1.
            //

            EXPECT_EQ(1U, fx.StorageNodes[0]->ReadCalls.size());
            EXPECT_EQ(1U, fx.StorageNodes[1]->ReadCalls.size());
            EXPECT_EQ(0U, fx.StorageNodes[2]->ReadCalls.size());

            EXPECT_EQ(
                fx.DeviceUUIDs[0],
                fx.StorageNodes[0]->ReadCalls[0].GetDeviceUUID());
            EXPECT_EQ(
                fx.DeviceUUIDs[1],
                fx.StorageNodes[1]->ReadCalls[0].GetDeviceUUID());

            EXPECT_EQ(1U, pageGroups.size());
            EXPECT_EQ(111U, pageGroups[0].FirstPageNo);
            EXPECT_EQ(1U, pageGroups[0].Content.size());
            const auto& c = pageGroups[0].Content[0];
            EXPECT_EQ("bbb", TString(c.Data(), c.Size()));

            CheckBackoffDurations(
                fx.Timer->GetSleepDurations(),
                1 /* expectedCount */);

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveGroupTest, PropagatesReadErrorUponRetryTimeout)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TStorageFixture fx;

            for (ui32 i = 0; i < TStorageFixture::NodeCount; ++i) {
                fx.StorageNodes[i]->ReadResp =
                    ReadErrorResponse(E_UNAVAILABLE);
            }

            TVector<TPageGroup> pageGroups;
            auto error = ReadSomething(*fx.Group, &pageGroups);

            EXPECT_EQ(E_UNAVAILABLE, error.GetCode()) << error.GetMessage();
            EXPECT_TRUE(pageGroups.empty());

            //
            // Each attempt advances the round-robin selector, so the retry
            // budget should be spread across the replicas evenly.
            //

            for (ui32 i = 0; i < TStorageFixture::NodeCount; ++i) {
                EXPECT_EQ(
                    ExpectedTimeoutAttempts / TStorageFixture::NodeCount,
                    fx.StorageNodes[i]->ReadCalls.size());
            }

            CheckBackoffDurations(
                fx.Timer->GetSleepDurations(),
                ExpectedTimeoutBackoffs);

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}
