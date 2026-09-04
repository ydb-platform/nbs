#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/sn/quorum/storage_group.h>
#include <cloud/filestore/libs/storage/fastshard/sn/quorum/storage_group_quorum.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/fake_storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/silk_env.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <silk/fibers/event.h>
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

/**
 * Storage node whose WriteLogRecord parks the calling fiber on a gate until the
 * test opens it. Lets a test hold one replica back and observe what the group
 * does with the remaining ones.
 */
struct TPausableStorageNode: public TFakeStorageNode
{
    silk::FiberEvent Gate;
    std::atomic<bool> Paused = false;
    std::atomic<ui64> HoldLsn = 0;
    std::atomic<ui32> Parked = 0;

    NProto::TWriteLogRecordResponse WriteLogRecord(
        NProto::TWriteLogRecordRequest request) override
    {
        const ui64 lsn = request.GetLogSequenceNumber();
        const ui64 held = HoldLsn;
        if (Paused || (held && held == lsn)) {
            ++Parked;
            Gate.wait();
        }

        return TFakeStorageNode::WriteLogRecord(std::move(request));
    }

    void Unpause()
    {
        Paused = false;
        HoldLsn = 0;
        Gate.set();
    }
};

////////////////////////////////////////////////////////////////////////////////
// Test fixture: three fake storage nodes and a group built over them. The gate
// on the nodes is closed by default, so tests that never touch it see a plain
// fake.

using TGroupFactory = IStorageGroupPtr (*)(
    TVector<TStorageDevice>,
    TStorageGroupRetryPolicy,
    ITimerPtr);

struct TStorageFixture
{
    static constexpr ui32 NodeCount = 3;
    TVector<std::shared_ptr<TPausableStorageNode>> StorageNodes;
    const TVector<TString> DeviceUUIDs = {
        "dev-a",
        "dev-b",
        "dev-c",
    };
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    IStorageGroupPtr Group;

    TStorageFixture(TGroupFactory createGroup = CreateNaiveMirroredStorageGroup)
        : StorageNodes(NodeCount)
    {
        TVector<TStorageDevice> devices(NodeCount);
        for (ui32 i = 0; i < NodeCount; ++i) {
            StorageNodes[i] = std::make_shared<TPausableStorageNode>();
            devices[i] = {
                .Node = StorageNodes[i],
                .DeviceUUID = DeviceUUIDs[i],
            };
        }

        Group = createGroup(
            std::move(devices),
            TStorageGroupRetryPolicy{},
            Timer);
    }
};

struct TQuorumFixture: TStorageFixture
{
    TQuorumFixture()
        : TStorageFixture(CreateQuorumMirroredStorageGroup)
    {}
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

////////////////////////////////////////////////////////////////////////////////
// Quorum group.

namespace {

////////////////////////////////////////////////////////////////////////////////

// Yields until the predicate holds, so a test can observe a detached fiber's
// side effect without sleeping.
template <typename TPredicate>
void WaitFor(TPredicate predicate)
{
    for (ui32 i = 0; i < 100000 && !predicate(); ++i) {
        FiberScheduler::yield();
    }
    ASSERT_TRUE(predicate());
}

// True if the fiber is still blocked after giving the scheduler a good chance
// to run everything else that is runnable.
bool StillRunning(silk::FiberFuture& future)
{
    for (ui32 i = 0; i < 2000; ++i) {
        FiberScheduler::yield();
    }
    int error = 0;
    return !future.isSet(&error);
}

ui32 TotalParked(const TStorageFixture& fx)
{
    ui32 total = 0;
    for (const auto& sn: fx.StorageNodes) {
        total += sn->Parked;
    }
    return total;
}

struct TConcurrentWriteParams
{
    TStorageFixture* Fixture;
    ui64 Lsn;
};

int ConcurrentWriteFiberMain(TConcurrentWriteParams* params) noexcept
{
    TPageGroup pageGroup{.FirstPageNo = 111};
    pageGroup.Content.emplace_back("page1", 5U /* len */);
    TVector<TPageGroup> pageGroups;
    pageGroups.push_back(std::move(pageGroup));

    auto error = params->Fixture->Group->WriteLogRecord(
        defaultHeaders,
        std::move(pageGroups),
        params->Lsn);
    return HasError(error) ? 1 : 0;
}

void StartWrite(TStorageFixture& fx, ui64 lsn, silk::FiberFuture* future)
{
    const int r = FiberScheduler::run(
        ConcurrentWriteFiberMain,
        TConcurrentWriteParams{.Fixture = &fx, .Lsn = lsn},
        future);
    EXPECT_EQ(0, r);
}

ui32 TotalWriteCalls(const TStorageFixture& fx)
{
    ui32 total = 0;
    for (const auto& sn: fx.StorageNodes) {
        total += sn->WriteCalls.size();
    }
    return total;
}

// Fails reads everywhere but @p i, then reads until one succeeds: only @p i
// can serve it, and only once it has caught up to the quorum lsn. A recorded
// write call proves neither, the fake logs it before the fiber acks.
void WaitUntilServes(TStorageFixture& fx, ui32 i)
{
    for (ui32 j = 0; j < TQuorumFixture::NodeCount; ++j) {
        if (j != i) {
            fx.StorageNodes[j]->ReadResp = ReadErrorResponse(E_ARGUMENT);
        }
    }
    WaitFor(
        [&]
        {
            TVector<TPageGroup> pageGroups;
            return !HasError(ReadSomething(*fx.Group, &pageGroups));
        });
    EXPECT_GT(fx.StorageNodes[i]->ReadCalls.size(), 0U);
}

}   // namespace

TEST(QuorumGroupTest, AcquireReleaseNeedEveryDevice)
{
    int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TQuorumFixture fx;

            auto error = fx.Group->AcquireDevices();
            EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();

            for (ui32 i = 0; i < TQuorumFixture::NodeCount; ++i) {
                auto& sn = fx.StorageNodes[i];
                EXPECT_EQ(1U, sn->AcquireCalls.size());
                EXPECT_EQ(
                    fx.DeviceUUIDs[i],
                    sn->AcquireCalls[0].GetDeviceUUIDs(0));
            }

            error = fx.Group->ReleaseDevices();
            EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
            for (ui32 i = 0; i < TQuorumFixture::NodeCount; ++i) {
                EXPECT_EQ(1U, fx.StorageNodes[i]->ReleaseCalls.size());
            }

            return 0;
        },
        0);
    EXPECT_EQ(0, r);

    r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TQuorumFixture fx;

            // Acquire is n/n: one bad device is enough to fail the group.
            *fx.StorageNodes[2]->AcquireResp.MutableError() =
                MakeError(E_ARGUMENT, "scripted error");

            auto error = fx.Group->AcquireDevices();
            EXPECT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(QuorumGroupTest, WriteReturnsOnMajorityAck)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TQuorumFixture fx;

            // Hold the third replica back: the write must not wait for it.
            fx.StorageNodes[2]->Paused = true;

            auto error = WriteSomething(*fx.Group);
            EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();

            EXPECT_EQ(1U, fx.StorageNodes[0]->WriteCalls.size());
            EXPECT_EQ(1U, fx.StorageNodes[1]->WriteCalls.size());
            EXPECT_EQ(0U, fx.StorageNodes[2]->WriteCalls.size());

            // The straggler was dispatched, just detached: opening the gate
            // lets it land after the caller already returned.
            fx.StorageNodes[2]->Unpause();
            WaitFor([&] { return TotalWriteCalls(fx) == 3; });

            WaitUntilServes(fx, 2);

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(QuorumGroupTest, ReadSkipsReplicaBehindQuorumLsn)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TQuorumFixture fx;

            for (ui32 i = 0; i < TQuorumFixture::NodeCount; ++i) {
                auto* pg = fx.StorageNodes[i]->ReadResp.AddPageGroups();
                pg->SetFirstPageNo(111);
                pg->AddContent(TStringBuilder() << "aaa" << i);
            }

            fx.StorageNodes[2]->Paused = true;

            auto error = WriteSomething(*fx.Group);
            EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();

            // dev-c is below the quorum lsn, so only the other two serve.
            for (ui32 i = 0; i < 6; ++i) {
                TVector<TPageGroup> pageGroups;
                error = ReadSomething(*fx.Group, &pageGroups);
                EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
            }

            EXPECT_EQ(0U, fx.StorageNodes[2]->ReadCalls.size());
            EXPECT_EQ(6U, fx.StorageNodes[0]->ReadCalls.size()
                + fx.StorageNodes[1]->ReadCalls.size());

            // Rotation is independent of the watermark, so both eligible
            // replicas take a share.
            EXPECT_GT(fx.StorageNodes[0]->ReadCalls.size(), 0U);
            EXPECT_GT(fx.StorageNodes[1]->ReadCalls.size(), 0U);

            fx.StorageNodes[2]->Unpause();
            WaitFor([&] { return TotalWriteCalls(fx) == 3; });

            WaitUntilServes(fx, 2);

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(QuorumGroupTest, WriteErrorBreaksGroup)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TQuorumFixture fx;

            // dev-b held, dev-c failing: the failure lands before a quorum can
            // form. The other order is StragglerFailureBreaksGroupAfterAck.
            fx.StorageNodes[1]->Paused = true;
            fx.StorageNodes[2]->WriteResp = WriteErrorResponse(E_ARGUMENT);

            auto error = WriteSomething(*fx.Group);
            EXPECT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();

            // Stays broken: later requests are refused up front.
            error = WriteSomething(*fx.Group);
            EXPECT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();

            TVector<TPageGroup> pageGroups;
            error = ReadSomething(*fx.Group, &pageGroups);
            EXPECT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();

            fx.StorageNodes[1]->Unpause();
            WaitFor([&] { return TotalWriteCalls(fx) == 3; });

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(QuorumGroupTest, AcksConcurrentWritesWhateverOrderTheyLand)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TQuorumFixture fx;

            // Nothing acks until every write has been dispatched.
            for (auto& sn: fx.StorageNodes) {
                sn->Paused = true;
            }

            // Three writes in flight, acked in whatever order the release
            // produces; every device gets all three, every caller is told.
            const ui64 lsns[] = {1236, 1234, 1235};
            silk::FiberFuture futures[3];
            for (ui32 i = 0; i < 3; ++i) {
                StartWrite(fx, lsns[i], &futures[i]);
            }

            // Nine parked dispatches: all three records reached all three
            // devices before any of them acked.
            WaitFor([&] { return TotalParked(fx) == 9; });
            for (auto& sn: fx.StorageNodes) {
                sn->Unpause();
            }

            for (ui32 i = 0; i < 3; ++i) {
                EXPECT_EQ(0, futures[i].wait());
            }

            // A write returns on two acks; its third dispatch may still be
            // landing.
            WaitFor([&] { return TotalWriteCalls(fx) == 9; });
            for (ui32 i = 0; i < 3; ++i) {
                EXPECT_EQ(3U, fx.StorageNodes[i]->WriteCalls.size());
            }

            // Every replica is caught up, so all of them serve reads again.
            TVector<TPageGroup> pageGroups;
            auto error = ReadSomething(*fx.Group, &pageGroups);
            EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(QuorumGroupTest, WriteWaitsForItsOwnQuorum)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TQuorumFixture fx;

            // Every replica holds the first record's ack.
            for (auto& sn: fx.StorageNodes) {
                sn->HoldLsn = 100;
            }

            silk::FiberFuture first;
            StartWrite(fx, 100, &first);
            WaitFor([&] { return TotalParked(fx) == 3; });

            silk::FiberFuture second;
            StartWrite(fx, 200, &second);

            // 200 completes on its own acks; 100 is never inferred from them.
            EXPECT_EQ(0, second.wait());
            EXPECT_TRUE(StillRunning(first));

            for (auto& sn: fx.StorageNodes) {
                sn->Unpause();
            }
            EXPECT_EQ(0, first.wait());
            WaitFor([&] { return TotalWriteCalls(fx) == 6; });

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(QuorumGroupTest, RejectsLsnZero)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TQuorumFixture fx;

            TVector<TPageGroup> pageGroups;
            auto error = fx.Group->WriteLogRecord(
                defaultHeaders,
                std::move(pageGroups),
                0 /* lsn */);
            EXPECT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
            EXPECT_EQ(0U, TotalWriteCalls(fx));

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(QuorumGroupTest, StragglerFailureBreaksGroupAfterAck)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TQuorumFixture fx;

            // dev-c is held, and will fail once released.
            fx.StorageNodes[2]->Paused = true;
            fx.StorageNodes[2]->WriteResp = WriteErrorResponse(E_ARGUMENT);

            auto error = WriteSomething(*fx.Group);
            EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();

            // The caller has its ack and the group is still in service.
            {
                TVector<TPageGroup> pageGroups;
                error = ReadSomething(*fx.Group, &pageGroups);
                EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
            }

            // The record was already acked; a replica failing it afterwards
            // still breaks the group.
            fx.StorageNodes[2]->Unpause();
            WaitFor(
                [&]
                {
                    TVector<TPageGroup> pageGroups;
                    return HasError(ReadSomething(*fx.Group, &pageGroups));
                });

            error = WriteSomething(*fx.Group);
            EXPECT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
            TVector<TPageGroup> pageGroups;
            error = ReadSomething(*fx.Group, &pageGroups);
            EXPECT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(QuorumGroupTest, ReadFailoverStaysWithinEligibleReplicas)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TQuorumFixture fx;

            fx.StorageNodes[2]->Paused = true;
            auto error = WriteSomething(*fx.Group);
            EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();

            // Both replicas holding the record fail reads. dev-c is healthy but
            // behind: failing over to it would return stale pages.
            fx.StorageNodes[0]->ReadResp = ReadErrorResponse(E_ARGUMENT);
            fx.StorageNodes[1]->ReadResp = ReadErrorResponse(E_ARGUMENT);

            TVector<TPageGroup> pageGroups;
            error = ReadSomething(*fx.Group, &pageGroups);
            EXPECT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
            EXPECT_EQ(1U, fx.StorageNodes[0]->ReadCalls.size());
            EXPECT_EQ(1U, fx.StorageNodes[1]->ReadCalls.size());
            EXPECT_EQ(0U, fx.StorageNodes[2]->ReadCalls.size());

            // Read failures do not break the group: writes still go through.
            error = WriteSomething(*fx.Group);
            EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();

            fx.StorageNodes[2]->Unpause();
            WaitUntilServes(fx, 2);

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(QuorumGroupTest, DeliversPayloadToEachDevice)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TQuorumFixture fx;

            for (ui32 i = 0; i < TQuorumFixture::NodeCount; ++i) {
                auto* pg = fx.StorageNodes[i]->ReadResp.AddPageGroups();
                pg->SetFirstPageNo(111);
                pg->AddContent(TStringBuilder() << "aaa" << i);
            }

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
            WaitFor([&] { return TotalWriteCalls(fx) == 3; });

            // Every device got the same record, stamped with its own id.
            for (ui32 i = 0; i < TQuorumFixture::NodeCount; ++i) {
                const auto& w = fx.StorageNodes[i]->WriteCalls[0];
                EXPECT_EQ(fx.DeviceUUIDs[i], w.GetDeviceUUID());
                EXPECT_EQ(1234U, w.GetLogSequenceNumber());
                EXPECT_EQ(1U, w.PageGroupsSize());
                if (w.PageGroupsSize() != 1) {
                    return 1;
                }
                EXPECT_EQ(111U, w.GetPageGroups(0).GetFirstPageNo());
                EXPECT_EQ(2U, w.GetPageGroups(0).ContentSize());
                if (w.GetPageGroups(0).ContentSize() != 2) {
                    return 1;
                }
                EXPECT_EQ("page1", w.GetPageGroups(0).GetContent(0));
                EXPECT_EQ("page2", w.GetPageGroups(0).GetContent(1));
            }

            // And a read returns whichever replica served it, verbatim.
            TVector<TPageGroup> pageGroups;
            auto error = ReadSomething(*fx.Group, &pageGroups);
            EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
            EXPECT_EQ(1U, pageGroups.size());
            if (pageGroups.size() != 1 || pageGroups[0].Content.size() != 1) {
                return 1;
            }
            EXPECT_EQ(111U, pageGroups[0].FirstPageNo);

            ui32 served = TQuorumFixture::NodeCount;
            for (ui32 i = 0; i < TQuorumFixture::NodeCount; ++i) {
                if (fx.StorageNodes[i]->ReadCalls.size()) {
                    served = i;
                }
            }
            EXPECT_LT(served, TQuorumFixture::NodeCount);
            if (served == TQuorumFixture::NodeCount) {
                return 1;
            }
            EXPECT_EQ(
                TStringBuilder() << "aaa" << served,
                TString(pageGroups[0].Content[0].Data(),
                        pageGroups[0].Content[0].Size()));

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}
