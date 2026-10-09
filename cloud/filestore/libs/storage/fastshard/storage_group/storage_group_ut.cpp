#include <cloud/filestore/libs/storage/fastshard/storage_group/storage_group.h>
#include <cloud/filestore/libs/storage/fastshard/storage_group/storage_group_helpers.h>
#include <cloud/filestore/libs/storage/fastshard/storage_group/storage_group_quorum.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/fastshard/protos/device.pb.h>
#include <cloud/fastshard/sn/iface/storage_node.h>
#include <cloud/fastshard/testlib/fake_storage_node.h>
#include <cloud/fastshard/testlib/fiber_test.h>
#include <cloud/fastshard/testlib/silk_env.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer_test.h>

#include <silk/fibers/event.h>
#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/fibers/sequencer.h>

#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

#include <gtest/gtest.h>

using namespace NCloud;
using namespace NFileStore::NStorage::NFastShard;
using namespace NCloud::NFastShard;
using silk::FiberScheduler;

namespace {

////////////////////////////////////////////////////////////////////////////////

[[maybe_unused]] auto* const gEnv =
    ::testing::AddGlobalTestEnvironment(MakeSilkTestEnv());

constexpr ui32 DeviceCount = 3;

constexpr ui64 PageNo = 111;
constexpr ui64 Lsn = 2;

const NProto::TDeviceRequestHeaders NoHeaders;

////////////////////////////////////////////////////////////////////////////////

/**
 * A storage node that keeps nothing: it answers every request with the canned
 * reply it inherits, blank pages for a read, and parks a write while the gate
 * is shut, which lets a test hold one replica back and watch what the group
 * does with the others. A test sets it up by scripting the replies and checks
 * what it has been asked. What the devices end up holding is the journalled
 * node's business.
 */
struct TFakeDevice: TFakeStorageNode
{
    silk::FiberEvent Gate;
    std::atomic<bool> Paused = false;
    std::atomic<ui64> HoldLsn = 0;
    std::atomic<ui32> Parked = 0;

    silk::FiberEvent AcquireGate;
    std::atomic<bool> AcquirePaused = false;
    silk::FiberEvent AcquireParked;
    silk::FiberSequencer Acquires;

    void Unpause()
    {
        Paused = false;
        HoldLsn = 0;
        Gate.set();
        AcquirePaused = false;
        AcquireGate.set();
    }

    ui32 WriteCount()
    {
        with_lock (Lock) {
            return WriteCalls.size();
        }
    }

    TVector<ui64> Lsns()
    {
        TVector<ui64> lsns;
        with_lock (Lock) {
            for (const auto& write: WriteCalls) {
                lsns.push_back(write.GetLogSequenceNumber());
            }
        }
        return lsns;
    }

    NProto::TReadPagesResponse ReadPages(
        NProto::TReadPagesRequest request) override
    {
        const auto refs = request.GetPageGroupRefs();
        auto response = TFakeStorageNode::ReadPages(std::move(request));
        if (HasError(response.GetError()) || response.PageGroupsSize()) {
            return response;
        }

        for (const auto& ref: refs) {
            auto* pg = response.AddPageGroups();
            pg->SetFirstPageNo(ref.GetFirstPageNo());
            for (ui64 i = 0; i < ref.GetPageCount(); ++i) {
                pg->AddContent(TString(ref.GetPageSize(), '\0'));
            }
        }

        return response;
    }

    NProto::TWriteLogRecordResponse WriteLogRecord(
        NProto::TWriteLogRecordRequest request) override
    {
        if (Paused || HoldLsn == request.GetLogSequenceNumber()) {
            ++Parked;
            Gate.wait();
        }

        return TFakeStorageNode::WriteLogRecord(std::move(request));
    }

    NProto::TAcquireDevicesResponse AcquireDevices(
        NProto::TAcquireDevicesRequest request) override
    {
        if (AcquirePaused) {
            AcquireParked.set();
            AcquireGate.wait();
        }

        auto response = TFakeStorageNode::AcquireDevices(std::move(request));
        Acquires.increment();
        return response;
    }
};

using TFakeDevicePtr = std::shared_ptr<TFakeDevice>;

// A reply carrying one page, for a device that is to answer with something
// other than a blank.
NProto::TReadPagesResponse PageResponse(ui64 pageNo, TString content)
{
    NProto::TReadPagesResponse response;
    auto* pg = response.AddPageGroups();
    pg->SetFirstPageNo(pageNo);
    pg->AddContent(std::move(content));
    return response;
}

// The reply of a claimed device whose journal reaches @p lsn.
NProto::TReadPagesResponse ClaimedAt(const TString& claim, ui64 lsn)
{
    auto response = PageResponse(0, claim);
    response.SetLastAckedLogSequenceNumber(lsn);
    return response;
}

////////////////////////////////////////////////////////////////////////////////

// The background loops are off unless a test asks for one.
TStorageGroupConfig MakeConfig(ui32 pageSize = DefaultBlockSize)
{
    TStorageGroupConfig config;
    config.ClientId = "test-client";
    config.AcquireGeneration = 42;
    config.LowWatermarkPeriod = TDuration::Zero();
    config.ReacquirePeriod = TDuration::Zero();
    config.PageSize = pageSize;
    return config;
}

TStorageGroupConfig MakeConfigWithWaterMarksLoop()
{
    auto config = MakeConfig();
    config.LowWatermarkPeriod = TDuration::MilliSeconds(1);
    // Retries are off for everything the group does, not just the push: a
    // retry sleeps on the tick timer, which a test cannot tell apart from the
    // loop parking at the top of its own iteration.
    config.RetryPolicy.TotalTimeout = TDuration::Zero();
    return config;
}

TStorageGroupConfig MakeConfigWithReacquireLoop()
{
    auto config = MakeConfig();
    config.ReacquirePeriod = TDuration::MilliSeconds(1);
    // Retries are off for the same reason as in the watermark loop config: a
    // backoff sleeps on the tick timer.
    config.RetryPolicy.TotalTimeout = TDuration::Zero();
    return config;
}

////////////////////////////////////////////////////////////////////////////////

/**
 * A timer whose Sleep waits for the test to call TickOnce, which lets the
 * loops run exactly one round and returns once every one of them is parked in
 * Sleep again. The loops to expect are given up front: a round is over once
 * each of them has parked once more.
 */
struct TTickTimer: ITimer
{
    const ui64 Loops;
    silk::FiberSequencer Rounds;   // test to loops
    silk::FiberSequencer Parked;   // loops to test

    explicit TTickTimer(ui64 loops = 1)
        : Loops(loops)
    {}

    TInstant Now() override
    {
        return TInstant::Now();
    }

    void Sleep(TDuration duration) override
    {
        Y_UNUSED(duration);
        const ui64 round = (Parked.increment() + Loops - 1) / Loops;
        Y_UNUSED(Rounds.wait(round));
    }

    void Sleep(TDuration duration, const std::atomic<bool>& cancelled) override
    {
        Y_UNUSED(cancelled);
        Sleep(duration);
    }

    void TickOnce()
    {
        ReleaseRound();
        Y_UNUSED(Parked.wait((Rounds.get() + 1) * Loops));
    }

    // A tick that does not wait for the loops to park again, for a round
    // after which a loop may leave instead.
    void ReleaseRound()
    {
        Y_UNUSED(Parked.wait((Rounds.get() + 1) * Loops));
        Rounds.increment();
    }

    // One full iteration per tick. A tick right after a write may find the
    // last ack not yet counted, hence more than one.
    template <typename TPredicate>
    [[nodiscard]] bool TickUntil(TPredicate predicate)
    {
        for (ui32 i = 0; i < 100; ++i) {
            TickOnce();
            if (predicate()) {
                return true;
            }
        }
        return false;
    }

    // Lets the loops out of Sleep for good, so TearDown can join them.
    void Stop()
    {
        Rounds.stop();
    }
};

using TTickTimerPtr = std::shared_ptr<TTickTimer>;

using TGroupFactory = IStorageGroupPtr (*)(
    TStorageGroupConfig,
    TVector<TStorageDevice>,
    ITimerPtr);

////////////////////////////////////////////////////////////////////////////////
// Fixtures: a group over a set of fake devices.

struct TGroupFixture
{
    TVector<TFakeDevicePtr> Nodes;
    TVector<TString> DeviceUUIDs;
    TStorageGroupConfig Config;
    ITimerPtr Timer;
    IStorageGroupPtr Group;

    TGroupFixture(
            TGroupFactory createGroup,
            TStorageGroupConfig config,
            ITimerPtr timer,
            ui32 deviceCount)
        : Config(std::move(config))
        , Timer(std::move(timer))
    {
        for (ui32 i = 0; i < deviceCount; ++i) {
            Nodes.push_back(std::make_shared<TFakeDevice>());
            DeviceUUIDs.push_back(TStringBuilder() << "dev-" << char('a' + i));
        }

        Group = createGroup(Config, StorageDevices(), Timer);
    }

    ~TGroupFixture()
    {
        for (auto& device: Nodes) {
            device->Unpause();
        }

        Group->TearDown();
    }

    ui32 Size() const
    {
        return Nodes.size();
    }

    TFakeDevice& operator[](ui32 i)
    {
        return *Nodes[i];
    }

    TVector<TStorageDevice> StorageDevices() const
    {
        TVector<TStorageDevice> devices;
        for (ui32 i = 0; i < Size(); ++i) {
            devices.push_back({.Node = Nodes[i], .DeviceUUID = DeviceUUIDs[i]});
        }
        return devices;
    }

    // The sleeps the group asked for, when it runs on a TTestTimer.
    TTestTimer& TestTimer()
    {
        auto* timer = dynamic_cast<TTestTimer*>(Timer.get());
        Y_ABORT_UNLESS(timer, "the group runs on another timer");
        return *timer;
    }

    // Forgets what the devices have been asked so far, so a test that follows
    // Init starts from a clean request log.
    void ForgetRequests()
    {
        for (auto& device: Nodes) {
            device->WriteCalls.clear();
            device->ReadCalls.clear();
            device->ReadJournalTailCalls.clear();
        }
    }
};

struct TNaiveFixture: TGroupFixture
{
    TNaiveFixture(TStorageGroupConfig config = MakeConfig())
        : TGroupFixture(
              CreateNaiveMirroredStorageGroup,
              std::move(config),
              std::make_shared<TTestTimer>(),
              DeviceCount)
    {}
};

// init = false leaves Init to the test, so it can set the devices up first.
struct TQuorumFixture: TGroupFixture
{
    TTickTimerPtr TickTimer;

    TQuorumFixture(
            bool init = true,
            TStorageGroupConfig config = MakeConfig(),
            ui32 deviceCount = DeviceCount)
        : TGroupFixture(
              CreateQuorumMirroredStorageGroup,
              std::move(config),
              std::make_shared<TTestTimer>(),
              deviceCount)
    {
        if (init) {
            auto error = Group->Init().GetError();
            Y_ABORT_UNLESS(
                !HasError(error),
                "failed to initialize: %s",
                FormatError(error).c_str());
        }
    }

    TQuorumFixture(ui32 deviceCount)
        : TQuorumFixture(true, MakeConfig(), deviceCount)
    {}

    // On a tick timer, for the watermark loop; the loop is let out of Sleep
    // before TearDown joins it.
    TQuorumFixture(TTickTimerPtr tickTimer, TStorageGroupConfig config)
        : TGroupFixture(
              CreateQuorumMirroredStorageGroup,
              std::move(config),
              tickTimer,
              DeviceCount)
        , TickTimer(std::move(tickTimer))
    {
        auto error = Group->Init().GetError();
        Y_ABORT_UNLESS(
            !HasError(error),
            "failed to initialize: %s",
            FormatError(error).c_str());
    }

    ~TQuorumFixture()
    {
        if (TickTimer) {
            TickTimer->Stop();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////
// Driving the group.

NProto::TError InitFails(TGroupFixture& fx)
{
    return fx.Group->Init().GetError();
}

// A record of one page group, linked to the record before it.
NProto::TError WritePages(
    IStorageGroup& group,
    ui64 lsn,
    ui64 firstPageNo,
    std::initializer_list<TStringBuf> pages)
{
    TPageGroup pageGroup{.FirstPageNo = firstPageNo};
    for (TStringBuf page: pages) {
        pageGroup.Content.emplace_back(page.data(), page.size());
    }

    TVector<TPageGroup> pageGroups;
    pageGroups.push_back(std::move(pageGroup));
    return group.WriteLogRecord(
        NoHeaders,
        std::move(pageGroups),
        {.Lsn = lsn, .PrevLsn = lsn ? lsn - 1 : 0});
}

NProto::TError Write(IStorageGroup& group, ui64 lsn = Lsn)
{
    return WritePages(group, lsn, PageNo, {"page1"});
}

NProto::TError ReadRange(
    IStorageGroup& group,
    ui64 firstPageNo,
    ui64 pageCount,
    TVector<TPageGroup>* pageGroups)
{
    TVector<TPageGroupRef> refs = {{
        .FirstPageNo = firstPageNo,
        .PageCount = pageCount,
    }};

    return group.ReadPages(NoHeaders, refs, pageGroups);
}

NProto::TError Read(IStorageGroup& group, TVector<TPageGroup>* pageGroups)
{
    return ReadRange(group, PageNo, 1, pageGroups);
}

// The page as a string, or what went wrong instead, so that comparing it to
// the expected page says both.
TString ReadOnePage(IStorageGroup& group)
{
    TVector<TPageGroup> pageGroups;
    auto error = Read(group, &pageGroups);
    if (HasError(error)) {
        return TStringBuilder() << "<" << FormatError(error) << ">";
    }
    if (pageGroups.size() != 1 || pageGroups[0].Content.size() != 1) {
        return TStringBuilder()
            << "<" << pageGroups.size() << " page groups>";
    }

    const auto& page = pageGroups[0].Content[0];
    return TString(page.Data(), page.Size());
}

struct TWriteParams
{
    TGroupFixture* Fixture;
    ui64 Lsn;
};

int WriteFiberMain(TWriteParams* params) noexcept
{
    return HasError(Write(*params->Fixture->Group, params->Lsn)) ? 1 : 0;
}

// A write on its own fiber, so a test can have several in flight.
void StartWrite(TGroupFixture& fx, ui64 lsn, silk::FiberFuture* future)
{
    const int r = FiberScheduler::run(
        WriteFiberMain,
        TWriteParams{.Fixture = &fx, .Lsn = lsn},
        future);
    Y_ABORT_UNLESS(r == 0, "failed to spawn a writer: %s", ::strerror(r));
}

////////////////////////////////////////////////////////////////////////////////
// Waiting and inspecting.

// Yields until the predicate holds, so a test can observe a detached fiber's
// side effect without sleeping. False if it never held: assert on it, or the
// test carries on with its precondition unmet.
template <typename TPredicate>
[[nodiscard]] bool WaitFor(TPredicate predicate)
{
    for (ui32 i = 0; i < 100000 && !predicate(); ++i) {
        FiberScheduler::yield();
    }
    return predicate();
}

// True if the fiber is still blocked after the scheduler has had a good chance
// to run everything else that is runnable.
bool StillRunning(silk::FiberFuture& future)
{
    for (ui32 i = 0; i < 2000; ++i) {
        FiberScheduler::yield();
    }
    int error = 0;
    return !future.isSet(&error);
}

ui32 TotalWrites(TGroupFixture& fx)
{
    ui32 total = 0;
    for (ui32 i = 0; i < fx.Size(); ++i) {
        total += fx[i].WriteCount();
    }
    return total;
}

ui32 TotalParked(TGroupFixture& fx)
{
    ui32 total = 0;
    for (auto& device: fx.Nodes) {
        total += device->Parked;
    }
    return total;
}

TVector<ui32> WriteCounts(TGroupFixture& fx)
{
    TVector<ui32> counts;
    for (ui32 i = 0; i < fx.Size(); ++i) {
        counts.push_back(fx[i].WriteCount());
    }
    return counts;
}

TVector<ui32> ReadCounts(TGroupFixture& fx)
{
    TVector<ui32> counts;
    for (ui32 i = 0; i < fx.Size(); ++i) {
        counts.push_back(fx[i].ReadCalls.size());
    }
    return counts;
}

// The sleeps the group asked for, in microseconds so a mismatch reads well.
TVector<ui64> Sleeps(TGroupFixture& fx)
{
    TVector<ui64> sleeps;
    for (TDuration sleep: fx.TestTimer().GetSleepDurations()) {
        sleeps.push_back(sleep.MicroSeconds());
    }
    return sleeps;
}

// The sleeps a request that fails @p count times in a row asks for: the k-th
// backoff is k increments.
TVector<ui64> Backoffs(ui32 count)
{
    const TDuration increment = MakeConfig().RetryPolicy.BackoffIncrement;
    TVector<ui64> backoffs;
    for (ui32 k = 1; k <= count; ++k) {
        backoffs.push_back((increment * k).MicroSeconds());
    }
    return backoffs;
}

bool Mentions(const NProto::TError& error, TStringBuf what)
{
    return error.GetMessage().find(what) != TString::npos;
}

// The claim a quorum group wrote to the device, as the device was asked to
// take it.
TString Claim(TFakeDevice& device)
{
    return device.WriteCalls.at(0).GetPageGroups(0).GetContent(0);
}

// Forgets the read log and reads once per device. With every replica eligible
// the rotation lands each read on a different one, so a read count of one
// everywhere is what "all of them serve" looks like.
NProto::TError ReadFromEachReplica(TGroupFixture& fx)
{
    for (auto& device: fx.Nodes) {
        device->ReadCalls.clear();
    }

    NProto::TError first;
    for (ui32 i = 0; i < fx.Size(); ++i) {
        TVector<TPageGroup> pageGroups;
        auto error = Read(*fx.Group, &pageGroups);
        if (HasError(error) && !HasError(first)) {
            first = error;
        }
    }
    return first;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
// MirrorGroup

FIBER_TEST(NaiveGroupTest, MirrorsEveryRequestToEveryDevice)
{
    TNaiveFixture fx;
    const auto init = fx.Group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode())
        << init.GetError().GetMessage();

    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(1U, fx[i].AcquireCalls.size()) << "dev " << i;
        ASSERT_EQ(1U, fx[i].AcquireCalls[0].DeviceUUIDsSize());
        ASSERT_EQ(fx.DeviceUUIDs[i], fx[i].AcquireCalls[0].GetDeviceUUIDs(0));
    }

    auto error = WritePages(*fx.Group, 1, PageNo, {"page1", "page2"});
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(1U, fx[i].WriteCount()) << "dev " << i;
        const auto& write = fx[i].WriteCalls[0];
        ASSERT_EQ(1U, write.GetLogSequenceNumber()) << "dev " << i;
        // The naive group keeps nothing of its own on the device, so the
        // caller's page numbers are the device's page numbers.
        ASSERT_EQ(PageNo, write.GetPageGroups(0).GetFirstPageNo()) << "dev " << i;
        ASSERT_EQ("page1", write.GetPageGroups(0).GetContent(0)) << "dev " << i;
        ASSERT_EQ("page2", write.GetPageGroups(0).GetContent(1)) << "dev " << i;
        ASSERT_EQ(fx.DeviceUUIDs[i], write.GetDeviceUUID());
        ASSERT_EQ("test-client", write.GetHeaders().GetClientId());
    }

    fx.Group->TearDown();

    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(1U, fx[i].ReleaseCalls.size()) << "dev " << i;
        ASSERT_EQ(1U, fx[i].ReleaseCalls[0].DeviceUUIDsSize());
        ASSERT_EQ(fx.DeviceUUIDs[i], fx[i].ReleaseCalls[0].GetDeviceUUIDs(0));
    }
}

FIBER_TEST(NaiveGroupTest, InitReportsTheHighestAckedLsnAndChainsFromIt)
{
    TNaiveFixture fx;

    // The devices come back from a crash at different positions.
    const ui64 acked[] = {3, 7, 5};
    for (ui32 i = 0; i < DeviceCount; ++i) {
        fx[i].ReadJournalTailResp.SetLsnLowWatermark(acked[i]);
    }

    const auto init = fx.Group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode())
        << init.GetError().GetMessage();
    ASSERT_EQ(7U, init.GetResult());

    // The caller continues from the furthest one, and the link it builds is
    // what reaches the devices.
    auto error = Write(*fx.Group, 8);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    for (ui32 i = 0; i < DeviceCount; ++i) {
        const auto& write = fx[i].WriteCalls.back();
        ASSERT_EQ(8U, write.GetLogSequenceNumber()) << "dev " << i;
        ASSERT_EQ(7U, write.GetPrevLogSequenceNumber()) << "dev " << i;
    }
}

FIBER_TEST(NaiveGroupTest, RoundRobinsReadsAcrossDevices)
{
    TNaiveFixture fx;
    const auto init = fx.Group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode())
        << init.GetError().GetMessage();

    // Devices that disagree, so every answer names the one that gave it.
    for (ui32 i = 0; i < DeviceCount; ++i) {
        fx[i].ReadResp = PageResponse(PageNo, TStringBuilder() << "dev" << i);
    }

    for (ui32 round = 0; round < 2; ++round) {
        for (ui32 i = 0; i < DeviceCount; ++i) {
            ASSERT_EQ(
                TString(TStringBuilder() << "dev" << i),
                ReadOnePage(*fx.Group))
                << "round " << round;
        }
    }

    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(2U, fx[i].ReadCalls.size()) << "dev " << i;
    }
}

FIBER_TEST(NaiveGroupTest, RetriesRetriableErrors)
{
    TNaiveFixture fx;
    const auto init = fx.Group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode())
        << init.GetError().GetMessage();

    auto& flaky = fx[1];
    flaky.WriteRespQueue.emplace_back();
    *flaky.WriteRespQueue.back().MutableError() =
        MakeError(E_REJECTED, "busy");
    flaky.WriteRespQueue.emplace_back();
    *flaky.WriteRespQueue.back().MutableError() =
        MakeError(E_TIMEOUT, "slow");

    auto error = Write(*fx.Group, 1);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    ASSERT_EQ((TVector<ui32>{1, 3, 1}), WriteCounts(fx));
    ASSERT_EQ(Backoffs(2), Sleeps(fx));

    fx[1].ReadResp = PageResponse(PageNo, "payload");
    fx[0].ReadRespQueue.emplace_back();
    *fx[0].ReadRespQueue.back().MutableError() =
        MakeError(E_REJECTED, "busy");

    ASSERT_EQ("payload", ReadOnePage(*fx.Group));
    ASSERT_EQ((TVector<ui32>{1, 1, 0}), ReadCounts(fx));

    // The read starts a backoff sequence of its own; it does not continue the
    // write's.
    auto sleeps = Backoffs(2);
    sleeps.push_back(Backoffs(1)[0]);
    ASSERT_EQ(sleeps, Sleeps(fx));
}

FIBER_TEST(NaiveGroupTest, DoesNotRetryNonRetriableErrors)
{
    TNaiveFixture fx;
    const auto init = fx.Group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode())
        << init.GetError().GetMessage();

    *fx[1].WriteResp.MutableError() = MakeError(E_ARGUMENT, "bad record");
    auto error = Write(*fx.Group, 1);
    ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
    ASSERT_EQ(TVector<ui32>(DeviceCount, 1), WriteCounts(fx));

    *fx[0].ReadResp.MutableError() = MakeError(E_ARGUMENT, "bad range");
    TVector<TPageGroup> pageGroups;
    error = Read(*fx.Group, &pageGroups);
    ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
    ASSERT_TRUE(pageGroups.empty());

    // One attempt, and no failover either: a device that rejects the request
    // outright means the request is wrong, not the device.
    ASSERT_EQ((TVector<ui32>{1, 0, 0}), ReadCounts(fx));

    ASSERT_TRUE(fx.TestTimer().GetSleepDurations().empty());
}

FIBER_TEST(NaiveGroupTest, GivesUpWhenTheRetryDeadlineExpires)
{
    // A one second budget over the default half second increment. The test
    // timer advances by every sleep, so a dead device fails at 0s and 0.5s;
    // the backoff of a second after that would cross the budget, so there is
    // no third attempt.
    auto config = MakeConfig();
    config.RetryPolicy.TotalTimeout = TDuration::Seconds(1);
    constexpr ui32 attempts = 2;

    {
        TNaiveFixture fx(config);
        const auto init = fx.Group->Init();
        ASSERT_EQ(S_OK, init.GetError().GetCode())
            << init.GetError().GetMessage();
        *fx[1].WriteResp.MutableError() = MakeError(E_REJECTED, "busy");

        auto error = Write(*fx.Group, 1);
        ASSERT_EQ(E_REJECTED, error.GetCode()) << error.GetMessage();

        ASSERT_EQ((TVector<ui32>{1, attempts, 1}), WriteCounts(fx));
        ASSERT_EQ(Backoffs(attempts - 1), Sleeps(fx));
    }

    {
        TNaiveFixture fx(config);
        const auto init = fx.Group->Init();
        ASSERT_EQ(S_OK, init.GetError().GetCode())
            << init.GetError().GetMessage();
        for (ui32 i = 0; i < DeviceCount; ++i) {
            *fx[i].ReadResp.MutableError() = MakeError(E_UNAVAILABLE, "down");
        }

        TVector<TPageGroup> pageGroups;
        auto error = Read(*fx.Group, &pageGroups);
        ASSERT_EQ(E_UNAVAILABLE, error.GetCode()) << error.GetMessage();
        ASSERT_TRUE(pageGroups.empty());

        // The rotation keeps moving while retrying, so each attempt lands on
        // the next device.
        ASSERT_EQ((TVector<ui32>{1, 1, 0}), ReadCounts(fx));
        ASSERT_EQ(Backoffs(attempts - 1), Sleeps(fx));
    }
}

////////////////////////////////////////////////////////////////////////////////
// The quorum group: Init acquires every device, claims it by writing a first
// page of its own, and levels the journals. Writes go to all and return on a
// majority, reads go to a replica that has reached the acked lsn, and any
// device failure breaks the group for good.

FIBER_TEST(QuorumGroupTest, InitAcquiresEveryDeviceThenClaimsIt)
{
    TQuorumFixture fx(false /* init */);
    const auto init = fx.Group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode())
        << init.GetError().GetMessage();

    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(1U, fx[i].AcquireCalls.size()) << "dev " << i;
        const auto& acquire = fx[i].AcquireCalls[0];
        ASSERT_EQ(1U, acquire.DeviceUUIDsSize());
        ASSERT_EQ(fx.DeviceUUIDs[i], acquire.GetDeviceUUIDs(0));
        ASSERT_EQ(42U, acquire.GetGeneration());
        ASSERT_EQ("test-client", acquire.GetHeaders().GetClientId());

        // A blank device is claimed with a first page, and the claim is the
        // first record it takes, because a device refuses lsn zero.
        ASSERT_EQ(1U, fx[i].WriteCount()) << "dev " << i;
        const auto& claim = fx[i].WriteCalls[0];
        ASSERT_EQ(1U, claim.GetLogSequenceNumber()) << "dev " << i;
        ASSERT_EQ(0U, claim.GetPageGroups(0).GetFirstPageNo()) << "dev " << i;
        ASSERT_EQ(DefaultBlockSize, Claim(fx[i]).size()) << "dev " << i;
    }

    fx.Group->TearDown();
    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(1U, fx[i].ReleaseCalls.size()) << "dev " << i;
        ASSERT_EQ(fx.DeviceUUIDs[i], fx[i].ReleaseCalls[0].GetDeviceUUIDs(0));
    }
}

FIBER_TEST(QuorumGroupTest, InitNeedsEveryDeviceAndStopsAtTheFirstPhaseThatFails)
{
    // Acquire is n of n, and nothing is touched before it succeeds everywhere.
    {
        TQuorumFixture fx(false /* init */);
        *fx[1].AcquireResp.MutableError() = MakeError(E_ARGUMENT, "no session");

        auto error = InitFails(fx);
        ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
        ASSERT_EQ(TVector<ui32>(fx.Size(), 0), ReadCounts(fx));
        ASSERT_EQ(TVector<ui32>(fx.Size(), 0), WriteCounts(fx));
    }

    // So is the replay: a device ahead of the others whose journal cannot be
    // read stops it.
    {
        TQuorumFixture first;
        TQuorumFixture fx(false /* init */);
        fx[0].ReadRespQueue.push_back(ClaimedAt(Claim(first[0]), 4));
        *fx[0].ReadJournalTailResp.MutableError() =
            MakeError(E_ARGUMENT, "no journal");

        auto error = InitFails(fx);
        ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();

        // An uninitialised group serves nobody.
        TVector<TPageGroup> pageGroups;
        error = Read(*fx.Group, &pageGroups);
        ASSERT_EQ(E_REJECTED, error.GetCode()) << error.GetMessage();
    }
}

FIBER_TEST(QuorumGroupTest, FirstRecordAfterInitIsNotConfusedWithTheClaim)
{
    TQuorumFixture fx(false /* init */);
    const auto init = fx.Group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode())
        << init.GetError().GetMessage();
    fx.ForgetRequests();

    fx[2].Paused = true;
    auto error = WritePages(*fx.Group, init.GetResult() + 1, PageNo, {"fresh"});
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    TVector<TPageGroup> pageGroups;
    for (ui32 i = 0; i < 2 * DeviceCount; ++i) {
        error = Read(*fx.Group, &pageGroups);
        ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    }
    ASSERT_EQ(0U, fx[2].ReadCalls.size()) << "served by the replica behind";

    fx[2].Unpause();
    ASSERT_TRUE(WaitFor([&] { return fx[2].WriteCount() == 1; }));
}

FIBER_TEST(QuorumGroupTest, InitRejectsADeviceClaimedByAnotherGroup)
{
    TQuorumFixture first;
    const TString claim = Claim(first[0]);

    // A device claimed as dev-a, offered to a group that calls it dev-b.
    {
        TQuorumFixture other(false /* init */);
        other[1].ReadResp = PageResponse(0, claim);

        auto error = InitFails(other);
        ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
        ASSERT_TRUE(Mentions(error, other.DeviceUUIDs[1])) << error.GetMessage();
        ASSERT_EQ(0U, other[1].WriteCount());
    }

    // The same claim, offered to a group that uses a different page size.
    {
        TQuorumFixture other(false /* init */, MakeConfig(8_KB));
        TString wide = claim;
        wide.resize(8_KB, '\0');
        other[0].ReadResp = PageResponse(0, wide);

        auto error = InitFails(other);
        ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
        ASSERT_TRUE(Mentions(error, other.DeviceUUIDs[0])) << error.GetMessage();
        ASSERT_EQ(0U, other[0].WriteCount());
    }

    // A first page holding something nobody here wrote is left alone too,
    // rather than being taken for a blank device and claimed.
    {
        TQuorumFixture fx(false /* init */);
        fx[0].ReadResp = PageResponse(0, TString(DefaultBlockSize, 'x'));

        auto error = InitFails(fx);
        ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
        ASSERT_TRUE(Mentions(error, fx.DeviceUUIDs[0])) << error.GetMessage();
        ASSERT_EQ(0U, fx[0].WriteCount());
    }
}

FIBER_TEST(QuorumGroupTest, InitRejectsAClaimItCannotUnderstand)
{
    //
    // The only case that needs the on-disk layout. A claim written by a newer
    // version, or by a kind of group this build does not have, cannot be
    // produced through the interface, so both are made by taking a claim this
    // build did write and moving one field out of range.
    //
    // The baseline is the claim dev-c wrote for itself, so the only thing
    // wrong with it afterwards is the field under test.
    TQuorumFixture first;
    const TString good = Claim(first[2]);
    ASSERT_GE(good.size(), sizeof(TStorageGroupHeader));

    for (TStringBuf field: {"version", "group type"}) {
        TStorageGroupHeader header;
        memcpy(&header, good.data(), sizeof(header));
        if (field == "version") {
            header.Version = TStorageGroupHeader::CurrentVersion + 1;
        } else {
            header.GroupType = header.GroupType + 1;
        }

        TString claim = good;
        memcpy(claim.begin(), &header, sizeof(header));

        TQuorumFixture fx(false /* init */);
        fx[2].ReadResp = PageResponse(0, claim);

        auto error = InitFails(fx);
        ASSERT_EQ(E_INVALID_STATE, error.GetCode())
            << field << ": " << error.GetMessage();
        ASSERT_TRUE(Mentions(error, fx.DeviceUUIDs[2])) << error.GetMessage();
        ASSERT_EQ(0U, fx[2].WriteCount()) << field << ": claimed anyway";
    }
}

FIBER_TEST(QuorumGroupTest, InitFailsWhenTheFirstPageCannotBeReadOrWritten)
{
    // The read itself fails.
    {
        TQuorumFixture fx(false /* init */);
        *fx[0].ReadResp.MutableError() = MakeError(E_IO, "media error");

        auto error = InitFails(fx);
        ASSERT_EQ(E_IO, error.GetCode()) << error.GetMessage();
        ASSERT_TRUE(Mentions(error, fx.DeviceUUIDs[0])) << error.GetMessage();
        ASSERT_EQ(0U, fx[0].WriteCount()) << "claimed despite the error";
    }

    // The device answers with less than a page.
    {
        TQuorumFixture fx(false /* init */);
        fx[2].ReadResp = PageResponse(0, TString(100, 'x'));

        auto error = InitFails(fx);
        ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
        ASSERT_TRUE(Mentions(error, fx.DeviceUUIDs[2])) << error.GetMessage();
        ASSERT_EQ(0U, fx[2].WriteCount()) << "claimed despite the error";
    }

    // Claiming a blank device is refused outright.
    {
        TQuorumFixture fx(false /* init */);
        fx[1].WriteRespQueue.emplace_back();
        *fx[1].WriteRespQueue.back().MutableError() =
            MakeError(E_ARGUMENT, "read only");

        auto error = InitFails(fx);
        ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
        ASSERT_TRUE(Mentions(error, fx.DeviceUUIDs[1])) << error.GetMessage();
        ASSERT_EQ(1U, fx[1].WriteCount());
    }

    // A retriable refusal is retried and the device ends up claimed.
    {
        TQuorumFixture fx(false /* init */);
        fx[1].WriteRespQueue.emplace_back();
        *fx[1].WriteRespQueue.back().MutableError() =
            MakeError(E_REJECTED, "busy");

        const auto init = fx.Group->Init();
        ASSERT_EQ(S_OK, init.GetError().GetCode())
            << init.GetError().GetMessage();
        ASSERT_EQ(2U, fx[1].WriteCount());
        ASSERT_EQ(Backoffs(1), Sleeps(fx));
    }
}

FIBER_TEST(QuorumGroupTest, InitValidatesEveryDeviceEvenIfOneFails)
{
    TQuorumFixture fx(false /* init */);
    fx[0].ReadResp = PageResponse(0, TString(DefaultBlockSize, 'x'));

    auto error = InitFails(fx);
    ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
    ASSERT_TRUE(Mentions(error, fx.DeviceUUIDs[0])) << error.GetMessage();

    // The others are still looked at, and the blank ones are still claimed:
    // validation fans out and joins rather than stopping at the first answer.
    ASSERT_EQ(TVector<ui32>(fx.Size(), 1), ReadCounts(fx));
    ASSERT_EQ((TVector<ui32>{0, 1, 1}), WriteCounts(fx));
}

FIBER_TEST(QuorumGroupTest, InitReplaysTheTailOntoDevicesBehind)
{
    // A claimed device that kept three records the blank ones never took.
    TQuorumFixture first;
    TQuorumFixture fx(false /* init */);
    fx[0].ReadRespQueue.push_back(ClaimedAt(Claim(first[0]), 4));
    for (ui64 lsn = 2; lsn <= 4; ++lsn) {
        auto* record = fx[0].ReadJournalTailResp.AddRecords();
        record->SetLogSequenceNumber(lsn);
        record->SetPrevLogSequenceNumber(lsn - 1);
        auto* pg = record->AddPageGroups();
        pg->SetFirstPageNo(100 + lsn);
        pg->AddContent(TStringBuilder() << "record" << lsn);
    }

    const auto init = fx.Group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode())
        << init.GetError().GetMessage();
    ASSERT_EQ(4U, init.GetResult());

    // The devices behind take the claim and then the three records, each
    // going back exactly as the device that kept it holds it, rather than
    // being placed past the group's own pages a second time.
    ASSERT_EQ((TVector<ui32>{0, 4, 4}), WriteCounts(fx));
    for (ui32 i = 1; i < DeviceCount; ++i) {
        ASSERT_EQ((TVector<ui64>{1, 2, 3, 4}), fx[i].Lsns()) << "dev " << i;
        for (ui64 lsn = 2; lsn <= 4; ++lsn) {
            const auto& write = fx[i].WriteCalls.at(lsn - 1);
            ASSERT_EQ(lsn - 1, write.GetPrevLogSequenceNumber())
                << "dev " << i << " lsn " << lsn;
            ASSERT_EQ(100 + lsn, write.GetPageGroups(0).GetFirstPageNo())
                << "dev " << i << " lsn " << lsn;
            ASSERT_EQ(
                TString(TStringBuilder() << "record" << lsn),
                write.GetPageGroups(0).GetContent(0))
                << "dev " << i << " lsn " << lsn;
        }
    }

    // And every device serves once it is level.
    ASSERT_EQ(S_OK, ReadFromEachReplica(fx).GetCode());
    ASSERT_EQ(TVector<ui32>(fx.Size(), 1), ReadCounts(fx));
}

FIBER_TEST(QuorumGroupTest, InitFailsIfTheJournalCannotBridgeTheGap)
{
    TQuorumFixture first;
    const TString claim = Claim(first[0]);

    // A device that is ahead but keeps no record past the claim, as after
    // being told the record is safe everywhere and then losing it: it has
    // nothing to bring the others up with.
    {
        TQuorumFixture fx(false /* init */);
        fx[0].ReadRespQueue.push_back(ClaimedAt(claim, Lsn));

        auto error = InitFails(fx);
        ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
        ASSERT_EQ((TVector<ui32>{0, 1, 1}), WriteCounts(fx))
            << "a replay was attempted";
    }

    // A refused replay fails Init, but every other replay still runs to
    // completion first: the fan-out is joined, not abandoned.
    {
        TQuorumFixture fx(false /* init */);
        fx[0].ReadRespQueue.push_back(ClaimedAt(claim, 4));
        for (ui64 lsn = 2; lsn <= 4; ++lsn) {
            auto* record = fx[0].ReadJournalTailResp.AddRecords();
            record->SetLogSequenceNumber(lsn);
            record->SetPrevLogSequenceNumber(lsn - 1);
        }
        // The claim goes through, the replay does not.
        fx[2].WriteRespQueue.emplace_back();
        *fx[2].WriteResp.MutableError() = MakeError(E_ARGUMENT, "read only");

        auto error = InitFails(fx);
        ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
        ASSERT_TRUE(Mentions(error, fx.DeviceUUIDs[2])) << error.GetMessage();

        ASSERT_EQ((TVector<ui32>{0, 4, 2}), WriteCounts(fx))
            << "the healthy replay was abandoned";

        TVector<TPageGroup> pageGroups;
        error = Read(*fx.Group, &pageGroups);
        ASSERT_EQ(E_REJECTED, error.GetCode()) << error.GetMessage();
    }
}

FIBER_TEST(QuorumGroupTest, TearDownBeforeInitReleasesAndReturns)
{
    TQuorumFixture fx(false /* init */);
    fx.Group->TearDown();

    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(1U, fx[i].ReleaseCalls.size()) << "dev " << i;
        ASSERT_EQ(fx.DeviceUUIDs[i], fx[i].ReleaseCalls[0].GetDeviceUUIDs(0));
    }
}

FIBER_TEST(QuorumGroupTest, WriteReturnsOnMajorityAndKeepsLaggardsOutOfReads)
{
    TQuorumFixture fx;
    fx.ForgetRequests();

    fx[2].Paused = true;
    auto error = WritePages(*fx.Group, Lsn, PageNo, {"fresh"});
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    // Two acks are enough to return; the third record is still in flight.
    ASSERT_EQ((TVector<ui32>{1, 1, 0}), WriteCounts(fx));

    // The record goes out with the link the caller built.
    for (ui32 i = 0; i < 2; ++i) {
        ASSERT_EQ(Lsn - 1, fx[i].WriteCalls.back().GetPrevLogSequenceNumber())
            << "dev " << i;
    }

    // The replica that has not taken the record must not answer with the page
    // as it was before. The cursor still advances over it, so the eligible two
    // do not get an even share, only a share each.
    TVector<TPageGroup> pageGroups;
    for (ui32 i = 0; i < 2 * DeviceCount; ++i) {
        error = Read(*fx.Group, &pageGroups);
        ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    }
    ASSERT_EQ(0U, fx[2].ReadCalls.size());
    ASSERT_GT(fx[0].ReadCalls.size(), 0U);
    ASSERT_GT(fx[1].ReadCalls.size(), 0U);

    // Once it catches up it is back in the rotation.
    fx[2].Unpause();
    ASSERT_TRUE(WaitFor(
        [&]
        {
            error = Read(*fx.Group, &pageGroups);
            return HasError(error) || !fx[2].ReadCalls.empty();
        }));
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    ASSERT_EQ(S_OK, ReadFromEachReplica(fx).GetCode());
    ASSERT_EQ(TVector<ui32>(fx.Size(), 1), ReadCounts(fx));
}

FIBER_TEST(QuorumGroupTest, MajorityIsMoreThanHalfOfTheDevices)
{
    // Two devices leave no room for a straggler: the majority is both.
    {
        TQuorumFixture fx(2U /* devices */);
        fx[1].Paused = true;

        silk::FiberFuture write;
        StartWrite(fx, Lsn, &write);
        ASSERT_TRUE(StillRunning(write)) << "acked without the second device";

        fx[1].Unpause();
        ASSERT_EQ(0, write.wait());
    }

    // Four tolerate one straggler.
    {
        TQuorumFixture fx(4U /* devices */);
        fx[3].Paused = true;

        auto error = Write(*fx.Group);
        ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

        fx[3].Unpause();
        ASSERT_TRUE(WaitFor([&] { return fx[3].WriteCount() == 2; }));
    }

    // But not two: half the devices is not a majority.
    {
        TQuorumFixture fx(4U /* devices */);
        fx[2].Paused = true;
        fx[3].Paused = true;

        silk::FiberFuture write;
        StartWrite(fx, Lsn, &write);
        ASSERT_TRUE(StillRunning(write)) << "acked on half the devices";

        fx[2].Unpause();
        ASSERT_EQ(0, write.wait());

        fx[3].Unpause();
        ASSERT_TRUE(WaitFor([&] { return fx[3].WriteCount() == 2; }));
    }
}

FIBER_TEST(QuorumGroupTest, ReadRetriesThenFailsOverWithinEligibleReplicas)
{
    TQuorumFixture fx;
    fx.ForgetRequests();

    // A retriable error is the same replica's problem to fix: it is retried
    // there rather than handed to the next one.
    fx[0].ReadRespQueue.emplace_back();
    *fx[0].ReadRespQueue.back().MutableError() = MakeError(E_REJECTED, "busy");

    TVector<TPageGroup> pageGroups;
    auto error = Read(*fx.Group, &pageGroups);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    ASSERT_EQ((TVector<ui32>{2, 0, 0}), ReadCounts(fx));
    ASSERT_EQ(Backoffs(1), Sleeps(fx));

    for (auto& device: fx.Nodes) {
        device->ReadCalls.clear();
    }

    // A replica that is behind is not a fallback: answering with the page as
    // it was before the last record is worse than failing the read.
    fx[2].Paused = true;
    error = Write(*fx.Group);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    *fx[0].ReadResp.MutableError() = MakeError(E_ARGUMENT, "bad range");
    *fx[1].ReadResp.MutableError() = MakeError(E_ARGUMENT, "bad range");

    error = Read(*fx.Group, &pageGroups);
    ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
    ASSERT_EQ(0U, fx[2].ReadCalls.size());
    ASSERT_GT(fx[0].ReadCalls.size(), 0U);
    ASSERT_GT(fx[1].ReadCalls.size(), 0U);

    // A failed read is not a failed device, so the group stays usable.
    error = Write(*fx.Group, Lsn + 1);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    fx[2].Unpause();
    ASSERT_TRUE(WaitFor([&] { return fx[2].WriteCount() == 2; }));
}

FIBER_TEST(QuorumGroupTest, ConcurrentWritesAckIndependently)
{
    // Records landing in any order all get their own majority.
    {
        TQuorumFixture fx;
        fx.ForgetRequests();
        for (ui32 i = 0; i < DeviceCount; ++i) {
            fx[i].Paused = true;
        }

        TVector<silk::FiberFuture> futures(3);
        StartWrite(fx, Lsn + 2, &futures[0]);
        StartWrite(fx, Lsn, &futures[1]);
        StartWrite(fx, Lsn + 1, &futures[2]);

        ASSERT_TRUE(WaitFor([&] { return TotalParked(fx) == 3 * DeviceCount; }));

        for (ui32 i = 0; i < DeviceCount; ++i) {
            fx[i].Unpause();
        }
        for (auto& future: futures) {
            ASSERT_EQ(0, future.wait());
        }

        ASSERT_TRUE(
            WaitFor([&] { return TotalWrites(fx) == 3 * DeviceCount; }));
    }

    // An ack for a later record is not an ack for an earlier one.
    {
        TQuorumFixture fx;
        fx.ForgetRequests();
        for (ui32 i = 0; i < DeviceCount; ++i) {
            fx[i].HoldLsn = Lsn;
        }

        silk::FiberFuture held;
        silk::FiberFuture free;
        StartWrite(fx, Lsn, &held);
        StartWrite(fx, Lsn + 1, &free);

        ASSERT_EQ(0, free.wait());
        ASSERT_TRUE(StillRunning(held)) << "held write acked on foreign acks";

        for (ui32 i = 0; i < DeviceCount; ++i) {
            fx[i].Unpause();
        }
        ASSERT_EQ(0, held.wait());
        ASSERT_TRUE(
            WaitFor([&] { return TotalWrites(fx) == 2 * DeviceCount; }));
    }
}

FIBER_TEST(QuorumGroupTest, AnyDeviceWriteFailureBreaksTheGroup)
{
    // A failure that arrives before the majority fails the write itself.
    {
        TQuorumFixture fx;
        fx.ForgetRequests();
        fx[1].Paused = true;
        *fx[2].WriteResp.MutableError() = MakeError(E_ARGUMENT, "read only");

        auto error = Write(*fx.Group);
        ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
        ASSERT_TRUE(Mentions(error, fx.DeviceUUIDs[2])) << error.GetMessage();

        // And the group stays broken for everything that follows.
        error = Write(*fx.Group, Lsn + 1);
        ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();

        TVector<TPageGroup> pageGroups;
        error = Read(*fx.Group, &pageGroups);
        ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();

        fx[1].Unpause();
        ASSERT_TRUE(WaitFor([&] { return fx[1].WriteCount() == 1; }));
    }

    // A failure that arrives after it does not un-ack the caller, but still
    // breaks the group: the devices have diverged.
    {
        TQuorumFixture fx;
        fx[2].Paused = true;
        *fx[2].WriteResp.MutableError() = MakeError(E_ARGUMENT, "read only");

        auto error = Write(*fx.Group);
        ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

        fx[2].Unpause();
        ASSERT_TRUE(WaitFor(
            [&]
            {
                TVector<TPageGroup> pageGroups;
                return HasError(Read(*fx.Group, &pageGroups));
            }));

        error = Write(*fx.Group, Lsn + 1);
        ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
        ASSERT_TRUE(Mentions(error, fx.DeviceUUIDs[2])) << error.GetMessage();
    }
}

FIBER_TEST(QuorumGroupTest, RejectsRequestsItCannotAddress)
{
    TQuorumFixture fx;
    fx.ForgetRequests();

    // Lsn zero is what an unwritten record looks like, so it is refused.
    auto error = WritePages(*fx.Group, 0, PageNo, {"page1"});
    ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();

    // So is a page range the group cannot place on a device without wrapping
    // around.
    error = WritePages(*fx.Group, Lsn, Max<ui64>(), {"page1"});
    ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();

    TVector<TPageGroup> pageGroups;
    error = ReadRange(*fx.Group, Max<ui64>() - 1, 2, &pageGroups);
    ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();

    ASSERT_EQ(TVector<ui32>(fx.Size(), 0), WriteCounts(fx));
    ASSERT_EQ(TVector<ui32>(fx.Size(), 0), ReadCounts(fx));
}

FIBER_TEST(QuorumGroupTest, ForwardsTheCallersLinkAndRefusesABrokenOne)
{
    TQuorumFixture fx;
    fx.ForgetRequests();

    // Which record this one follows is the caller's business, and a link
    // that skips a number goes out exactly as given.
    {
        TPageGroup pageGroup{.FirstPageNo = PageNo};
        pageGroup.Content.emplace_back("page1", 5U /* len */);
        TVector<TPageGroup> pageGroups;
        pageGroups.push_back(std::move(pageGroup));

        auto error = fx.Group->WriteLogRecord(
            NoHeaders,
            std::move(pageGroups),
            {.Lsn = Lsn + 1, .PrevLsn = Lsn - 1});
        ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    }
    ASSERT_TRUE(WaitFor([&] { return TotalWrites(fx) == DeviceCount; }));
    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(Lsn + 1, fx[i].WriteCalls.back().GetLogSequenceNumber());
        ASSERT_EQ(Lsn - 1, fx[i].WriteCalls.back().GetPrevLogSequenceNumber());
    }

    // A link that does not move forward is refused before any device sees it.
    {
        TPageGroup pageGroup{.FirstPageNo = PageNo};
        pageGroup.Content.emplace_back("page1", 5U /* len */);
        TVector<TPageGroup> pageGroups;
        pageGroups.push_back(std::move(pageGroup));

        auto error = fx.Group->WriteLogRecord(
            NoHeaders,
            std::move(pageGroups),
            {.Lsn = Lsn, .PrevLsn = Lsn});
        ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
    }
    ASSERT_EQ(TVector<ui32>(DeviceCount, 1), WriteCounts(fx));
}

FIBER_TEST(QuorumGroupTest, RefusesAReservedPageFromADevice)
{
    TQuorumFixture fx;

    // A device answering with a page group the caller never asked for would
    // come back as a page number that does not exist, so it is dropped.
    for (auto& device: fx.Nodes) {
        device->ReadResp = PageResponse(0, "the claim");
    }

    TVector<TPageGroup> pageGroups;
    auto error = ReadRange(*fx.Group, 0, 1, &pageGroups);
    ASSERT_EQ(E_FAIL, error.GetCode()) << error.GetMessage();
    ASSERT_TRUE(pageGroups.empty());
}

FIBER_TEST(QuorumGroupTest, LowWatermarkFollowsTheSlowestDevice)
{
    auto timer = std::make_shared<TTickTimer>();
    TQuorumFixture fx(timer, MakeConfigWithWaterMarksLoop());

    // Everything every device already holds may be trimmed from the start.
    ASSERT_TRUE(timer->TickUntil(
        [&] { return !fx[0].AdvanceLsnLowWatermarkCalls.empty(); }));
    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(1U, fx[i].AdvanceLsnLowWatermarkCalls.size()) << "dev " << i;
        ASSERT_EQ(
            1U,
            fx[i].AdvanceLsnLowWatermarkCalls[0].GetLsnLowWatermark())
            << "dev " << i;
    }

    // A record one device is still missing may not be trimmed anywhere: a
    // trimmed peer could never replay it to that device.
    fx[2].Paused = true;
    auto error = Write(*fx.Group);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    timer->TickOnce();
    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(1U, fx[i].AdvanceLsnLowWatermarkCalls.size()) << "dev " << i;
    }

    fx[2].Unpause();
    ASSERT_TRUE(timer->TickUntil(
        [&] { return fx[0].AdvanceLsnLowWatermarkCalls.size() == 2; }));
    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(2U, fx[i].AdvanceLsnLowWatermarkCalls.size()) << "dev " << i;
        ASSERT_EQ(
            Lsn,
            fx[i].AdvanceLsnLowWatermarkCalls[1].GetLsnLowWatermark())
            << "dev " << i;
    }

    // Nothing moved, so nothing is pushed again.
    timer->TickOnce();
    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(2U, fx[i].AdvanceLsnLowWatermarkCalls.size()) << "dev " << i;
    }
}

FIBER_TEST(QuorumGroupTest, LowWatermarkMissedByADeviceIsSupersededNotRetried)
{
    auto timer = std::make_shared<TTickTimer>();
    TQuorumFixture fx(timer, MakeConfigWithWaterMarksLoop());
    *fx[2].AdvanceLsnLowWatermarkResp.MutableError() =
        MakeError(E_REJECTED, "busy");

    ASSERT_TRUE(timer->TickUntil(
        [&] { return !fx[2].AdvanceLsnLowWatermarkCalls.empty(); }));
    ASSERT_EQ(1U, fx[2].AdvanceLsnLowWatermarkCalls.size());

    // The refusal was retriable, so the group does not break, and the missed
    // watermark is not resent on its own.
    fx[2].AdvanceLsnLowWatermarkResp = {};
    timer->TickOnce();
    ASSERT_EQ(1U, fx[2].AdvanceLsnLowWatermarkCalls.size());

    // The next one carries the device forward anyway.
    auto error = Write(*fx.Group);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    ASSERT_TRUE(timer->TickUntil(
        [&] { return fx[2].AdvanceLsnLowWatermarkCalls.size() == 2; }));
    ASSERT_EQ(
        Lsn,
        fx[2].AdvanceLsnLowWatermarkCalls[1].GetLsnLowWatermark());
}

FIBER_TEST(QuorumGroupTest, LowWatermarkRefusedOutrightBreaksTheGroup)
{
    auto timer = std::make_shared<TTickTimer>();
    TQuorumFixture fx(timer, MakeConfigWithWaterMarksLoop());
    *fx[2].AdvanceLsnLowWatermarkResp.MutableError() =
        MakeError(E_ARGUMENT, "unknown device");

    ASSERT_TRUE(timer->TickUntil(
        [&] { return !fx[2].AdvanceLsnLowWatermarkCalls.empty(); }));

    // A device that refuses to trim is a device we can no longer reason about.
    const ui32 writes = TotalWrites(fx);
    auto error = Write(*fx.Group);
    ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
    ASSERT_TRUE(Mentions(error, fx.DeviceUUIDs[2])) << error.GetMessage();
    ASSERT_EQ(writes, TotalWrites(fx));
}

FIBER_TEST(QuorumGroupTest, ShouldRenewSessions)
{
    auto timer = std::make_shared<TTickTimer>(DeviceCount);
    TQuorumFixture fx(timer, MakeConfigWithReacquireLoop());

    // A storage node lets go of a claim that stops being renewed, so Init's
    // acquire is repeated on every device for as long as the group lives.
    timer->TickOnce();
    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(2U, fx[i].AcquireCalls.size()) << "dev " << i;
        const auto& renewal = fx[i].AcquireCalls[1];
        ASSERT_EQ(1U, renewal.DeviceUUIDsSize()) << "dev " << i;
        ASSERT_EQ(fx.DeviceUUIDs[i], renewal.GetDeviceUUIDs(0)) << "dev " << i;
        ASSERT_EQ(42U, renewal.GetGeneration()) << "dev " << i;
        ASSERT_EQ("test-client", renewal.GetHeaders().GetClientId())
            << "dev " << i;
    }

    // A retriable or aborted refusal is no verdict on the claim: the group
    // carries on, and the next round is the retry.
    *fx[2].AcquireResp.MutableError() = MakeError(E_REJECTED, "busy");
    timer->TickOnce();
    auto error = Write(*fx.Group);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    *fx[2].AcquireResp.MutableError() = MakeError(E_ABORTED, "shutting down");
    timer->TickOnce();
    error = Write(*fx.Group, Lsn + 1);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    fx[2].AcquireResp = {};
    timer->TickOnce();
    ASSERT_EQ(5U, fx[2].AcquireCalls.size());

    // The device belongs to someone else now, which is not a device this
    // group can go on writing to.
    *fx[2].AcquireResp.MutableError() =
        MakeError(E_BS_MOUNT_CONFLICT, "another writer");
    timer->ReleaseRound();
    TVector<TPageGroup> pageGroups;
    ASSERT_TRUE(
        WaitFor([&] { return HasError(Read(*fx.Group, &pageGroups)); }));
    error = Write(*fx.Group, Lsn + 2);
    ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
    ASSERT_TRUE(Mentions(error, fx.DeviceUUIDs[2])) << error.GetMessage();
}

FIBER_TEST(QuorumGroupTest, ShouldRenewSessionsIndependently)
{
    auto config = MakeConfig();
    config.ReacquirePeriod = TDuration::MilliSeconds(1);
    TGroupFixture fx(
        CreateQuorumMirroredStorageGroup,
        config,
        CreateFiberTimer(),
        DeviceCount);

    auto error = fx.Group->Init().GetError();
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    // Each device renews on a clock of its own: a node that does not answer
    // holds up only its own renewal.
    fx[2].AcquirePaused = true;
    fx[2].AcquireParked.wait();

    const ui64 renewals0 = fx[0].Acquires.get();
    const ui64 renewals1 = fx[1].Acquires.get();
    const ui64 renewals2 = fx[2].Acquires.get();
    ASSERT_EQ(0, fx[0].Acquires.wait(renewals0 + 2));
    ASSERT_EQ(0, fx[1].Acquires.wait(renewals1 + 2));
    ASSERT_EQ(renewals2, fx[2].Acquires.get());
}

FIBER_TEST(QuorumGroupTest, ShouldCutSleepsShortOnTearDown)
{
    // Counts the cancellable sleeps, so the test knows the loops and the
    // backoff are under way before it tears down.
    struct TCountingTimer: ITimer
    {
        ITimerPtr Inner = CreateFiberTimer();
        std::atomic<ui32> Sleeps = 0;

        TInstant Now() override
        {
            return Inner->Now();
        }

        void Sleep(TDuration duration) override
        {
            Inner->Sleep(duration);
        }

        void Sleep(
            TDuration duration,
            const std::atomic<bool>& cancelled) override
        {
            ++Sleeps;
            Inner->Sleep(duration, cancelled);
        }
    };

    auto config = MakeConfig();
    config.ReacquirePeriod = TDuration::Hours(1);
    config.LowWatermarkPeriod = TDuration::Hours(1);
    config.RetryPolicy.BackoffIncrement = TDuration::Hours(1);
    config.RetryPolicy.TotalTimeout = TDuration::Hours(2);
    auto timer = std::make_shared<TCountingTimer>();
    TGroupFixture fx(
        CreateQuorumMirroredStorageGroup,
        config,
        timer,
        DeviceCount);

    auto error = fx.Group->Init().GetError();
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    ASSERT_TRUE(WaitFor([&] { return timer->Sleeps == DeviceCount + 1; }));

    // A write whose third replica is in a backoff for an hour.
    *fx[2].WriteResp.MutableError() = MakeError(E_REJECTED, "busy");
    error = Write(*fx.Group);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    ASSERT_TRUE(WaitFor([&] { return timer->Sleeps == DeviceCount + 2; }));

    // Neither the loops nor the backoff hold TearDown past a slice.
    const TInstant start = TInstant::Now();
    fx.Group->TearDown();
    ASSERT_LT(TInstant::Now() - start, TDuration::Seconds(5));
    ASSERT_EQ(1U, fx[2].ReleaseCalls.size());
}
