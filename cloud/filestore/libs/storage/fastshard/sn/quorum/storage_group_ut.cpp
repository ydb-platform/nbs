#include <cloud/filestore/libs/storage/fastshard/sn/quorum/storage_group.h>
#include <cloud/filestore/libs/storage/fastshard/sn/quorum/storage_group_helpers.h>
#include <cloud/filestore/libs/storage/fastshard/sn/quorum/storage_group_quorum.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/fastshard/sn/iface/storage_node.h>
#include <cloud/fastshard/testlib/fake_storage_node.h>
#include <cloud/fastshard/testlib/silk_env.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <silk/fibers/event.h>
#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <util/generic/hash.h>
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

// The page the tests read and write, and the record they write it with.
constexpr ui64 PageNo = 111;
constexpr ui64 Lsn = 1234;

const NProto::TDeviceRequestHeaders NoHeaders;

////////////////////////////////////////////////////////////////////////////////

/**
 * A storage device: the pages it holds, the records it has taken, and the
 * requests it has been sent.
 *
 * It answers from its own state, so a test sets a device up by driving a group
 * over it and then checks what is left behind. The position it reports is the
 * last watermark the group pushed to it, as on the real device, not the last
 * record it took. Two escapes let a test play a faulty device, in this order:
 *
 *   - an error left in one of the inherited response fields is returned as is;
 *   - a scripted reply carrying a body of its own is returned instead of the
 *     real answer.
 *
 * WriteLogRecord also parks the calling fiber while the gate is shut, which
 * lets a test hold one replica back and watch what the group does with the
 * others.
 */
struct TFakeDevice: TFakeStorageNode
{
    THashMap<ui64, TString> Pages;
    TVector<NProto::TJournalRecord> Records;
    ui64 Watermark = 0;

    silk::FiberEvent Gate;
    std::atomic<bool> Paused = false;
    std::atomic<ui64> HoldLsn = 0;
    std::atomic<ui32> Parked = 0;

    TString Page(ui64 pageNo)
    {
        with_lock (Lock) {
            const auto* page = Pages.FindPtr(pageNo);
            return page ? *page : TString();
        }
    }

    TVector<ui64> Lsns()
    {
        TVector<ui64> lsns;
        with_lock (Lock) {
            for (const auto& record: Records) {
                lsns.push_back(record.GetLogSequenceNumber());
            }
        }
        return lsns;
    }

    ui64 LastLsn()
    {
        with_lock (Lock) {
            return Records.empty()
                ? 0
                : Records.back().GetLogSequenceNumber();
        }
    }

    // Everything after @p count records is gone, pages included, as after a
    // crash that took the tail of the device with it.
    void LoseTailAfter(size_t count)
    {
        with_lock (Lock) {
            for (size_t i = count; i < Records.size(); ++i) {
                for (const auto& pg: Records[i].GetPageGroups()) {
                    for (size_t j = 0; j < pg.ContentSize(); ++j) {
                        Pages.erase(pg.GetFirstPageNo() + j);
                    }
                }
            }
            Records.resize(count);
        }
    }

    void Unpause()
    {
        Paused = false;
        HoldLsn = 0;
        Gate.set();
    }

    NProto::TReadPagesResponse ReadPages(
        NProto::TReadPagesRequest request) override
    {
        const auto refs = request.GetPageGroupRefs();
        auto response = TFakeStorageNode::ReadPages(std::move(request));
        if (HasError(response.GetError()) || response.PageGroupsSize()) {
            return response;
        }

        with_lock (Lock) {
            for (const auto& ref: refs) {
                auto* pg = response.AddPageGroups();
                pg->SetFirstPageNo(ref.GetFirstPageNo());
                for (ui64 i = 0; i < ref.GetPageCount(); ++i) {
                    const auto* page = Pages.FindPtr(ref.GetFirstPageNo() + i);
                    pg->AddContent(
                        page ? *page : TString(ref.GetPageSize(), '\0'));
                }
            }
        }

        return response;
    }

    NProto::TWriteLogRecordResponse WriteLogRecord(
        NProto::TWriteLogRecordRequest request) override
    {
        const ui64 lsn = request.GetLogSequenceNumber();
        const ui64 held = HoldLsn;
        if (Paused || (held && held == lsn)) {
            ++Parked;
            Gate.wait();
        }

        NProto::TJournalRecord record;
        *record.MutablePageGroups() = request.GetPageGroups();
        record.SetLogSequenceNumber(lsn);
        record.SetPrevLogSequenceNumber(request.GetPrevLogSequenceNumber());

        auto response = TFakeStorageNode::WriteLogRecord(std::move(request));
        if (HasError(response.GetError())) {
            return response;
        }

        with_lock (Lock) {
            for (const auto& pg: record.GetPageGroups()) {
                for (size_t i = 0; i < pg.ContentSize(); ++i) {
                    Pages[pg.GetFirstPageNo() + i] = pg.GetContent(i);
                }
            }
            Records.push_back(std::move(record));
        }

        return response;
    }

    NProto::TReadJournalTailResponse ReadJournalTail(
        NProto::TReadJournalTailRequest request) override
    {
        const ui64 afterLsn = request.GetAfterLogSequenceNumber();
        const ui32 maxRecords = request.GetMaxRecordCount();

        auto response = TFakeStorageNode::ReadJournalTail(std::move(request));
        if (HasError(response.GetError()) || response.RecordsSize() ||
            response.GetLastAckedLogSequenceNumber())
        {
            return response;
        }

        with_lock (Lock) {
            TVector<const NProto::TJournalRecord*> tail;
            for (const auto& record: Records) {
                if (record.GetLogSequenceNumber() > afterLsn) {
                    tail.push_back(&record);
                }
            }
            if (maxRecords && tail.size() > maxRecords) {
                tail.erase(tail.begin(), tail.end() - maxRecords);
            }

            for (const auto* record: tail) {
                *response.AddRecords() = *record;
            }
            response.SetLastAckedLogSequenceNumber(Watermark);
        }

        return response;
    }

    NProto::TAdvanceLsnLowWatermarkResponse AdvanceLsnLowWatermark(
        NProto::TAdvanceLsnLowWatermarkRequest request) override
    {
        const ui64 watermark = request.GetLsnLowWatermark();
        auto response =
            TFakeStorageNode::AdvanceLsnLowWatermark(std::move(request));
        if (!HasError(response.GetError())) {
            with_lock (Lock) {
                Watermark = watermark;
            }
        }

        return response;
    }
};

using TFakeDevicePtr = std::shared_ptr<TFakeDevice>;

////////////////////////////////////////////////////////////////////////////////

// The watermark loop is off unless a test asks for it.
TStorageGroupConfig MakeConfig(ui32 pageSize = DefaultBlockSize)
{
    TStorageGroupConfig config;
    config.ClientId = "test-client";
    config.AcquireGeneration = 42;
    config.LowWatermarkPeriod = TDuration::Zero();
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

////////////////////////////////////////////////////////////////////////////////

/**
 * A timer whose Sleep waits for the test to call TickOnce, which lets the
 * watermark loop run exactly one iteration and returns once it is parked in
 * Sleep again. Sleep announces itself before waiting, so the loop is parked
 * exactly when Sleeps == Ticks + 1.
 */
struct TTickTimer: ITimer
{
    silk::FiberSequencer Ticks;    // test to loop
    silk::FiberSequencer Sleeps;   // loop to test

    TInstant Now() override
    {
        return TInstant::Now();
    }

    void Sleep(TDuration duration) override
    {
        Y_UNUSED(duration);
        Y_UNUSED(Ticks.wait(Sleeps.increment()));
    }

    void TickOnce()
    {
        Y_UNUSED(Sleeps.wait(Ticks.get() + 1));
        Y_UNUSED(Sleeps.wait(Ticks.increment() + 1));
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

    // Lets the loop out of Sleep for good, so TearDown can join it.
    void Stop()
    {
        Ticks.stop();
    }
};

using TTickTimerPtr = std::shared_ptr<TTickTimer>;

////////////////////////////////////////////////////////////////////////////////
// Fixtures: a group over a set of devices, blank ones by default.

using TGroupFactory = IStorageGroupPtr (*)(
    TStorageGroupConfig,
    TVector<TStorageDevice>,
    ITimerPtr);

struct TGroupFixture
{
    TVector<TFakeDevicePtr> Devices;
    TVector<TString> DeviceUUIDs;
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    IStorageGroupPtr Group;

    TGroupFixture(
            TGroupFactory createGroup,
            TStorageGroupConfig config,
            ITimerPtr timer,
            ui32 deviceCount,
            TVector<TFakeDevicePtr> devices)
        : Devices(std::move(devices))
        , DeviceUUIDs(deviceCount)
    {
        Devices.resize(deviceCount);

        TVector<TStorageDevice> group(deviceCount);
        for (ui32 i = 0; i < deviceCount; ++i) {
            DeviceUUIDs[i] = TStringBuilder() << "dev-" << char('a' + i);
            if (!Devices[i]) {
                Devices[i] = std::make_shared<TFakeDevice>();
            }
            group[i] = {.Node = Devices[i], .DeviceUUID = DeviceUUIDs[i]};
        }

        Group = createGroup(
            std::move(config),
            std::move(group),
            timer ? std::move(timer) : Timer);
    }

    ui32 Size() const
    {
        return Devices.size();
    }

    TFakeDevice& operator[](ui32 i)
    {
        return *Devices[i];
    }

    // Forgets what the devices have been asked so far, so a second group over
    // them starts from a clean request log.
    void ForgetRequests()
    {
        for (auto& device: Devices) {
            device->WriteCalls.clear();
            device->ReadCalls.clear();
            device->ReadJournalTailCalls.clear();
        }
    }
};

struct TNaiveFixture: TGroupFixture
{
    explicit TNaiveFixture(TStorageGroupConfig config = MakeConfig())
        : TGroupFixture(
              CreateNaiveMirroredStorageGroup,
              std::move(config),
              nullptr,
              DeviceCount,
              {})
    {}

    ~TNaiveFixture()
    {
        Group->TearDown();
    }
};

// init = false leaves Init to the test, so it can set the devices up first.
// Passing devices in reuses ones an earlier group has already written.
struct TQuorumFixture: TGroupFixture
{
    TTickTimerPtr TickTimer;

    TQuorumFixture(
            bool init = true,
            TStorageGroupConfig config = MakeConfig(),
            TTickTimerPtr tickTimer = nullptr,
            ui32 deviceCount = DeviceCount,
            TVector<TFakeDevicePtr> devices = {})
        : TGroupFixture(
              CreateQuorumMirroredStorageGroup,
              std::move(config),
              tickTimer,
              deviceCount,
              std::move(devices))
        , TickTimer(std::move(tickTimer))
    {
        if (init) {
            auto error = Group->Init().GetError();
            EXPECT_EQ(S_OK, error.GetCode())
                << ::testing::UnitTest::GetInstance()
                       ->current_test_info()
                       ->name()
                << ": " << error.GetMessage();
        }
    }

    ~TQuorumFixture()
    {
        if (TickTimer) {
            TickTimer->Stop();
        }
        Group->TearDown();
    }
};

////////////////////////////////////////////////////////////////////////////////
// Driving the group.

NProto::TError InitFails(TGroupFixture& fx)
{
    return fx.Group->Init().GetError();
}

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

// The single page the read returned as a string, or what went wrong instead,
// so that comparing it to the expected page says both.
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

ui32 TotalRecords(TGroupFixture& fx)
{
    ui32 total = 0;
    for (ui32 i = 0; i < fx.Size(); ++i) {
        total += fx[i].Lsns().size();
    }
    return total;
}

ui32 TotalParked(TGroupFixture& fx)
{
    ui32 total = 0;
    for (auto& device: fx.Devices) {
        total += device->Parked;
    }
    return total;
}

TVector<ui32> WriteCounts(TGroupFixture& fx)
{
    TVector<ui32> counts;
    for (ui32 i = 0; i < fx.Size(); ++i) {
        counts.push_back(fx[i].WriteCalls.size());
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
    for (TDuration sleep: fx.Timer->GetSleepDurations()) {
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

// Forgets the read log and reads once per device. With every replica eligible
// the rotation lands each read on a different one, so a read count of one
// everywhere is what "all of them serve" looks like.
NProto::TError ReadFromEachReplica(TGroupFixture& fx)
{
    for (auto& device: fx.Devices) {
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

#define FIBER_TEST(suite, name)                                                \
    void suite##_##name##_Body();                                              \
    TEST(suite, name)                                                          \
    {                                                                          \
        const int r = FiberScheduler::run(                                     \
            +[](int*) noexcept -> int                                          \
            {                                                                  \
                suite##_##name##_Body();                                       \
                return 0;                                                      \
            },                                                                 \
            0);                                                                \
        EXPECT_EQ(0, r);                                                       \
    }                                                                          \
    void suite##_##name##_Body()

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
        EXPECT_EQ(fx.DeviceUUIDs[i], fx[i].AcquireCalls[0].GetDeviceUUIDs(0));
    }

    auto error = WritePages(*fx.Group, Lsn, PageNo, {"page1", "page2"});
    EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    for (ui32 i = 0; i < DeviceCount; ++i) {
        EXPECT_EQ((TVector<ui64>{Lsn}), fx[i].Lsns()) << "dev " << i;
        // The naive group keeps nothing of its own on the device, so the
        // caller's page numbers are the device's page numbers.
        EXPECT_EQ("page1", fx[i].Page(PageNo)) << "dev " << i;
        EXPECT_EQ("page2", fx[i].Page(PageNo + 1)) << "dev " << i;
        EXPECT_EQ(fx.DeviceUUIDs[i], fx[i].WriteCalls[0].GetDeviceUUID());
        EXPECT_EQ(
            "test-client",
            fx[i].WriteCalls[0].GetHeaders().GetClientId());
    }

    fx.Group->TearDown();

    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(1U, fx[i].ReleaseCalls.size()) << "dev " << i;
        ASSERT_EQ(1U, fx[i].ReleaseCalls[0].DeviceUUIDsSize());
        EXPECT_EQ(fx.DeviceUUIDs[i], fx[i].ReleaseCalls[0].GetDeviceUUIDs(0));
    }
}

FIBER_TEST(NaiveGroupTest, InitReportsTheHighestAckedLsnAndChainsFromIt)
{
    TNaiveFixture fx;

    // The devices come back from a crash at different positions.
    const ui64 acked[] = {3, 7, 5};
    for (ui32 i = 0; i < DeviceCount; ++i) {
        fx[i].Watermark = acked[i];
    }

    const auto init = fx.Group->Init();

    ASSERT_EQ(S_OK, init.GetError().GetCode())

        << init.GetError().GetMessage();

    EXPECT_EQ(7U, init.GetResult());

    // The caller continues from the furthest one, and the link it builds is
    // what reaches the devices.
    auto error = Write(*fx.Group, 8);
    EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    for (ui32 i = 0; i < DeviceCount; ++i) {
        EXPECT_EQ(8U, fx[i].LastLsn()) << "dev " << i;
        EXPECT_EQ(7U, fx[i].Records.back().GetPrevLogSequenceNumber())
            << "dev " << i;
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
        fx[i].Pages[PageNo] = TStringBuilder() << "dev" << i;
    }

    for (ui32 round = 0; round < 2; ++round) {
        for (ui32 i = 0; i < DeviceCount; ++i) {
            EXPECT_EQ(
                TString(TStringBuilder() << "dev" << i),
                ReadOnePage(*fx.Group))
                << "round " << round;
        }
    }

    for (ui32 i = 0; i < DeviceCount; ++i) {
        EXPECT_EQ(2U, fx[i].ReadCalls.size()) << "dev " << i;
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

    auto error = Write(*fx.Group);
    EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    EXPECT_EQ(3U, flaky.WriteCalls.size());
    EXPECT_EQ((TVector<ui64>{Lsn}), flaky.Lsns()) << "record taken twice";
    for (ui32 i = 0; i < DeviceCount; ++i) {
        EXPECT_EQ("page1", fx[i].Page(PageNo)) << "dev " << i;
    }
    EXPECT_EQ(Backoffs(2), Sleeps(fx));

    fx[1].Pages[PageNo] = "payload";
    fx[0].ReadRespQueue.emplace_back();
    *fx[0].ReadRespQueue.back().MutableError() =
        MakeError(E_REJECTED, "busy");

    EXPECT_EQ("payload", ReadOnePage(*fx.Group));
    EXPECT_EQ(1U, fx[0].ReadCalls.size());
    EXPECT_EQ(1U, fx[1].ReadCalls.size());
    EXPECT_EQ(0U, fx[2].ReadCalls.size());

    // The read starts a backoff sequence of its own; it does not continue the
    // write's.
    auto sleeps = Backoffs(2);
    sleeps.push_back(Backoffs(1)[0]);
    EXPECT_EQ(sleeps, Sleeps(fx));
}

FIBER_TEST(NaiveGroupTest, DoesNotRetryNonRetriableErrors)
{
    TNaiveFixture fx;
    const auto init = fx.Group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode())
        << init.GetError().GetMessage();

    *fx[1].WriteResp.MutableError() = MakeError(E_ARGUMENT, "bad record");
    auto error = Write(*fx.Group);
    EXPECT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
    for (ui32 i = 0; i < DeviceCount; ++i) {
        EXPECT_EQ(1U, fx[i].WriteCalls.size()) << "dev " << i;
    }

    *fx[0].ReadResp.MutableError() = MakeError(E_ARGUMENT, "bad range");
    TVector<TPageGroup> pageGroups;
    error = Read(*fx.Group, &pageGroups);
    EXPECT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
    EXPECT_TRUE(pageGroups.empty());

    // One attempt, and no failover either: a device that rejects the request
    // outright means the request is wrong, not the device.
    EXPECT_EQ(1U, fx[0].ReadCalls.size());
    EXPECT_EQ(0U, fx[1].ReadCalls.size());
    EXPECT_EQ(0U, fx[2].ReadCalls.size());

    EXPECT_TRUE(fx.Timer->GetSleepDurations().empty());
}

FIBER_TEST(NaiveGroupTest, GivesUpWhenTheRetryDeadlineExpires)
{
    // A one second budget over the default half second increment. The test
    // timer advances by every sleep, so a dead device fails at 0s, 0.5s and
    // 1.5s: the third error is past the budget and ends the attempts.
    auto config = MakeConfig();
    config.RetryPolicy.TotalTimeout = TDuration::Seconds(1);
    constexpr ui32 attempts = 3;

    {
        TNaiveFixture fx(config);
        const auto init = fx.Group->Init();
        ASSERT_EQ(S_OK, init.GetError().GetCode())
            << init.GetError().GetMessage();
        *fx[1].WriteResp.MutableError() = MakeError(E_REJECTED, "busy");

        auto error = Write(*fx.Group);
        EXPECT_EQ(E_REJECTED, error.GetCode()) << error.GetMessage();

        EXPECT_EQ(1U, fx[0].WriteCalls.size());
        EXPECT_EQ(attempts, fx[1].WriteCalls.size());
        EXPECT_EQ(1U, fx[2].WriteCalls.size());
        EXPECT_EQ(Backoffs(attempts - 1), Sleeps(fx));
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
        EXPECT_EQ(E_UNAVAILABLE, error.GetCode()) << error.GetMessage();
        EXPECT_TRUE(pageGroups.empty());

        // The rotation keeps moving while retrying, so each device gets one.
        EXPECT_EQ(TVector<ui32>(DeviceCount, 1), ReadCounts(fx));
        EXPECT_EQ(Backoffs(attempts - 1), Sleeps(fx));
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
        EXPECT_EQ(fx.DeviceUUIDs[i], acquire.GetDeviceUUIDs(0));
        EXPECT_EQ(42U, acquire.GetGeneration());
        EXPECT_EQ("test-client", acquire.GetHeaders().GetClientId());

        // A blank device is claimed, and the claim is the first record it
        // takes, because a device refuses lsn zero.
        EXPECT_EQ((TVector<ui64>{1}), fx[i].Lsns()) << "dev " << i;
        EXPECT_FALSE(fx[i].Page(0).empty()) << "dev " << i;
    }

    fx.Group->TearDown();
    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(1U, fx[i].ReleaseCalls.size()) << "dev " << i;
        EXPECT_EQ(fx.DeviceUUIDs[i], fx[i].ReleaseCalls[0].GetDeviceUUIDs(0));
    }
}

FIBER_TEST(QuorumGroupTest, InitNeedsEveryDeviceAndStopsAtTheFirstPhaseThatFails)
{
    // Acquire is n of n, and nothing is touched before it succeeds everywhere.
    {
        TQuorumFixture fx(false /* init */);
        *fx[1].AcquireResp.MutableError() = MakeError(E_ARGUMENT, "no session");

        auto error = InitFails(fx);
        EXPECT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
        for (ui32 i = 0; i < DeviceCount; ++i) {
            EXPECT_EQ(0U, fx[i].ReadCalls.size()) << "dev " << i;
        }
        EXPECT_EQ(TVector<ui32>(fx.Size(), 0), WriteCounts(fx));
    }

    // So is the position query, and a failure there stops the replay.
    {
        TQuorumFixture fx(false /* init */);
        *fx[2].ReadJournalTailResp.MutableError() =
            MakeError(E_ARGUMENT, "no journal");

        auto error = InitFails(fx);
        EXPECT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();

        // An uninitialised group serves nobody.
        TVector<TPageGroup> pageGroups;
        error = Read(*fx.Group, &pageGroups);
        EXPECT_EQ(E_REJECTED, error.GetCode()) << error.GetMessage();
    }
}

FIBER_TEST(QuorumGroupTest, InitClaimsABlankDeviceOnceAndRecognisesItLater)
{
    GTEST_SKIP() << "waiting on device journal discovery fix";

    TQuorumFixture first;

    TVector<TString> claims;
    for (ui32 i = 0; i < DeviceCount; ++i) {
        claims.push_back(first[i].Page(0));
        EXPECT_FALSE(claims.back().empty()) << "dev " << i;
    }

    // Each device is claimed with something of its own, so a device that has
    // been swapped for another is recognisable.
    EXPECT_NE(claims[0], claims[1]);
    EXPECT_NE(claims[1], claims[2]);
    EXPECT_NE(claims[0], claims[2]);

    EXPECT_EQ(S_OK, ReadFromEachReplica(first).GetCode());

    EXPECT_EQ(TVector<ui32>(first.Size(), 1), ReadCounts(first));
    first.Group->TearDown();
    first.ForgetRequests();

    // A second group over the same devices finds them claimed, leaves them
    // alone and picks up where the first one stopped.
    TQuorumFixture again(
        false /* init */,
        MakeConfig(),
        nullptr,
        DeviceCount,
        first.Devices);

    const auto init = again.Group->Init();

    ASSERT_EQ(S_OK, init.GetError().GetCode()) << init.GetError().GetMessage();
    EXPECT_EQ(1U, init.GetResult());
    EXPECT_EQ(TVector<ui32>(again.Size(), 0), WriteCounts(again));
    for (ui32 i = 0; i < DeviceCount; ++i) {
        EXPECT_EQ(claims[i], again[i].Page(0)) << "dev " << i;
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
    EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    for (ui32 i = 0; i < 2 * DeviceCount; ++i) {
        EXPECT_EQ("fresh", ReadOnePage(*fx.Group));
    }
    EXPECT_EQ(0U, fx[2].ReadCalls.size()) << "served by the replica behind";

    fx[2].Unpause();
    ASSERT_TRUE(WaitFor([&] { return fx[2].Lsns().size() == 2; }));
}

FIBER_TEST(QuorumGroupTest, InitRejectsADeviceClaimedByAnotherGroup)
{
    // A device claimed as dev-a, offered to a group that calls it dev-b.
    {
        TQuorumFixture first;
        const TString claim = first[0].Page(0);
        first.Group->TearDown();
        first.ForgetRequests();

        TQuorumFixture other(
            false /* init */,
            MakeConfig(),
            nullptr,
            DeviceCount,
            {first.Devices[1], first.Devices[0], first.Devices[2]});

        auto error = InitFails(other);
        EXPECT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
        EXPECT_EQ(claim, other[1].Page(0)) << "the claim was overwritten";
        EXPECT_EQ(TVector<ui32>(other.Size(), 0), WriteCounts(other));
    }

    // The same devices, offered to a group that uses a different page size.
    {
        TQuorumFixture first;
        const TString claim = first[0].Page(0);
        first.Group->TearDown();
        first.ForgetRequests();

        TQuorumFixture other(
            false /* init */,
            MakeConfig(8_KB),
            nullptr,
            DeviceCount,
            first.Devices);

        auto error = InitFails(other);
        EXPECT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
        EXPECT_EQ(claim, other[0].Page(0)) << "the claim was overwritten";
        EXPECT_EQ(TVector<ui32>(other.Size(), 0), WriteCounts(other));
    }

    // A first page holding something nobody here wrote is left alone too,
    // rather than being taken for a blank device and claimed.
    {
        TQuorumFixture fx(false /* init */);
        const TString garbage(DefaultBlockSize, 'x');
        fx[0].Pages[0] = garbage;

        auto error = InitFails(fx);
        EXPECT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
        EXPECT_TRUE(Mentions(error, fx.DeviceUUIDs[0])) << error.GetMessage();
        EXPECT_EQ(garbage, fx[0].Page(0));
        EXPECT_EQ(0U, fx[0].WriteCalls.size());
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
    const TString good = first[2].Page(0);
    first.Group->TearDown();
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
        fx[2].Pages[0] = claim;

        auto error = InitFails(fx);
        EXPECT_EQ(E_INVALID_STATE, error.GetCode())
            << field << ": " << error.GetMessage();
        EXPECT_TRUE(Mentions(error, fx.DeviceUUIDs[2])) << error.GetMessage();
        EXPECT_EQ(claim, fx[2].Page(0)) << field << ": claim overwritten";
    }
}

FIBER_TEST(QuorumGroupTest, InitFailsWhenTheFirstPageCannotBeReadOrWritten)
{
    // The read itself fails.
    {
        TQuorumFixture fx(false /* init */);
        *fx[0].ReadResp.MutableError() = MakeError(E_IO, "media error");

        auto error = InitFails(fx);
        EXPECT_EQ(E_IO, error.GetCode()) << error.GetMessage();
        EXPECT_TRUE(Mentions(error, fx.DeviceUUIDs[0])) << error.GetMessage();
        EXPECT_EQ(0U, fx[0].WriteCalls.size()) << "claimed despite the error";
    }

    // The device answers with less than a page.
    {
        TQuorumFixture fx(false /* init */);
        fx[2].Pages[0] = TString(100, 'x');

        auto error = InitFails(fx);
        EXPECT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
        EXPECT_TRUE(Mentions(error, fx.DeviceUUIDs[2])) << error.GetMessage();
        EXPECT_EQ(0U, fx[2].WriteCalls.size()) << "claimed despite the error";
    }

    // Claiming a blank device is refused outright.
    {
        TQuorumFixture fx(false /* init */);
        fx[1].WriteRespQueue.emplace_back();
        *fx[1].WriteRespQueue.back().MutableError() =
            MakeError(E_ARGUMENT, "read only");

        auto error = InitFails(fx);
        EXPECT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
        EXPECT_TRUE(Mentions(error, fx.DeviceUUIDs[1])) << error.GetMessage();
        EXPECT_EQ(1U, fx[1].WriteCalls.size());
        EXPECT_TRUE(fx[1].Page(0).empty()) << "claimed anyway";
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
        EXPECT_EQ(2U, fx[1].WriteCalls.size());
        EXPECT_FALSE(fx[1].Page(0).empty());
        EXPECT_EQ(Backoffs(1), Sleeps(fx));
    }
}

FIBER_TEST(QuorumGroupTest, InitValidatesEveryDeviceEvenIfOneFails)
{
    TQuorumFixture fx(false /* init */);
    fx[0].Pages[0] = TString(DefaultBlockSize, 'x');

    auto error = InitFails(fx);
    EXPECT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
    EXPECT_TRUE(Mentions(error, fx.DeviceUUIDs[0])) << error.GetMessage();

    // The others are still looked at, and the blank ones are still claimed:
    // validation fans out and joins rather than stopping at the first answer.
    for (ui32 i = 0; i < DeviceCount; ++i) {
        EXPECT_EQ(1U, fx[i].ReadCalls.size()) << "dev " << i;
    }
    EXPECT_EQ(0U, fx[0].WriteCalls.size());
    EXPECT_FALSE(fx[1].Page(0).empty());
    EXPECT_FALSE(fx[2].Page(0).empty());
}

FIBER_TEST(QuorumGroupTest, InitCatchesUpDevicesThatLostTheirTail)
{
    // A group that wrote three records, after which two devices lost part of
    // what they had taken.
    TQuorumFixture first;
    for (ui64 lsn = 8; lsn <= 10; ++lsn) {
        auto error = WritePages(
            *first.Group,
            lsn,
            lsn,
            {TStringBuilder() << "record" << lsn});
        EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    }
    ASSERT_TRUE(
        WaitFor([&] { return TotalRecords(first) == 4 * DeviceCount; }));

    const auto whole = first[0].Lsns();
    ui64 lastPage = 0;
    for (const auto& record: first[0].Records) {
        lastPage = record.GetPageGroups(0).GetFirstPageNo();
    }
    first.Group->TearDown();

    // The group had told two of the devices that everything up to 10 is safe
    // everywhere; the third never got that push and then lost its tail.
    first[0].Watermark = 10;
    first[1].Watermark = 10;
    first[2].Watermark = 1;
    first[2].LoseTailAfter(1);
    ASSERT_TRUE(first[2].Page(lastPage).empty()) << "nothing was lost";
    first.ForgetRequests();

    TQuorumFixture fx(
        false /* init */,
        MakeConfig(),
        nullptr,
        DeviceCount,
        first.Devices);

    const auto init = fx.Group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode())
        << init.GetError().GetMessage();
    EXPECT_EQ(10U, init.GetResult());

    // The device that was behind holds what the others hold, and the data
    // that was lost is back.
    for (ui32 i = 0; i < DeviceCount; ++i) {
        EXPECT_EQ(whole, fx[i].Lsns()) << "dev " << i;
    }
    EXPECT_EQ("record10", fx[2].Page(lastPage));

    // And the pages those records carried are readable from any of them.
    for (ui32 i = 0; i < DeviceCount; ++i) {
        TVector<TPageGroup> pageGroups;
        auto error = ReadRange(*fx.Group, 10, 1, &pageGroups);
        EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
        ASSERT_EQ(1U, pageGroups.size());
        ASSERT_EQ(1U, pageGroups[0].Content.size());
        EXPECT_EQ(
            "record10",
            TString(
                pageGroups[0].Content[0].Data(),
                pageGroups[0].Content[0].Size()));
    }
    EXPECT_EQ((TVector<ui32>{0, 0, 3}), WriteCounts(fx));

    // A replayed record goes back exactly as the device that kept it holds
    // it, rather than being placed past the group's own pages a second time.
    THashMap<ui64, ui64> sourcePage;
    for (const auto& record: fx[0].Records) {
        ASSERT_EQ(1U, record.PageGroupsSize());
        sourcePage[record.GetLogSequenceNumber()] =
            record.GetPageGroups(0).GetFirstPageNo();
    }
    for (const auto& write: fx[2].WriteCalls) {
        const ui64 lsn = write.GetLogSequenceNumber();
        ASSERT_EQ(1U, write.PageGroupsSize());
        EXPECT_EQ(
            sourcePage.at(lsn),
            write.GetPageGroups(0).GetFirstPageNo())
            << "lsn " << lsn;
    }

    EXPECT_EQ(S_OK, ReadFromEachReplica(fx).GetCode());

    EXPECT_EQ(TVector<ui32>(fx.Size(), 1), ReadCounts(fx));
}

FIBER_TEST(QuorumGroupTest, InitServesAtOnceWhenEveryDeviceIsLevel)
{
    // A record taken everywhere and then pushed as safe everywhere.
    TQuorumFixture first;
    auto error = Write(*first.Group, 10);
    EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    ASSERT_TRUE(
        WaitFor([&] { return TotalRecords(first) == 2 * DeviceCount; }));
    first.Group->TearDown();
    for (auto& device: first.Devices) {
        device->Watermark = 10;
    }
    first.ForgetRequests();

    TQuorumFixture again(
        false /* init */,
        MakeConfig(),
        nullptr,
        DeviceCount,
        first.Devices);

    const auto init = again.Group->Init();

    ASSERT_EQ(S_OK, init.GetError().GetCode())

        << init.GetError().GetMessage();

    EXPECT_EQ(10U, init.GetResult());
    EXPECT_EQ(TVector<ui32>(again.Size(), 0), WriteCounts(again));
    EXPECT_EQ(S_OK, ReadFromEachReplica(again).GetCode());
    EXPECT_EQ(TVector<ui32>(again.Size(), 1), ReadCounts(again));
}

FIBER_TEST(QuorumGroupTest, InitFailsIfTheJournalCannotBridgeTheGap)
{
    // A device that was told 9 is safe everywhere, then lost the record
    // itself, while another device never heard of 9 at all: the source has
    // nothing to bring the laggard up with.
    {
        TQuorumFixture first;
        auto error = Write(*first.Group, 9);
        EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
        ASSERT_TRUE(
            WaitFor([&] { return TotalRecords(first) == 2 * DeviceCount; }));
        first.Group->TearDown();
        first[0].Watermark = 9;
        first[0].LoseTailAfter(1);
        first[1].Watermark = 9;
        first[2].Watermark = 1;
        first[2].LoseTailAfter(1);
        first.ForgetRequests();

        TQuorumFixture fx(
            false /* init */,
            MakeConfig(),
            nullptr,
            DeviceCount,
            first.Devices);

        auto error2 = InitFails(fx);
        EXPECT_EQ(E_INVALID_STATE, error2.GetCode()) << error2.GetMessage();
        EXPECT_EQ(TVector<ui32>(fx.Size(), 0), WriteCounts(fx));
    }

    // A refused replay fails Init, but every other replay still runs to
    // completion first: the fan-out is joined, not abandoned.
    {
        TQuorumFixture first;
        for (ui64 lsn = 8; lsn <= 10; ++lsn) {
            auto error = Write(*first.Group, lsn);
            EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
        }
        ASSERT_TRUE(
            WaitFor([&] { return TotalRecords(first) == 4 * DeviceCount; }));
        first.Group->TearDown();
        first[0].Watermark = 10;
        first[1].Watermark = 1;
        first[1].LoseTailAfter(1);
        first[2].Watermark = 1;
        first[2].LoseTailAfter(1);
        first.ForgetRequests();

        TQuorumFixture fx(
            false /* init */,
            MakeConfig(),
            nullptr,
            DeviceCount,
            first.Devices);
        *fx[2].WriteResp.MutableError() = MakeError(E_ARGUMENT, "read only");

        auto error = InitFails(fx);
        EXPECT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
        EXPECT_TRUE(Mentions(error, fx.DeviceUUIDs[2])) << error.GetMessage();

        EXPECT_EQ(10U, fx[1].LastLsn()) << "the healthy replay was abandoned";
        EXPECT_EQ(1U, fx[2].LastLsn());

        TVector<TPageGroup> pageGroups;
        error = Read(*fx.Group, &pageGroups);
        EXPECT_EQ(E_REJECTED, error.GetCode()) << error.GetMessage();
    }
}

FIBER_TEST(QuorumGroupTest, TearDownBeforeInitReleasesAndReturns)
{
    TQuorumFixture fx(false /* init */);
    fx.Group->TearDown();

    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(1U, fx[i].ReleaseCalls.size()) << "dev " << i;
        EXPECT_EQ(fx.DeviceUUIDs[i], fx[i].ReleaseCalls[0].GetDeviceUUIDs(0));
    }
}

FIBER_TEST(QuorumGroupTest, WriteReturnsOnMajorityAndKeepsLaggardsOutOfReads)
{
    TQuorumFixture fx;
    fx.ForgetRequests();

    fx[2].Paused = true;
    auto error = WritePages(*fx.Group, Lsn, PageNo, {"fresh"});
    EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    // Two acks are enough to return; the third record is still in flight.
    EXPECT_EQ(2U, fx[0].Lsns().size());
    EXPECT_EQ(2U, fx[1].Lsns().size());
    EXPECT_EQ(1U, fx[2].Lsns().size());

    // The record goes out with the link the caller built.
    for (ui32 i = 0; i < 2; ++i) {
        EXPECT_EQ(Lsn - 1, fx[i].WriteCalls.back().GetPrevLogSequenceNumber())
            << "dev " << i;
    }

    // The replica that has not taken the record must not answer with the page
    // as it was before. The cursor still advances over it, so the eligible two
    // do not get an even share, only a share each.
    for (ui32 i = 0; i < 2 * DeviceCount; ++i) {
        EXPECT_EQ("fresh", ReadOnePage(*fx.Group));
    }
    EXPECT_EQ(0U, fx[2].ReadCalls.size());
    EXPECT_GT(fx[0].ReadCalls.size(), 0U);
    EXPECT_GT(fx[1].ReadCalls.size(), 0U);

    // Once it catches up it is back in the rotation.
    fx[2].Unpause();
    ASSERT_TRUE(WaitFor([&] { return fx[2].Lsns().size() == 2; }));
    EXPECT_EQ(S_OK, ReadFromEachReplica(fx).GetCode());
    EXPECT_EQ(TVector<ui32>(fx.Size(), 1), ReadCounts(fx));
}

FIBER_TEST(QuorumGroupTest, MajorityIsMoreThanHalfOfTheDevices)
{
    // Two devices leave no room for a straggler: the majority is both.
    {
        TQuorumFixture fx(true /* init */, MakeConfig(), nullptr, 2);
        fx[1].Paused = true;

        silk::FiberFuture write;
        StartWrite(fx, Lsn, &write);
        EXPECT_TRUE(StillRunning(write)) << "acked without the second device";

        fx[1].Unpause();
        EXPECT_EQ(0, write.wait());
    }

    // Four tolerate one straggler.
    {
        TQuorumFixture fx(true /* init */, MakeConfig(), nullptr, 4);
        fx[3].Paused = true;

        auto error = Write(*fx.Group);
        EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();

        fx[3].Unpause();
        ASSERT_TRUE(WaitFor([&] { return fx[3].Lsns().size() == 2; }));
    }

    // But not two: half the devices is not a majority.
    {
        TQuorumFixture fx(true /* init */, MakeConfig(), nullptr, 4);
        fx[2].Paused = true;
        fx[3].Paused = true;

        silk::FiberFuture write;
        StartWrite(fx, Lsn, &write);
        EXPECT_TRUE(StillRunning(write)) << "acked on half the devices";

        fx[2].Unpause();
        EXPECT_EQ(0, write.wait());

        fx[3].Unpause();
        ASSERT_TRUE(WaitFor([&] { return fx[3].Lsns().size() == 2; }));
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
    EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    EXPECT_EQ(2U, fx[0].ReadCalls.size());
    EXPECT_EQ(0U, fx[1].ReadCalls.size());
    EXPECT_EQ(Backoffs(1), Sleeps(fx));

    for (auto& device: fx.Devices) {
        device->ReadCalls.clear();
    }

    // A replica that is behind is not a fallback: answering with the page as
    // it was before the last record is worse than failing the read.
    fx[2].Paused = true;
    error = Write(*fx.Group);
    EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    *fx[0].ReadResp.MutableError() = MakeError(E_ARGUMENT, "bad range");
    *fx[1].ReadResp.MutableError() = MakeError(E_ARGUMENT, "bad range");

    error = Read(*fx.Group, &pageGroups);
    EXPECT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
    EXPECT_EQ(0U, fx[2].ReadCalls.size());
    EXPECT_GT(fx[0].ReadCalls.size(), 0U);
    EXPECT_GT(fx[1].ReadCalls.size(), 0U);

    // A failed read is not a failed device, so the group stays usable.
    error = Write(*fx.Group, Lsn + 1);
    EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    fx[2].Unpause();
    ASSERT_TRUE(WaitFor([&] { return fx[2].Lsns().size() == 3; }));
}

FIBER_TEST(QuorumGroupTest, ConcurrentWritesAckIndependently)
{
    // Records landing in any order all get their own majority.
    {
        TQuorumFixture fx;
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
            EXPECT_EQ(0, future.wait());
        }

        ASSERT_TRUE(
            WaitFor([&] { return TotalRecords(fx) == 4 * DeviceCount; }));
    }

    // An ack for a later record is not an ack for an earlier one.
    {
        TQuorumFixture fx;
        for (ui32 i = 0; i < DeviceCount; ++i) {
            fx[i].HoldLsn = 100;
        }

        silk::FiberFuture held;
        silk::FiberFuture free;
        StartWrite(fx, 100, &held);
        StartWrite(fx, 200, &free);

        EXPECT_EQ(0, free.wait());
        EXPECT_TRUE(StillRunning(held)) << "held write acked on foreign acks";

        for (ui32 i = 0; i < DeviceCount; ++i) {
            fx[i].Unpause();
        }
        EXPECT_EQ(0, held.wait());
        ASSERT_TRUE(
            WaitFor([&] { return TotalRecords(fx) == 3 * DeviceCount; }));
    }
}

FIBER_TEST(QuorumGroupTest, AnyDeviceWriteFailureBreaksTheGroup)
{
    // A failure that arrives before the majority fails the write itself.
    {
        TQuorumFixture fx;
        fx[1].Paused = true;
        *fx[2].WriteResp.MutableError() = MakeError(E_ARGUMENT, "read only");

        auto error = Write(*fx.Group);
        EXPECT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
        EXPECT_TRUE(Mentions(error, fx.DeviceUUIDs[2])) << error.GetMessage();

        // And the group stays broken for everything that follows.
        error = Write(*fx.Group, Lsn + 1);
        EXPECT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();

        TVector<TPageGroup> pageGroups;
        error = Read(*fx.Group, &pageGroups);
        EXPECT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();

        fx[1].Unpause();
        ASSERT_TRUE(WaitFor([&] { return fx[1].Lsns().size() == 2; }));
    }

    // A failure that arrives after it does not un-ack the caller, but still
    // breaks the group: the devices have diverged.
    {
        TQuorumFixture fx;
        fx[2].Paused = true;
        *fx[2].WriteResp.MutableError() = MakeError(E_ARGUMENT, "read only");

        auto error = Write(*fx.Group);
        EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();

        fx[2].Unpause();
        ASSERT_TRUE(WaitFor(
            [&]
            {
                TVector<TPageGroup> pageGroups;
                return HasError(Read(*fx.Group, &pageGroups));
            }));

        error = Write(*fx.Group, Lsn + 1);
        EXPECT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
        EXPECT_TRUE(Mentions(error, fx.DeviceUUIDs[2])) << error.GetMessage();
    }
}

FIBER_TEST(QuorumGroupTest, RejectsRequestsItCannotAddress)
{
    TQuorumFixture fx;
    fx.ForgetRequests();

    // Lsn zero is what an unwritten record looks like, so it is refused.
    auto error = WritePages(*fx.Group, 0, PageNo, {"page1"});
    EXPECT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();

    // So is a page range the group cannot place on a device without wrapping
    // around.
    error = WritePages(*fx.Group, Lsn, Max<ui64>(), {"page1"});
    EXPECT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();

    TVector<TPageGroup> pageGroups;
    error = ReadRange(*fx.Group, Max<ui64>() - 1, 2, &pageGroups);
    EXPECT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();

    for (ui32 i = 0; i < DeviceCount; ++i) {
        EXPECT_EQ(0U, fx[i].WriteCalls.size()) << "dev " << i;
        EXPECT_EQ(0U, fx[i].ReadCalls.size()) << "dev " << i;
    }
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
            {.Lsn = Lsn + 2, .PrevLsn = Lsn});
        EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    }
    ASSERT_TRUE(WaitFor([&] { return TotalRecords(fx) == 2 * DeviceCount; }));
    for (ui32 i = 0; i < DeviceCount; ++i) {
        EXPECT_EQ(Lsn + 2, fx[i].WriteCalls.back().GetLogSequenceNumber());
        EXPECT_EQ(Lsn, fx[i].WriteCalls.back().GetPrevLogSequenceNumber());
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
        EXPECT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
    }
    EXPECT_EQ(TVector<ui32>(DeviceCount, 1), WriteCounts(fx));
}

FIBER_TEST(QuorumGroupTest, KeepsTheCallersPagesClearOfItsOwn)
{
    TQuorumFixture fx;

    TVector<TString> claims;
    for (ui32 i = 0; i < DeviceCount; ++i) {
        claims.push_back(fx[i].Page(0));
        ASSERT_FALSE(claims.back().empty()) << "dev " << i;
    }

    // Page zero belongs to the caller like any other page, and writing it must
    // not disturb what the group keeps at the front of the device.
    auto error = WritePages(*fx.Group, Lsn, 0, {"page0", "page1"});
    EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    ASSERT_TRUE(WaitFor([&] { return TotalRecords(fx) == 2 * DeviceCount; }));

    for (ui32 i = 0; i < DeviceCount; ++i) {
        EXPECT_EQ(claims[i], fx[i].Page(0)) << "dev " << i;
    }

    // And it comes back as page zero.
    TVector<TPageGroup> pageGroups;
    error = ReadRange(*fx.Group, 0, 2, &pageGroups);
    EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    ASSERT_EQ(1U, pageGroups.size());
    EXPECT_EQ(0U, pageGroups[0].FirstPageNo);
    ASSERT_EQ(2U, pageGroups[0].Content.size());
    EXPECT_EQ(
        "page0",
        TString(
            pageGroups[0].Content[0].Data(),
            pageGroups[0].Content[0].Size()));

    // A device answering with a page group the caller never asked for would
    // come back as a page number that does not exist, so it is dropped.
    for (auto& device: fx.Devices) {
        device->ReadResp = {};
        auto* pg = device->ReadResp.AddPageGroups();
        pg->SetFirstPageNo(0);
        pg->AddContent("the claim");
    }

    pageGroups.clear();
    error = ReadRange(*fx.Group, 0, 1, &pageGroups);
    EXPECT_EQ(E_FAIL, error.GetCode()) << error.GetMessage();
    EXPECT_TRUE(pageGroups.empty());
}

FIBER_TEST(QuorumGroupTest, LowWatermarkFollowsTheSlowestDevice)
{
    auto timer = std::make_shared<TTickTimer>();
    TQuorumFixture fx(true /* init */, MakeConfigWithWaterMarksLoop(), timer);

    // Everything every device already holds may be trimmed from the start.
    ASSERT_TRUE(timer->TickUntil(
        [&] { return !fx[0].AdvanceLsnLowWatermarkCalls.empty(); }));
    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(1U, fx[i].AdvanceLsnLowWatermarkCalls.size()) << "dev " << i;
        EXPECT_EQ(
            1U,
            fx[i].AdvanceLsnLowWatermarkCalls[0].GetLsnLowWatermark())
            << "dev " << i;
    }

    // A record one device is still missing may not be trimmed anywhere: a
    // trimmed peer could never replay it to that device.
    fx[2].Paused = true;
    auto error = Write(*fx.Group, 20);
    EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    timer->TickOnce();
    for (ui32 i = 0; i < DeviceCount; ++i) {
        EXPECT_EQ(1U, fx[i].AdvanceLsnLowWatermarkCalls.size()) << "dev " << i;
    }

    fx[2].Unpause();
    ASSERT_TRUE(timer->TickUntil(
        [&] { return fx[0].AdvanceLsnLowWatermarkCalls.size() == 2; }));
    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(2U, fx[i].AdvanceLsnLowWatermarkCalls.size()) << "dev " << i;
        EXPECT_EQ(
            20U,
            fx[i].AdvanceLsnLowWatermarkCalls[1].GetLsnLowWatermark())
            << "dev " << i;
    }

    // Nothing moved, so nothing is pushed again.
    timer->TickOnce();
    for (ui32 i = 0; i < DeviceCount; ++i) {
        EXPECT_EQ(2U, fx[i].AdvanceLsnLowWatermarkCalls.size()) << "dev " << i;
    }
}

FIBER_TEST(QuorumGroupTest, LowWatermarkMissedByADeviceIsSupersededNotRetried)
{
    auto timer = std::make_shared<TTickTimer>();
    TQuorumFixture fx(true /* init */, MakeConfigWithWaterMarksLoop(), timer);
    *fx[2].AdvanceLsnLowWatermarkResp.MutableError() =
        MakeError(E_REJECTED, "busy");

    ASSERT_TRUE(timer->TickUntil(
        [&] { return !fx[2].AdvanceLsnLowWatermarkCalls.empty(); }));
    EXPECT_EQ(1U, fx[2].AdvanceLsnLowWatermarkCalls.size());

    // The refusal was retriable, so the group does not break, and the missed
    // watermark is not resent on its own.
    fx[2].AdvanceLsnLowWatermarkResp = {};
    timer->TickOnce();
    EXPECT_EQ(1U, fx[2].AdvanceLsnLowWatermarkCalls.size());

    // The next one carries the device forward anyway.
    auto error = Write(*fx.Group, 20);
    EXPECT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    ASSERT_TRUE(timer->TickUntil(
        [&] { return fx[2].AdvanceLsnLowWatermarkCalls.size() == 2; }));
    EXPECT_EQ(
        20U,
        fx[2].AdvanceLsnLowWatermarkCalls[1].GetLsnLowWatermark());
}

FIBER_TEST(QuorumGroupTest, LowWatermarkRefusedOutrightBreaksTheGroup)
{
    auto timer = std::make_shared<TTickTimer>();
    TQuorumFixture fx(true /* init */, MakeConfigWithWaterMarksLoop(), timer);
    *fx[2].AdvanceLsnLowWatermarkResp.MutableError() =
        MakeError(E_ARGUMENT, "unknown device");

    ASSERT_TRUE(timer->TickUntil(
        [&] { return !fx[2].AdvanceLsnLowWatermarkCalls.empty(); }));

    // A device that refuses to trim is a device we can no longer reason about.
    const ui32 records = TotalRecords(fx);
    auto error = Write(*fx.Group, 20);
    EXPECT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
    EXPECT_TRUE(Mentions(error, fx.DeviceUUIDs[2])) << error.GetMessage();
    EXPECT_EQ(records, TotalRecords(fx));
}
