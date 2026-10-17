#include "storage_group_quorum.h"

#include "context.h"
#include "storage_group_helpers.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/fibers/mutex.h>
#include <silk/fibers/sequencer.h>
#include <silk/util/logger.h>

#include <util/digest/city.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <algorithm>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>

namespace NCloud::NFileStore::NStorage::NFastShard {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 ReservedPages = TStorageGroupHeader::StorageGroupReservedPages;
constexpr ui32 QuorumMirrorGroupType = NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR;

// TODO(#5895): unify with blockstore
bool IsAllZeroes(const char* src, size_t size)
{
    if (size < sizeof(ui64)) {
        return std::all_of(src, src + size, [](char c) { return c == 0; });
    }

    const bool aligned = reinterpret_cast<uintptr_t>(src) % sizeof(ui64) == 0;
    const size_t step = aligned ? sizeof(ui64) : 1;
    return std::all_of(src, src + step, [](char c) { return c == 0; })
        && memcmp(src, src + step, size - step) == 0;
}

ui64 HashDeviceUUID(TStringBuf deviceUUID)
{
    return CityHash64(deviceUUID.data(), deviceUUID.size());
}

TString MakeHeaderPage(const TStorageGroupHeader& header, ui32 pageSize)
{
    TString page(pageSize, '\0');
    std::memcpy(page.begin(), &header, sizeof(header));
    return page;
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError CheckPageRange(ui64 firstPageNo, ui64 pageCount)
{
    // firstPageNo + ReservedPages + pageCount must not wrap around.
    if (firstPageNo > Max<ui64>() - ReservedPages ||
        pageCount > Max<ui64>() - ReservedPages - firstPageNo)
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "page range [" << firstPageNo << ", +" << pageCount
                << ") overflows internal page reserve");
    }

    return {};
}

NProto::TError ShiftToDevice(NProto::TWriteLogRecordRequest& request)
{
    for (auto& pg: *request.MutablePageGroups()) {
        auto error = CheckPageRange(pg.GetFirstPageNo(), pg.ContentSize());
        if (HasError(error)) {
            return error;
        }

        pg.SetFirstPageNo(pg.GetFirstPageNo() + ReservedPages);
    }

    return {};
}

NProto::TError ShiftToDevice(NProto::TReadPagesRequest& request)
{
    for (auto& ref: *request.MutablePageGroupRefs()) {
        auto error = CheckPageRange(ref.GetFirstPageNo(), ref.GetPageCount());
        if (HasError(error)) {
            return error;
        }

        ref.SetFirstPageNo(ref.GetFirstPageNo() + ReservedPages);
    }

    return {};
}

NProto::TError ShiftToClient(TVector<TPageGroup>* pageGroups)
{
    for (auto& pg: *pageGroups) {
        if (pg.FirstPageNo < ReservedPages) {
            const ui64 pageNo = pg.FirstPageNo;
            pageGroups->clear();
            return MakeError(
                E_FAIL,
                TStringBuilder()
                    << "device answered with reserved page " << pageNo);
        }

        pg.FirstPageNo -= ReservedPages;
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TGroupHealth
{
public:
    void Fail(const NProto::TError& error, const TString& deviceUUID)
    {
        {
            std::lock_guard g(Mutex);
            Error = MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "storage group broken: device " << deviceUUID
                    << " failed: " << FormatError(error));

            SILK_ERROR("%s", FormatError(Error).c_str());
        }

        BrokenFlag.store(true, std::memory_order_release);
    }

    bool IsBroken() const
    {
        return BrokenFlag.load(std::memory_order_acquire);
    }

    NProto::TError GetError() const
    {
        std::lock_guard g(Mutex);
        return Error;
    }

private:
    mutable silk::FiberMutex Mutex;
    NProto::TError Error;
    std::atomic<bool> BrokenFlag{false};
};

////////////////////////////////////////////////////////////////////////////////

class TInflight
{
public:
    void Increment()
    {
        Started.increment();
    }

    void Decrement()
    {
        Finished.increment();
    }

    void Wait()
    {
        Y_UNUSED(Finished.wait(Started.get()));
    }

private:
    silk::FiberSequencer Started;
    silk::FiberSequencer Finished;
};

////////////////////////////////////////////////////////////////////////////////

/**
 * Keeping track of largest write lsn acked so far.
 */
class TDeviceProxy
{
public:
    TDeviceProxy(TStorageDevice device, TStorageGroupConfig config)
        : DeviceUUID(std::move(device.DeviceUUID))
        , StorageNode(std::move(device.Node))
        , Config(std::move(config))
    {}

    bool CanServe(ui64 lsn) const
    {
        return Acked.get() >= lsn;
    }

    NProto::TError Write(
        TFastShardContext& ctx,
        const NProto::TWriteLogRecordRequest& request)
    {
        auto response = CallWithRetries(
            ctx,
            Config.RetryPolicy,
            [&]
            {
                NProto::TWriteLogRecordRequest deviceRequest = request;
                deviceRequest.SetDeviceUUID(DeviceUUID);
                return StorageNode->WriteLogRecord(std::move(deviceRequest));
            });

        if (!HasError(response.GetError())) {
            // Acked only moves up, so we do not care about ordering here
            Acked.advance(request.GetLogSequenceNumber());
        }

        return response.GetError();
    }

    NProto::TError Read(
        TFastShardContext& ctx,
        const NProto::TReadPagesRequest& request,
        NProto::TReadPagesResponse* response)
    {
        *response = CallWithRetries(
            ctx,
            Config.RetryPolicy,
            [&]
            {
                NProto::TReadPagesRequest deviceRequest = request;
                deviceRequest.SetDeviceUUID(DeviceUUID);
                return StorageNode->ReadPages(std::move(deviceRequest));
            });

        return response->GetError();
    }

    NProto::TError ReadJournalTail(
        TFastShardContext& ctx,
        ui64 afterLsn,
        ui32 maxRecords,
        NProto::TReadJournalTailResponse* response)
    {
        NProto::TReadJournalTailRequest request;
        FillHeaders(Config, request.MutableHeaders());
        request.SetAfterLogSequenceNumber(afterLsn);
        request.SetMaxRecordCount(maxRecords);

        *response = CallWithRetries(
            ctx,
            Config.RetryPolicy,
            [&]
            {
                NProto::TReadJournalTailRequest deviceRequest = request;
                deviceRequest.SetDeviceUUID(DeviceUUID);
                return StorageNode->ReadJournalTail(std::move(deviceRequest));
            });

        return response->GetError();
    }

    ui64 GetLastAckedLsn() const
    {
        return Acked.get();
    }

    void SeedLsn(ui64 lsn)
    {
        Acked.advance(lsn);
    }

    NProto::TError AdvanceLsnLowWatermark(TFastShardContext& ctx, ui64 lsn)
    {
        NProto::TAdvanceLsnLowWatermarkRequest request;
        FillHeaders(Config, request.MutableHeaders());
        request.SetDeviceUUID(DeviceUUID);
        request.SetLsnLowWatermark(lsn);

        auto response = CallWithRetries(
            ctx,
            Config.RetryPolicy,
            [&]
            {
                return StorageNode->AdvanceLsnLowWatermark(request);
            });

        return response.GetError();
    }

    NProto::TError Acquire(TFastShardContext& ctx)
    {
        NProto::TAcquireDevicesRequest request;
        FillHeaders(Config, request.MutableHeaders());
        request.SetGeneration(Config.AcquireGeneration);
        request.AddDeviceUUIDs(DeviceUUID);

        auto response = CallWithRetries(
            ctx,
            Config.RetryPolicy,
            [&]
            {
                return StorageNode->AcquireDevices(request);
            });

        return response.GetError();
    }

    NProto::TError Release(TFastShardContext& ctx)
    {
        NProto::TReleaseDevicesRequest request;
        FillHeaders(Config, request.MutableHeaders());
        request.AddDeviceUUIDs(DeviceUUID);

        auto response = CallWithRetries(
            ctx,
            Config.RetryPolicy,
            [&]
            {
                return StorageNode->ReleaseDevices(request);
            });

        return response.GetError();
    }

public:
    const TString DeviceUUID;
    const IStorageNodePtr StorageNode;

private:
    const TStorageGroupConfig Config;
    silk::FiberSequencer Acked;
};

using TDeviceProxyPtr = std::shared_ptr<TDeviceProxy>;

////////////////////////////////////////////////////////////////////////////////

struct TGroupState
{
    TStorageGroupConfig Config;
    ITimerPtr Timer;
    TGroupHealth Health;
    TVector<TDeviceProxyPtr> Proxies;
    ui32 WriteQuorum = 0;

    std::atomic<ui32> Selector = 0;

    // Highest lsn the group has acked so far. Readers use it as a basic filter
    // for the devices.
    silk::FiberSequencer QuorumLsn;

    // Highest lsn acked by every device.
    silk::FiberSequencer LowWatermarkLsn;

    TInflight ReadsInflight;
    TInflight WritesInflight;
    TInflight Loops;

    std::atomic<bool> Initialized = false;

    std::atomic<bool> Stopped = false;

    // On a line of its own: every request bumps it, and the flags above are
    // read by every request too.
    alignas(64) std::atomic<ui64> RequestId = 1;

    TFastShardContext MakeContext()
    {
        return {
            RequestId++,
            *Timer,
            Stopped,
            Config.RetryPolicy.TotalTimeout};
    }
};

using TGroupStatePtr = std::shared_ptr<TGroupState>;

////////////////////////////////////////////////////////////////////////////////

using TProxyCall = std::function<NProto::TError(TDeviceProxy&)>;

struct TProxyCallParams
{
    TDeviceProxy* Proxy;
    const TProxyCall* Call;
    NProto::TError* Error;
};

// Runs @p call on every proxy at once and returns the first error. The error
// slots live on this frame: every fiber is joined before the return.
NProto::TError ForEachProxy(const TGroupState& state, const TProxyCall& call)
{
    const ui32 count = state.Proxies.size();
    TVector<silk::FiberFuture> futures(count);
    TVector<NProto::TError> errors(count);
    for (ui32 i = 0; i < count; ++i) {
        const int r = silk::FiberScheduler::run<TProxyCallParams>(
            [](TProxyCallParams* params) noexcept
            {
                *params->Error = (*params->Call)(*params->Proxy);
                return 0;
            },
            TProxyCallParams{
                .Proxy = state.Proxies[i].get(),
                .Call = &call,
                .Error = &errors[i]},
            &futures[i]);
        Y_ABORT_UNLESS(r == 0, "failed to spawn fiber: %s", ::strerror(r));
    }

    for (auto& future: futures) {
        future.wait();
    }

    for (const auto& error: errors) {
        if (HasError(error)) {
            return error;
        }
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

struct TLoop
{
    TGroupStatePtr State;
    TDuration Period;
    std::function<void(TGroupState&)> Body;
};

using TLoopPtr = std::shared_ptr<TLoop>;

int LoopFiberMain(TLoopPtr* params) noexcept
{
    auto& loop = **params;
    auto& state = *loop.State;
    for (;;) {
        state.Timer->Sleep(loop.Period, state.Stopped);
        if (state.Stopped.load(std::memory_order_acquire) ||
            state.Health.IsBroken())
        {
            break;
        }

        loop.Body(state);
    }

    state.Loops.Decrement();
    return 0;
}

void RunLoop(
    const TGroupStatePtr& state,
    TDuration period,
    std::function<void(TGroupState&)> body)
{
    state->Loops.Increment();
    const int r = silk::FiberScheduler::run(
        LoopFiberMain,
        std::make_shared<TLoop>(state, period, std::move(body)),
        nullptr);
    Y_ABORT_UNLESS(r == 0, "failed to spawn fiber: %s", ::strerror(r));
}

////////////////////////////////////////////////////////////////////////////////

struct TWriteState
{
    TWriteState(TGroupState& state)
        : Context(state.MakeContext())
    {}

    TFastShardContext Context;
    NProto::TWriteLogRecordRequest Request;
    ui64 Lsn = 0;
    silk::FiberSequencer Acks;
};

using TWriteStatePtr = std::shared_ptr<TWriteState>;

struct TWriteDispatchParams
{
    TGroupStatePtr State;
    TWriteStatePtr Op;
    TDeviceProxyPtr Proxy;
};

int WriteDispatchFiberMain(TWriteDispatchParams* params) noexcept
{
    auto& state = *params->State;
    auto& proxy = *params->Proxy;

    auto error = proxy.Write(params->Op->Context, params->Op->Request);
    // The proxy has already retried per the policy: whatever error is left
    // is final for this device.
    if (HasError(error)) {
        // Break the group first, so the writer this wakes finds it broken.
        if (!params->Op->Context.IsStopped()) {
            state.Health.Fail(error, proxy.DeviceUUID);
        }
        params->Op->Acks.stop();
    } else if (params->Op->Acks.increment() == state.Proxies.size()) {
        state.LowWatermarkLsn.advance(params->Op->Lsn);
    }

    state.WritesInflight.Decrement();
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError InitializeDevice(
    TFastShardContext& ctx,
    const TGroupState& state,
    TDeviceProxy& proxy)
{
    TStorageGroupHeader header;
    header.GroupType = QuorumMirrorGroupType;
    header.PageSize = state.Config.PageSize;
    header.DeviceUUIDHash = HashDeviceUUID(proxy.DeviceUUID);

    NProto::TWriteLogRecordRequest request;
    FillHeaders(state.Config, request.MutableHeaders());
    request.SetLogSequenceNumber(1);

    auto* pg = request.AddPageGroups();
    pg->SetFirstPageNo(0);
    *pg->AddContent() = MakeHeaderPage(header, state.Config.PageSize);

    SILK_INFO("sg init: writing header to %s", proxy.DeviceUUID.c_str());

    auto error = proxy.Write(ctx, request);
    if (HasError(error)) {
        return MakeError(
            error.GetCode(),
            TStringBuilder()
                << proxy.DeviceUUID << " header write failed: "
                << FormatError(error));
    }

    return {};
}

NProto::TError ValidateDeviceConfigAndInitIfNeeded(
    TFastShardContext& ctx,
    const TGroupState& state,
    TDeviceProxy& proxy)
{
    NProto::TReadPagesRequest request;
    FillHeaders(state.Config, request.MutableHeaders());

    auto* ref = request.AddPageGroupRefs();
    ref->SetPageSize(state.Config.PageSize);
    ref->SetFirstPageNo(0);
    ref->SetPageCount(1);

    NProto::TReadPagesResponse response;
    auto error = proxy.Read(ctx, request, &response);
    if (HasError(error)) {
        return MakeError(
            error.GetCode(),
            TStringBuilder()
                << proxy.DeviceUUID << " header read failed: "
                << FormatError(error));
    }

    const auto& groups = response.GetPageGroups();
    if (groups.size() != 1 || groups.Get(0).ContentSize() != 1) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << proxy.DeviceUUID << " header read returned "
                << groups.size() << " page groups instead of one page");
    }

    const auto& content = groups.Get(0).GetContent(0);
    if (content.size() != state.Config.PageSize) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << proxy.DeviceUUID << " header page is " << content.size()
                << " bytes instead of " << state.Config.PageSize);
    }

    if (IsAllZeroes(content.data(), content.size())) {
        return InitializeDevice(ctx, state, proxy);
    }

    TStorageGroupHeader header;
    std::memcpy(&header, content.data(), sizeof(header));

    if (header.MagicNumber != TStorageGroupHeader::Magic ||
        header.Version != TStorageGroupHeader::CurrentVersion ||
        header.GroupType != QuorumMirrorGroupType ||
        header.PageSize != state.Config.PageSize ||
        header.DeviceUUIDHash != HashDeviceUUID(proxy.DeviceUUID))
    {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << proxy.DeviceUUID << " header mismatch: " << header
                << ", expected page size " << state.Config.PageSize);
    }

    proxy.SeedLsn(response.GetLastAckedLogSequenceNumber());
    return {};
}

NProto::TError ValidateGroupConfigAndInitIfNeeded(
    TFastShardContext& ctx,
    const TGroupState& state)
{
    return ForEachProxy(
        state,
        [&](TDeviceProxy& proxy) -> NProto::TError
        {
            auto error = ValidateDeviceConfigAndInitIfNeeded(ctx, state, proxy);
            if (!HasError(error)) {
                return {};
            }

            SILK_ERROR(
                "sg device validation failed: %s",
                FormatError(error).c_str());

            return MakeError(
                error.GetCode(),
                TStringBuilder()
                    << "group setup validation failed: "
                    << FormatError(error));
        });
}

////////////////////////////////////////////////////////////////////////////////

using TJournalRecords =
    google::protobuf::RepeatedPtrField<NProto::TJournalRecord>;

ui64 GetLastLsn(const NProto::TReadJournalTailResponse& journal)
{
    if (!journal.GetRecords().empty()) {
        return journal.GetRecords().rbegin()->GetLogSequenceNumber();
    }

    return 0;
}

NProto::TError ReplayOnDevicesBehind(
    TFastShardContext& ctx,
    const TGroupState& state,
    const TVector<ui64>& maxLsnPerDevice,
    ui64 maxKnownLsn,
    const TJournalRecords& records)
{
    THashMap<TString, int> startPositions;
    for (ui32 i = 0; i < maxLsnPerDevice.size(); ++i) {
        if (maxLsnPerDevice[i] == maxKnownLsn) {
            continue;
        }

        const auto it = std::upper_bound(
            records.begin(),
            records.end(),
            maxLsnPerDevice[i],
            [](ui64 lsn, const NProto::TJournalRecord& record)
            {
                return lsn < record.GetLogSequenceNumber();
            });

        if (it == records.end()) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "journal tail does not continue "
                    << state.Proxies[i]->DeviceUUID << " from lsn "
                    << maxLsnPerDevice[i]);
        }

        startPositions[state.Proxies[i]->DeviceUUID] =
            std::distance(records.begin(), it);
    }

    return ForEachProxy(
        state,
        [&](TDeviceProxy& proxy) -> NProto::TError
        {
            const int* start = startPositions.FindPtr(proxy.DeviceUUID);
            if (!start) {
                return {};
            }

            for (int i = *start; i < records.size(); ++i) {
                NProto::TDeviceRequestHeaders headers;
                FillHeaders(state.Config, &headers);
                auto error = proxy.Write(
                    ctx,
                    MakeReplayRequest(std::move(headers), records[i]));
                if (HasError(error)) {
                    return MakeError(
                        error.GetCode(),
                        TStringBuilder()
                            << "replay onto " << proxy.DeviceUUID
                            << " failed: " << FormatError(error));
                }
            }

            return {};
        });
}

// Read records above @p afterLsn, from a device at @p maxKnownLsn.
NProto::TError ReadRecordsAbove(
    TFastShardContext& ctx,
    TDeviceProxy& source,
    ui64 afterLsn,
    ui64 maxKnownLsn,
    NProto::TReadJournalTailResponse* tail)
{
    auto error = source.ReadJournalTail(ctx, afterLsn, 0 /*no limit*/, tail);
    if (HasError(error)) {
        return MakeError(
            error.GetCode(),
            TStringBuilder()
                << "journal of " << source.DeviceUUID << " after lsn "
                << afterLsn << ": " << FormatError(error));
    }

    if (GetLastLsn(*tail) != maxKnownLsn) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "journal of " << source.DeviceUUID
                << " does not reach from lsn " << afterLsn << " to "
                << maxKnownLsn);
    }

    return {};
}

// Reads what the slowest device lacks from the most advanced one and replays
// it onto everyone behind. Where each device is was learnt at validation:
// from its header read, or from the claim written to it.
NProto::TError RebuildJournal(
    TFastShardContext& ctx,
    TGroupState& state)
{
    TVector<ui64> maxLsnPerDevice(state.Proxies.size());
    for (ui32 i = 0; i < state.Proxies.size(); ++i) {
        maxLsnPerDevice[i] = state.Proxies[i]->GetLastAckedLsn();
    }

    auto low =
        std::min_element(maxLsnPerDevice.begin(), maxLsnPerDevice.end());
    auto high =
        std::max_element(maxLsnPerDevice.begin(), maxLsnPerDevice.end());

    if (*low < *high) {
        const ui32 source = std::distance(maxLsnPerDevice.begin(), high);

        NProto::TReadJournalTailResponse tail;
        auto error = ReadRecordsAbove(
            ctx,
            *state.Proxies[source],
            *low,
            *high,
            &tail);

        if (HasError(error)) {
            return error;
        }

        error = ReplayOnDevicesBehind(
            ctx,
            state,
            maxLsnPerDevice,
            *high,
            tail.GetRecords());

        if (HasError(error)) {
            return error;
        }
    }

    state.QuorumLsn.advance(*high);
    state.LowWatermarkLsn.advance(*high);

    SILK_INFO("sg rebuild: every device at lsn %lu", *high);
    return {};
}

void PushLowWatermarkEverywhere(TGroupState& state, ui64 watermark)
{
    auto ctx = state.MakeContext();
    auto error = ForEachProxy(
        state,
        [&](TDeviceProxy& proxy) -> NProto::TError
        {
            auto error = proxy.AdvanceLsnLowWatermark(ctx, watermark);
            if (HasError(error) && GetErrorKind(error) != EErrorKind::ErrorRetriable) {
                state.Health.Fail(error, proxy.DeviceUUID);
            }

            return error;
        });

    if (HasError(error)) {
        SILK_ERROR(
            "sg low watermark %lu not delivered: %s",
            watermark,
            FormatError(error).c_str());
    }
}

void RenewSession(TGroupState& state, TDeviceProxy& proxy)
{
    auto ctx = state.MakeContext();
    auto error = proxy.Acquire(ctx);
    if (!HasError(error)) {
        return;
    }

    const auto kind = GetErrorKind(error);
    if (kind == EErrorKind::ErrorSession || kind == EErrorKind::ErrorFatal) {
        state.Health.Fail(error, proxy.DeviceUUID);
        return;
    }

    SILK_WARN(
        "sg claim on %s not renewed: %s",
        proxy.DeviceUUID.c_str(),
        FormatError(error).c_str());
}

////////////////////////////////////////////////////////////////////////////////

class TQuorumMirroredStorageGroup final: public IStorageGroup
{
public:
    TQuorumMirroredStorageGroup(
            TStorageGroupConfig config,
            TVector<TStorageDevice> devices,
            ITimerPtr timer)
        : State(std::make_shared<TGroupState>())
    {
        State->Config = std::move(config);
        State->Timer = std::move(timer);
        State->WriteQuorum = devices.size() / 2 + 1;
        State->Proxies.reserve(devices.size());
        for (auto& device: devices) {
            State->Proxies.push_back(
                std::make_shared<TDeviceProxy>(
                    std::move(device),
                    State->Config));
        }
    }

    TResultOrError<ui64> Init() override
    {
        if (State->Proxies.empty()) {
            return MakeError(
                E_INVALID_STATE,
                "empty storage configuration");
        }

        auto ctx = State->MakeContext();
        auto error = ForEachProxy(
            *State,
            [&](TDeviceProxy& proxy)
            {
                return proxy.Acquire(ctx);
            });

        if (HasError(error)) {
            return error;
        }

        error = ValidateGroupConfigAndInitIfNeeded(ctx, *State);
        if (HasError(error)) {
            return error;
        }

        error = RebuildJournal(ctx, *State);
        if (HasError(error)) {
            return error;
        }

        if (State->Config.ReacquirePeriod != TDuration::Zero()) {
            for (const auto& proxy: State->Proxies) {
                RunLoop(
                    State,
                    State->Config.ReacquirePeriod,
                    [proxy](TGroupState& state)
                    { RenewSession(state, *proxy); });
            }
        }

        if (State->Config.LowWatermarkPeriod != TDuration::Zero()) {
            RunLoop(
                State,
                State->Config.LowWatermarkPeriod,
                [pushed = ui64(0)](TGroupState& state) mutable
                {
                    const ui64 watermark = state.LowWatermarkLsn.get();
                    if (watermark > pushed) {
                        PushLowWatermarkEverywhere(state, watermark);
                        pushed = watermark;
                    }
                });
        }

        State->Initialized = true;
        return State->QuorumLsn.get();
    }

    void TearDown() override
    {
        State->Stopped.store(true, std::memory_order_release);
        State->Loops.Wait();
        State->ReadsInflight.Wait();
        State->WritesInflight.Wait();

        // The release is the teardown's own work: the stop flag does not
        // apply to it, the policy's deadline does.
        const std::atomic<bool> noStop = false;
        TFastShardContext ctx(
            State->RequestId++,
            *State->Timer,
            noStop,
            State->Config.RetryPolicy.TotalTimeout);

        auto error = ForEachProxy(
            *State,
            [&](TDeviceProxy& proxy) -> NProto::TError
            {
                return proxy.Release(ctx);
            });

        if (HasError(error)) {
            SILK_WARN(
                "sg tear down failed: %s",
                FormatError(error).c_str());
        }
    }

    NProto::TError WriteLogRecord(
        NProto::TDeviceRequestHeaders headers,
        TVector<TPageGroup> pageGroups,
        TLsnLink link) override
    {
        if (auto error = CheckReady(); HasError(error)) {
            return error;
        }

        if (link.Lsn <= link.PrevLsn) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "lsn " << link.Lsn
                    << " must be above the previous one "
                    << link.PrevLsn);
        }

        FillHeaders(State->Config, &headers);
        auto op = std::make_shared<TWriteState>(*State);
        op->Lsn = link.Lsn;
        op->Request = MakeWriteLogRecordRequest(
            std::move(headers),
            pageGroups,
            link);

        if (auto error = ShiftToDevice(op->Request); HasError(error)) {
            return error;
        }

        SILK_DEBUG("sg write: %s", DebugMessage(op->Request).c_str());
        for (const auto& proxy: State->Proxies) {
            State->WritesInflight.Increment();
            const int r = silk::FiberScheduler::run(
                WriteDispatchFiberMain,
                TWriteDispatchParams{
                    .State = State,
                    .Op = op,
                    .Proxy = proxy},
                nullptr);
            Y_ABORT_UNLESS(r == 0, "failed to spawn fiber: %s", ::strerror(r));
        }

        const int cancelled = op->Acks.wait(State->WriteQuorum);

        // Check overall health first
        if (State->Health.IsBroken()) {
            return State->Health.GetError();
        }

        if (cancelled) {
            return MakeError(E_REJECTED, "write cancelled");
        }

        // Publish before acking the caller. Quorum is monotonic, so we
        // do not care about actual ordering here.
        State->QuorumLsn.advance(link.Lsn);

        return {};
    }

    NProto::TError ReadPages(
        NProto::TDeviceRequestHeaders headers,
        const TVector<TPageGroupRef>& pageGroupRefs,
        TVector<TPageGroup>* pageGroups) override
    {
        if (auto error = CheckReady(); HasError(error)) {
            return error;
        }

        pageGroups->clear();
        FillHeaders(State->Config, &headers);
        auto request = MakeReadPagesRequest(
            std::move(headers),
            pageGroupRefs,
            State->Config.PageSize);

        if (auto error = ShiftToDevice(request); HasError(error)) {
            return error;
        }

        State->ReadsInflight.Increment();

        auto ctx = State->MakeContext();
        const ui64 required = State->QuorumLsn.get();
        const ui32 count = State->Proxies.size();
        const ui32 start = State->Selector.fetch_add(
            1,
            std::memory_order_relaxed);

        NProto::TError lastError = MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "no replica has reached expected lsn " << required);

        for (ui32 j = 0; j < count; ++j) {
            auto& proxy = *State->Proxies[(start + j) % count];
            if (!proxy.CanServe(required)) {
                continue;
            }

            SILK_DEBUG(
                "sg read at lsn %lu: %s",
                required,
                request.ShortUtf8DebugString().c_str());

            NProto::TReadPagesResponse response;
            auto error = proxy.Read(ctx, request, &response);
            if (!HasError(error)) {
                ExtractPageGroups(response, pageGroups);
                State->ReadsInflight.Decrement();
                return ShiftToClient(pageGroups);
            }

            lastError = std::move(error);
        }

        State->ReadsInflight.Decrement();
        return lastError;
    }

private:
    NProto::TError CheckReady() const
    {
        if (State->Health.IsBroken()) {
            return State->Health.GetError();
        }
        if (!State->Initialized) {
            return MakeError(E_REJECTED, "storage group is not initialized");
        }

        return {};
    }

private:
    TGroupStatePtr State;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageGroupPtr CreateQuorumMirroredStorageGroup(
    TStorageGroupConfig config,
    TVector<TStorageDevice> devices,
    ITimerPtr timer)
{
    return std::make_shared<TQuorumMirroredStorageGroup>(
        std::move(config),
        std::move(devices),
        std::move(timer));
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
