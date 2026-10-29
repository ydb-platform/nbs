#include "storage_group_quorum.h"

#include "storage_group_helpers.h"
#include "storage_group_inflight.h"

#include <cloud/fastshard/libs/common/context.h>
#include <cloud/fastshard/libs/fiber/event.h>

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/event.h>
#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/fibers/mutex.h>
#include <silk/fibers/sequencer.h>
#include <silk/util/logger.h>

#include <util/digest/city.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>

namespace NCloud::NFileStore::NStorage::NFastShard {

using NCloud::NFastShard::TFastShardContext;
using NCloud::NFastShard::WaitFor;

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
    void Fail(const NProto::TError& error)
    {
        {
            std::lock_guard g(Mutex);
            Error = MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "storage group broken: " << FormatError(error));

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

/**
 * Keeping track of largest write lsn acked so far.
 */
class TDeviceProxy
{
public:
    const TString DeviceUUID;

private:
    const IStorageNodePtr StorageNode;
    const TStorageGroupConfig Config;
    const ITimerPtr Timer;
    silk::FiberSequencer Acked;

public:
    TDeviceProxy(
            TStorageDevice device,
            TStorageGroupConfig config,
            ITimerPtr timer)
        : DeviceUUID(std::move(device.DeviceUUID))
        , StorageNode(std::move(device.Node))
        , Config(std::move(config))
        , Timer(std::move(timer))
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
            *Timer,
            [&]
            {
                NProto::TWriteLogRecordRequest deviceRequest = request;
                deviceRequest.SetDeviceUUID(DeviceUUID);
                return StorageNode->WriteLogRecord(std::move(deviceRequest));
            });

        if (HasError(response)) {
            return MakeProxyError(response.GetError(), "write");
        }

        // Acked only moves up, so we do not care about ordering here
        Acked.advance(request.GetLogSequenceNumber());
        return {};
    }

    NProto::TError Read(
        TFastShardContext& ctx,
        const NProto::TReadPagesRequest& request,
        NProto::TReadPagesResponse* response)
    {
        *response = CallWithRetries(
            ctx,
            Config.RetryPolicy,
            *Timer,
            [&]
            {
                NProto::TReadPagesRequest deviceRequest = request;
                deviceRequest.SetDeviceUUID(DeviceUUID);
                return StorageNode->ReadPages(std::move(deviceRequest));
            });

        if (HasError(*response)) {
            return MakeProxyError(response->GetError(), "read");
        }

        return {};
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
            *Timer,
            [&]
            {
                NProto::TReadJournalTailRequest deviceRequest = request;
                deviceRequest.SetDeviceUUID(DeviceUUID);
                return StorageNode->ReadJournalTail(std::move(deviceRequest));
            });

        if (HasError(*response)) {
            return MakeProxyError(
                response->GetError(),
                TStringBuilder() << "read tail after " << afterLsn);
        }

        return {};
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
            *Timer,
            [&]
            {
                return StorageNode->AdvanceLsnLowWatermark(request);
            });

        if (HasError(response)) {
            return MakeProxyError(
                response.GetError(),
                TStringBuilder() << "advance lwm to " << lsn);
        }

        return {};
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
            *Timer,
            [&]
            {
                return StorageNode->AcquireDevices(request);
            });

        if (HasError(response)) {
            return MakeProxyError(response.GetError(), "acquire");
        }

        return {};
    }

    NProto::TError Release(TFastShardContext& ctx)
    {
        NProto::TReleaseDevicesRequest request;
        FillHeaders(Config, request.MutableHeaders());
        request.AddDeviceUUIDs(DeviceUUID);

        auto response = CallWithRetries(
            ctx,
            Config.RetryPolicy,
            *Timer,
            [&]
            {
                return StorageNode->ReleaseDevices(request);
            });

        if (HasError(response)) {
            return MakeProxyError(response.GetError(), "release");
        }

        return {};
    }

private:
    NProto::TError MakeProxyError(const NProto::TError& error, TStringBuf what) const
    {
        return MakeError(
            error.GetCode(),
            TStringBuilder()
                << "[" << DeviceUUID << "] " << what
                << " failed: " << error.GetMessage(),
            error.GetFlags());
    }
};

using TDeviceProxyPtr = std::shared_ptr<TDeviceProxy>;

////////////////////////////////////////////////////////////////////////////////

struct TGroupState;
using TGroupStatePtr = std::shared_ptr<TGroupState>;
using TGroupStateRef = std::weak_ptr<TGroupState>;

// A periodic job of the group, with sync stop in TearDown.
struct TLoop
{
    TGroupStateRef State;
    TDuration Period;
    std::function<void(TFastShardContext&, TGroupState&)> Body;
    silk::FiberEvent Stop;
    silk::FiberFuture Done;
};

using TLoopPtr = std::shared_ptr<TLoop>;

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
    ui64 LowWatermarkPushed = 0;

    TVector<TLoopPtr> Loops;
    TInflightTracker InflightTracker;

    std::atomic<bool> Initialized = false;
    std::atomic<ui64> RequestId = 1;

    TGroupState(TStorageGroupConfig config, ITimerPtr timer)
        : Config(std::move(config))
        , Timer(std::move(timer))
        , InflightTracker(Config.MaxInflight)
    {}
};

////////////////////////////////////////////////////////////////////////////////

using TProxyCall = std::function<
    NProto::TError(TFastShardContext& ctx, ui32 index, TDeviceProxy& proxy)>;

struct TProxyCallParams
{
    const TProxyCall* Call;
    TDeviceProxy* Proxy;
    ui32 Index;
    TFastShardContext* Ctx;
    NProto::TError* Error;
};

int ProxyCallFiberMain(TProxyCallParams* params) noexcept
{
    *params->Error =
        (*params->Call)(*params->Ctx, params->Index, *params->Proxy);
    return 0;
}

NProto::TError ForEachProxy(
    TFastShardContext& ctx,
    const TGroupState& state,
    const TProxyCall& call)
{
    const ui32 count = state.Proxies.size();
    TVector<silk::FiberFuture> futures(count);
    TVector<NProto::TError> errors(count);

    for (ui32 i = 0; i < count; ++i) {
        const int r = silk::FiberScheduler::run(
            ProxyCallFiberMain,
            TProxyCallParams{
                .Call = &call,
                .Proxy = state.Proxies[i].get(),
                .Index = i,
                .Ctx = &ctx,
                .Error = &errors[i]},
            &futures[i]);
        if (r) {
            errors[i] = MakeSpawnError(r);
            futures[i].set(0);
        }
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

struct TWriteState: public TInflightRequest
{
    NProto::TWriteLogRecordRequest Request;
    ui64 Lsn = 0;

    silk::FiberSequencer Acked;

    std::atomic<ui32> Done = 0;

    TWriteState(ui64 requestId)
        : TInflightRequest(requestId)
    {}

    void Finish(TGroupState& state)
    {
        if (++Done == state.Proxies.size() + 1) {
            state.InflightTracker.Remove(*this);
        }
    }
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

    auto error = proxy.Write(params->Op->Ctx, params->Op->Request);
    // The proxy has already retried per the policy: whatever error is left
    // is final for this device.
    if (HasError(error)) {
        // Break the group first, so the writer this wakes finds it broken.
        if (!params->Op->Ctx.Cancel.isSet()) {
            state.Health.Fail(error);
        }
        params->Op->Acked.stop();
    } else if (params->Op->Acked.increment() == state.Proxies.size()) {
        state.LowWatermarkLsn.advance(params->Op->Lsn);
    }

    params->Op->Finish(state);
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

    return proxy.Write(ctx, request);
}

NProto::TError ValidateDevice(
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
        return error;
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
        ctx,
        state,
        [&](TFastShardContext& ctx, ui32, TDeviceProxy& proxy)
        {
            auto error = ValidateDevice(ctx, state, proxy);
            if (HasError(error)) {
                SILK_ERROR(
                    "sg device validation failed: %s",
                    FormatError(error).c_str());
            }

            return error;
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
    // Where each device's replay starts in the records; -1 for none.
    TVector<int> startPositions(maxLsnPerDevice.size(), -1);
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

        startPositions[i] = std::distance(records.begin(), it);
    }

    return ForEachProxy(
        ctx,
        state,
        [&](TFastShardContext& ctx, ui32 index, TDeviceProxy& proxy)
            -> NProto::TError
        {
            for (int i = startPositions[index]; i >= 0 && i < records.size(); ++i) {
                NProto::TDeviceRequestHeaders headers;
                FillHeaders(state.Config, &headers);
                auto error = proxy.Write(
                    ctx,
                    MakeReplayRequest(
                        std::move(headers),
                        records[i]));

                if (HasError(error)) {
                    return error;
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
    auto error = source.ReadJournalTail(
        ctx,
        afterLsn,
        0 /*no limit*/,
        tail);

    if (HasError(error)) {
        return error;
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
// from its header read, or from the header page written to it.
NProto::TError RebuildJournal(TFastShardContext& ctx, TGroupState& state)
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

////////////////////////////////////////////////////////////////////////////////

void PushLowWatermarkLoop(TFastShardContext& ctx, TGroupState& state)
{
    const ui64 watermark = state.LowWatermarkLsn.get();
    if (watermark <= state.LowWatermarkPushed) {
        return;
    }

    ForEachProxy(
        ctx,
        state,
        [&](TFastShardContext& ctx, ui32, TDeviceProxy& proxy)
            -> NProto::TError
        {
            auto error = proxy.AdvanceLsnLowWatermark(ctx, watermark);
            if (!HasError(error)) {
                return {};
            }

            if (GetErrorKind(error) != EErrorKind::ErrorRetriable) {
                state.Health.Fail(error);
                return {};
            }

            SILK_WARN(
                "sg low watermark not delivered: %s",
                FormatError(error).c_str());
            return {};
        });
    state.LowWatermarkPushed = watermark;
}

////////////////////////////////////////////////////////////////////////////////

void RenewSessionsLoop(TFastShardContext& ctx, TGroupState& state)
{
    ForEachProxy(
        ctx,
        state,
        [&](TFastShardContext& ctx, ui32, TDeviceProxy& proxy)
            -> NProto::TError
        {
            auto error = proxy.Acquire(ctx);
            if (!HasError(error)) {
                return {};
            }

            const auto kind = GetErrorKind(error);
            if (kind == EErrorKind::ErrorSession || kind == EErrorKind::ErrorFatal)
            {
                state.Health.Fail(error);
                return {};
            }

            SILK_WARN(
                "sg session not renewed: %s",
                FormatError(error).c_str());
            return {};
        });
}

////////////////////////////////////////////////////////////////////////////////

int LoopFiberMain(TLoopPtr* params) noexcept
{
    auto& loop = **params;
    for (;;) {
        if (WaitFor(loop.Stop, loop.Period)) {
            break;
        }

        auto state = loop.State.lock();
        if (!state || state->Health.IsBroken()) {
            break;
        }

        TInflightRequest op(state->RequestId++);
        if (auto error = state->InflightTracker.AddUnlimited(op); HasError(error))
        {
            break;
        }

        loop.Body(op.Ctx, *state);
        state->InflightTracker.RemoveUnlimited(op);
    }

    return 0;
}

TResultOrError<TLoopPtr> RunLoop(
    const TGroupStatePtr& state,
    TDuration period,
    std::function<void(TFastShardContext&, TGroupState&)> body)
{
    auto loop = std::make_shared<TLoop>();
    loop->State = state;
    loop->Period = period;
    loop->Body = std::move(body);

    const int r =
        silk::FiberScheduler::run(LoopFiberMain, TLoopPtr(loop), &loop->Done);
    if (r) {
        return MakeSpawnError(r);
    }

    return loop;
}

////////////////////////////////////////////////////////////////////////////////

class TQuorumMirroredStorageGroup final: public IStorageGroup
{
public:
    TQuorumMirroredStorageGroup(
            TStorageGroupConfig config,
            TVector<TStorageDevice> devices,
            ITimerPtr timer)
        : State(std::make_shared<TGroupState>(
              std::move(config),
              std::move(timer)))
    {
        State->WriteQuorum = devices.size() / 2 + 1;
        State->Proxies.reserve(devices.size());
        for (auto& device: devices) {
            State->Proxies.push_back(std::make_shared<TDeviceProxy>(
                std::move(device),
                State->Config,
                State->Timer));
        }
    }

    TResultOrError<ui64> Init() override
    {
        if (State->Proxies.empty()) {
            return MakeError(E_ARGUMENT, "empty device configuration");
        }

        TInflightRequest op(State->RequestId++);
        if (auto error = State->InflightTracker.AddUnlimited(op); HasError(error))
        {
            return error;
        }

        auto result = DoInit(op.Ctx);
        State->InflightTracker.RemoveUnlimited(op);
        return result;
    }

    void TearDown() override
    {
        State->InflightTracker.Stop();
        State->InflightTracker.Wait();

        // Nothing starts a loop once Init is drained.
        for (const auto& loop: State->Loops) {
            loop->Stop.set();
        }

        for (const auto& loop: State->Loops) {
            Y_UNUSED(loop->Done.wait());
        }

        TFastShardContext ctx(State->RequestId++);
        auto error = ForEachProxy(
            ctx,
            *State,
            [](TFastShardContext& ctx, ui32, TDeviceProxy& proxy)
            {
                return proxy.Release(ctx);
            });

        if (HasError(error)) {
            SILK_WARN("sg release error=%s", FormatError(error).c_str());
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
        auto op = std::make_shared<TWriteState>(State->RequestId++);
        op->Lsn = link.Lsn;
        op->Request = MakeWriteLogRecordRequest(
            std::move(headers),
            pageGroups,
            link);
        if (auto error = ShiftToDevice(op->Request); HasError(error)) {
            return error;
        }

        SILK_DEBUG("sg write: %s", DebugMessage(op->Request).c_str());
        if (auto error = State->InflightTracker.Add(*op); HasError(error)) {
            return error;
        }

        auto error = DoWrite(op);
        op->Finish(*State);
        return error;
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

        TInflightRequest op(State->RequestId++);
        if (auto error = State->InflightTracker.Add(op); HasError(error)) {
            return error;
        }

        auto error = DoRead(op.Ctx, request, pageGroups);
        State->InflightTracker.Remove(op);

        return error;
    }

private:
    TResultOrError<ui64> DoInit(TFastShardContext& ctx)
    {
        auto error = ForEachProxy(
            ctx,
            *State,
            [](TFastShardContext& ctx, ui32, TDeviceProxy& proxy)
            {
                return proxy.Acquire(ctx);
            });

        if (HasError(error)) {
            return error;
        }

        // launch reacquire loop immediately
        auto loop = RunLoop(
            State,
            State->Config.ReacquirePeriod,
            RenewSessionsLoop);
        if (HasError(loop)) {
            return loop.GetError();
        }
        State->Loops.push_back(loop.ExtractResult());

        error = ValidateGroupConfigAndInitIfNeeded(ctx, *State);
        if (HasError(error)) {
            return error;
        }

        error = RebuildJournal(ctx, *State);
        if (HasError(error)) {
            return error;
        }

        loop = RunLoop(
            State,
            State->Config.LowWatermarkPeriod,
            PushLowWatermarkLoop);
        if (HasError(loop)) {
            return loop.GetError();
        }
        State->Loops.push_back(loop.ExtractResult());

        State->Initialized = true;
        return State->QuorumLsn.get();
    }

    NProto::TError DoWrite(const TWriteStatePtr& op)
    {
        for (const auto& proxy: State->Proxies) {
            const int r = silk::FiberScheduler::run(
                WriteDispatchFiberMain,
                TWriteDispatchParams{
                    .State = State,
                    .Op = op,
                    .Proxy = proxy},
                nullptr);
            if (r) {
                State->Health.Fail(MakeSpawnError(r));
                op->Acked.stop();
                op->Finish(*State);
            }
        }

        const int cancelled = op->Acked.wait(State->WriteQuorum);

        // Check overall health first
        if (State->Health.IsBroken()) {
            return State->Health.GetError();
        }

        if (cancelled) {
            return MakeError(E_REJECTED, "write cancelled");
        }

        // Publish before acking the caller. Quorum is monotonic, so we
        // do not care about actual ordering here.
        State->QuorumLsn.advance(op->Lsn);

        return {};
    }

    NProto::TError DoRead(
        TFastShardContext& ctx,
        const NProto::TReadPagesRequest& request,
        TVector<TPageGroup>* pageGroups)
    {
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
                return ShiftToClient(pageGroups);
            }

            lastError = std::move(error);
        }

        return lastError;
    }

    NProto::TError CheckReady() const
    {
        if (!State->Initialized) {
            return MakeError(E_REJECTED, "storage group is shutdown");
        }

        if (State->Health.IsBroken()) {
            return State->Health.GetError();
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
