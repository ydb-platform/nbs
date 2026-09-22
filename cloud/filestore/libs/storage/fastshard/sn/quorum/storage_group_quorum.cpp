#include "storage_group_quorum.h"

#include "storage_group_helpers.h"

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/fibers/mutex.h>
#include <silk/fibers/sequencer.h>
#include <silk/util/logger.h>

#include <util/digest/city.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>

namespace NCloud::NFileStore::NStorage::NFastShard {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 ReservedPages = TStorageGroupHeader::StorageGroupReservedPages;
constexpr ui32 QuorumMirrorGroupType = NProtoPrivate::TStorageGroup::E_SG_QUORUM_MIRROR;

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

/**
 * Keeping track of largest write lsn acked so far.
 */
class TDeviceProxy
{
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

    NProto::TError Write(const NProto::TWriteLogRecordRequest& request)
    {
        auto response = CallWithRetries(
            Config.RetryPolicy,
            *Timer,
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
        const NProto::TReadPagesRequest& request,
        NProto::TReadPagesResponse* response)
    {
        *response = CallWithRetries(
            Config.RetryPolicy,
            *Timer,
            [&]
            {
                NProto::TReadPagesRequest deviceRequest = request;
                deviceRequest.SetDeviceUUID(DeviceUUID);
                return StorageNode->ReadPages(std::move(deviceRequest));
            });

        return response->GetError();
    }

    NProto::TError ReadJournalTail(
        ui64 afterLsn,
        ui32 maxRecords,
        NProto::TReadJournalTailResponse* response)
    {
        NProto::TReadJournalTailRequest request;
        FillHeaders(Config, request.MutableHeaders());
        request.SetAfterLogSequenceNumber(afterLsn);
        request.SetMaxRecordCount(maxRecords);

        *response = CallWithRetries(
            Config.RetryPolicy,
            *Timer,
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

    NProto::TError AdvanceLsnLowWatermark(ui64 lsn)
    {
        NProto::TAdvanceLsnLowWatermarkRequest request;
        FillHeaders(Config, request.MutableHeaders());
        request.SetDeviceUUID(DeviceUUID);
        request.SetLsnLowWatermark(lsn);

        auto response = CallWithRetries(
            Config.RetryPolicy,
            *Timer,
            [&]
            {
                return StorageNode->AdvanceLsnLowWatermark(request);
            });

        return response.GetError();
    }

public:
    const TString DeviceUUID;
    const IStorageNodePtr StorageNode;

private:
    const TStorageGroupConfig Config;
    const ITimerPtr Timer;

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

    std::atomic<bool> Initialized = false;
    std::atomic<bool> Stopped = false;

    silk::FiberFuture WatermarkLoopStopped;
};

using TGroupStatePtr = std::shared_ptr<TGroupState>;

TVector<TStorageDevice> CollectDeviceList(const TGroupState& state)
{
    TVector<TStorageDevice> devices;
    devices.reserve(state.Proxies.size());
    for (const auto& proxy: state.Proxies) {
        devices.push_back({
            .Node = proxy->StorageNode,
            .DeviceUUID = proxy->DeviceUUID,
        });
    }

    return devices;
}

////////////////////////////////////////////////////////////////////////////////

struct TWriteState
{
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

    auto error = proxy.Write(params->Op->Request);
    // The proxy has already retried per the policy: whatever error is left
    // is final for this device.
    if (HasError(error)) {
        // Break the group first, so the writer this wakes finds it broken.
        state.Health.Fail(error, proxy.DeviceUUID);
        params->Op->Acks.stop();
        return 0;
    }

    if (params->Op->Acks.increment() == state.Proxies.size()) {
        state.LowWatermarkLsn.advance(params->Op->Lsn);
    }

    return 0;
}

////////////////////////////////////////////////////////////////////////////////

struct TValidateDeviceParams
{
    TGroupStatePtr State;
    TDeviceProxyPtr Proxy;
    NProto::TError* Error;
};

NProto::TError InitializeDevice(
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

    auto error = proxy.Write(request);
    if (HasError(error)) {
        return MakeError(
            error.GetCode(),
            TStringBuilder()
                << proxy.DeviceUUID << " header write failed: "
                << FormatError(error));
    }

    return {};
}

int ValidateDeviceConfigAndInitIfNeeded(TValidateDeviceParams* params) noexcept
{
    const auto& state = *params->State;
    auto& proxy = *params->Proxy;

    NProto::TReadPagesRequest request;
    FillHeaders(state.Config, request.MutableHeaders());

    auto* ref = request.AddPageGroupRefs();
    ref->SetPageSize(state.Config.PageSize);
    ref->SetFirstPageNo(0);
    ref->SetPageCount(1);

    NProto::TReadPagesResponse response;
    auto error = proxy.Read(request, &response);
    if (HasError(error)) {
        *params->Error = MakeError(
            error.GetCode(),
            TStringBuilder()
                << proxy.DeviceUUID << " header read failed: "
                << FormatError(error));
        return 0;
    }

    const auto& groups = response.GetPageGroups();
    if (groups.size() != 1 || groups.Get(0).ContentSize() != 1) {
        *params->Error = MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << proxy.DeviceUUID << " header read returned "
                << groups.size() << " page groups instead of one page");
        return 0;
    }

    const auto& content = groups.Get(0).GetContent(0);
    if (content.size() != state.Config.PageSize) {
        *params->Error = MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << proxy.DeviceUUID << " header page is " << content.size()
                << " bytes instead of " << state.Config.PageSize);
        return 0;
    }

    if (IsAllZeroes(content.data(), content.size())) {
        *params->Error = InitializeDevice(state, proxy);
        return 0;
    }

    TStorageGroupHeader header;
    std::memcpy(&header, content.data(), sizeof(header));

    if (header.MagicNumber != TStorageGroupHeader::Magic ||
        header.Version != TStorageGroupHeader::CurrentVersion ||
        header.GroupType != QuorumMirrorGroupType ||
        header.PageSize != state.Config.PageSize ||
        header.DeviceUUIDHash != HashDeviceUUID(proxy.DeviceUUID))
    {
        *params->Error = MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << proxy.DeviceUUID << " header mismatch: " << header
                << ", expected page size " << state.Config.PageSize);
    }

    return 0;
}

NProto::TError ValidateGroupConfigAndInitIfNeeded(const TGroupStatePtr& state)
{
    const ui32 count = state->Proxies.size();
    TVector<NProto::TError> errors(count);
    TVector<silk::FiberFuture> futures(count);

    for (ui32 i = 0; i < count; ++i) {
        const int r = silk::FiberScheduler::run(
            ValidateDeviceConfigAndInitIfNeeded,
            TValidateDeviceParams{
                .State = state,
                .Proxy = state->Proxies[i],
                .Error = &errors[i],
            },
            &futures[i]);
        Y_ABORT_UNLESS(r == 0, "failed to spawn fiber: %s", ::strerror(r));
    }

    NProto::TError error;
    for (ui32 i = 0; i < count; ++i) {
        futures[i].wait();
        if (!HasError(errors[i])) {
            continue;
        }

        SILK_ERROR(
            "sg device validation failed: %s",
            FormatError(errors[i]).c_str());

        if (!HasError(error)) {
            error = MakeError(
                errors[i].GetCode(),
                TStringBuilder()
                    << "group setup validation failed: "
                    << FormatError(errors[i]));
        }
    }

    return error;
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

struct TReplayParams
{
    TGroupStatePtr State;
    TDeviceProxyPtr Proxy;
    const TJournalRecords* Records;
    ui32 StartPosition;
    NProto::TError* Error;
};

int ReplayFiberMain(TReplayParams* params) noexcept
{
    const auto& records = *params->Records;
    for (int i = params->StartPosition; i < records.size(); ++i) {
        NProto::TDeviceRequestHeaders headers;
        FillHeaders(params->State->Config, &headers);
        auto error = params->Proxy->Write(
            MakeReplayRequest(std::move(headers), records[i]));

        if (HasError(error)) {
            *params->Error = std::move(error);
            break;
        }
    }

    return 0;
}

NProto::TError ReplayOnDevicesBehind(
    const TGroupStatePtr& state,
    const TVector<ui64>& maxLsnPerDevice,
    ui64 maxKnownLsn,
    const TJournalRecords& records)
{
    TVector<ui32> proxyIndexesToReplay;
    TVector<ui32> journalPositionsToStartReplay;
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

        auto startPos = std::distance(records.begin(), it);
        if (startPos == records.size())
        {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "journal tail does not continue "
                    << state->Proxies[i]->DeviceUUID << " from lsn "
                    << maxLsnPerDevice[i]);
        }

        proxyIndexesToReplay.push_back(i);
        journalPositionsToStartReplay.push_back(startPos);
    }

    TVector<silk::FiberFuture> futures(proxyIndexesToReplay.size());
    TVector<NProto::TError> errors(proxyIndexesToReplay.size());
    for (ui32 i = 0; i < proxyIndexesToReplay.size(); ++i) {
        const int r = silk::FiberScheduler::run(
            ReplayFiberMain,
            TReplayParams{
                .State = state,
                .Proxy = state->Proxies[proxyIndexesToReplay[i]],
                .Records = &records,
                .StartPosition = journalPositionsToStartReplay[i],
                .Error = &errors[i]
            },
            &futures[i]);
        Y_ABORT_UNLESS(r == 0, "failed to spawn fiber: %s", ::strerror(r));
    }

    NProto::TError error;
    for (ui32 i = 0; i < proxyIndexesToReplay.size(); ++i) {
        futures[i].wait();
        if (HasError(errors[i]) && !HasError(error)) {
            error = MakeError(
                errors[i].GetCode(),
                TStringBuilder()
                    << "replay onto "
                    << state->Proxies[proxyIndexesToReplay[i]]->DeviceUUID
                    << " failed: " << FormatError(errors[i]));
        }
    }

    return error;
}

// Read records above @p afterLsn, from a device at @p maxKnownLsn.
NProto::TError ReadRecordsAbove(
    TDeviceProxy& source,
    ui64 afterLsn,
    ui64 maxKnownLsn,
    NProto::TReadJournalTailResponse* tail)
{
    auto error = source.ReadJournalTail(afterLsn, 0 /*no limit*/, tail);
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

struct TQueryPositionParams
{
    TDeviceProxyPtr Proxy;
    ui64* Lsn = nullptr;
    NProto::TError* Error = nullptr;
};

NProto::TError QueryCurrentJournalPosition(
    TGroupState& state,
    TVector<ui64>& maxLsnPerDevice)
{
    const ui32 count = state.Proxies.size();

    TVector<silk::FiberFuture> futures(count);
    TVector<NProto::TError> errors(count);
    for (ui32 i = 0; i < count; ++i) {
        const int r = silk::FiberScheduler::run<TQueryPositionParams>(
            [] (TQueryPositionParams* params) noexcept
            {
                NProto::TReadJournalTailResponse response;
                auto error = params->Proxy->ReadJournalTail(
                    0, // after Lsn
                    1, // max records
                    &response);

                if (HasError(error)) {
                    *params->Error = std::move(error);
                } else {
                    // TODO(#6957): so far device advances lsn only after wm moves
                    // gracefully handle sg device init writes.
                    *params->Lsn = std::max(
                        response.GetLastAckedLogSequenceNumber(),
                        params->Proxy->GetLastAckedLsn());
                }

                return 0;
            },
            TQueryPositionParams{
                .Proxy = state.Proxies[i],
                .Lsn = &maxLsnPerDevice[i],
                .Error = &errors[i]
            },
            &futures[i]);
        Y_ABORT_UNLESS(r == 0, "failed to spawn fiber: %s", ::strerror(r));
    }

    NProto::TError error;
    for (ui32 i = 0; i < count; ++i) {
        futures[i].wait();
        if (!HasError(errors[i])) {
            continue;
        }

        SILK_ERROR(
            "sg position of %s: %s",
            state.Proxies[i]->DeviceUUID.c_str(),
            FormatError(errors[i]).c_str());
        if (!HasError(error)) {
            error = errors[i];
        }
    }

    return error;
}

// Finds where every device is, reads what the slowest one lacks from the
// most advanced one and replays it onto everyone behind.
NProto::TError RebuildJournal(const TGroupStatePtr& state)
{
    TVector<ui64> maxLsnPerDevice(state->Proxies.size());
    auto error = QueryCurrentJournalPosition(*state, maxLsnPerDevice);
    if (HasError(error)) {
        return error;
    }

    for (ui32 i = 0; i < maxLsnPerDevice.size(); ++i) {
        state->Proxies[i]->SeedLsn(maxLsnPerDevice[i]);
    }

    auto low =
        std::min_element(maxLsnPerDevice.begin(), maxLsnPerDevice.end());
    auto high =
        std::max_element(maxLsnPerDevice.begin(), maxLsnPerDevice.end());
    if (*low < *high) {
        const ui32 source = std::distance(maxLsnPerDevice.begin(), high);

        NProto::TReadJournalTailResponse tail;
        error = ReadRecordsAbove(*state->Proxies[source], *low, *high, &tail);
        if (HasError(error)) {
            return error;
        }

        error = ReplayOnDevicesBehind(
            state,
            maxLsnPerDevice,
            *high,
            tail.GetRecords());
        if (HasError(error)) {
            return error;
        }
    }

    state->QuorumLsn.advance(*high);
    state->LowWatermarkLsn.advance(*high);

    SILK_INFO("sg rebuild: every device at lsn %lu", *high);
    return {};
}

struct TPushWatermarkParams
{
    TDeviceProxyPtr Proxy;
    ui64 Watermark;
    NProto::TError* Error;
};

void PushLowWatermarkEverywhere(TGroupState& state, ui64 watermark)
{
    const ui32 count = state.Proxies.size();
    TVector<silk::FiberFuture> futures(count);
    TVector<NProto::TError> errors(count);
    for (ui32 i = 0; i < count; ++i) {
        const int r = silk::FiberScheduler::run<TPushWatermarkParams>(
            [] (TPushWatermarkParams* params) noexcept
            {
                *params->Error = params->Proxy->AdvanceLsnLowWatermark(
                    params->Watermark);
                return 0;
            },
            TPushWatermarkParams{
                .Proxy = state.Proxies[i],
                .Watermark = watermark,
                .Error = &errors[i]},
            &futures[i]);
        Y_ABORT_UNLESS(r == 0, "failed to spawn fiber: %s", ::strerror(r));
    }

    for (ui32 i = 0; i < count; ++i) {
        futures[i].wait();
        if (!HasError(errors[i])) {
            continue;
        }

        if (GetErrorKind(errors[i]) != EErrorKind::ErrorRetriable) {
            state.Health.Fail(errors[i], state.Proxies[i]->DeviceUUID);
            continue;
        }

        SILK_WARN(
            "sg low watermark %lu not delivered to %s: %s",
            watermark,
            state.Proxies[i]->DeviceUUID.c_str(),
            FormatError(errors[i]).c_str());
    }
}

struct TWatermarkParams
{
    TGroupStatePtr State;
};

int LowWatermarkFiberMain(TWatermarkParams* params) noexcept
{
    auto& state = *params->State;
    ui64 pushed = 0;   // last watermark pushed

    for (;;) {
        state.Timer->Sleep(state.Config.LowWatermarkPeriod);
        if (state.Stopped || state.Health.IsBroken()) {
            break;
        }

        const ui64 watermark = state.LowWatermarkLsn.get();
        if (watermark <= pushed) {
            continue;
        }

        // A refusal breaks the group; the loop notices after its next sleep,
        // the only place it ever leaves from.
        PushLowWatermarkEverywhere(state, watermark);
        pushed = watermark;
    }
    return 0;
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
        // TODO(#5895): handle a bad device list gracefully instead of aborting.
        Y_ABORT_UNLESS(!devices.empty(), "storage group needs a device");

        State->Config = std::move(config);
        State->Timer = std::move(timer);
        State->WriteQuorum = devices.size() / 2 + 1;
        State->Proxies.reserve(devices.size());
        for (auto& device: devices) {
            State->Proxies.push_back(
                std::make_shared<TDeviceProxy>(
                    std::move(device),
                    State->Config,
                    State->Timer));
        }

        // Allow TearDown to pass by default. Init resets it upon launching
        // the loop.
        State->WatermarkLoopStopped.set(0);
    }

    TResultOrError<ui64> Init() override
    {
        NProto::TAcquireDevicesRequest acquire;
        acquire.SetGeneration(State->Config.AcquireGeneration);
        auto error = MirrorRequest<NProto::TAcquireDevicesResponse>(
            State->Config,
            CollectDeviceList(*State),
            *State->Timer,
            AcquireDevicesFiberMain,
            std::move(acquire));

        if (HasError(error)) {
            return error;
        }

        error = ValidateGroupConfigAndInitIfNeeded(State);
        if (HasError(error)) {
            return error;
        }

        error = RebuildJournal(State);
        if (HasError(error)) {
            return error;
        }

        if (State->Config.LowWatermarkPeriod != TDuration::Zero()) {
            State->WatermarkLoopStopped.reset();
            const int r = silk::FiberScheduler::run(
                LowWatermarkFiberMain,
                TWatermarkParams{.State = State},
                &State->WatermarkLoopStopped);
            Y_ABORT_UNLESS(r == 0, "failed to spawn fiber: %s", ::strerror(r));
        }

        State->Initialized = true;
        return State->QuorumLsn.get();
    }

    void TearDown() override
    {
        State->Stopped = true;

        // TODO(#6957): Need a proper cancellation token for requests inflight
        State->WatermarkLoopStopped.wait();

        auto error = MirrorRequest<NProto::TReleaseDevicesResponse>(
            State->Config,
            CollectDeviceList(*State),
            *State->Timer,
            ReleaseDevicesFiberMain,
            NProto::TReleaseDevicesRequest{});

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
        auto op = std::make_shared<TWriteState>();
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
            auto error = proxy.Read(request, &response);
            if (!HasError(error)) {
                ExtractPageGroups(response, pageGroups);
                return ShiftToClient(pageGroups);
            }

            lastError = std::move(error);
        }

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
