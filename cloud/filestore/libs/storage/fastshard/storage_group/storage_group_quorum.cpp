#include "storage_group_quorum.h"

#include "storage_group_helpers.h"
#include "storage_group_impl.h"
#include "storage_group_inflight.h"
#include "storage_group_proxy.h"

#include <cloud/fastshard/libs/common/context.h>

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/fibers/sequencer.h>
#include <silk/util/logger.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <algorithm>
#include <atomic>
#include <functional>
#include <memory>
#include <utility>

namespace NCloud::NFileStore::NStorage::NFastShard {

using NCloud::NFastShard::TFastShardContext;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 QuorumMirrorGroupType = NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR;

////////////////////////////////////////////////////////////////////////////////

struct TQuorumState: TGroupState
{
    TVector<TDeviceProxyPtr> Proxies;
    ui32 WriteQuorum = 0;

    std::atomic<ui32> Selector = 0;

    // Highest lsn the group has acked so far. Readers use it as a basic filter
    // for the devices.
    silk::FiberSequencer QuorumLsn;

    // Highest lsn acked by every device.
    silk::FiberSequencer LowWatermarkLsn;
    ui64 LowWatermarkPushed = 0;

    using TGroupState::TGroupState;
};

using TQuorumStatePtr = std::shared_ptr<TQuorumState>;

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
    const TQuorumState& state,
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

    explicit TWriteState(ui64 requestId)
        : TInflightRequest(requestId)
    {}

    void Finish(TQuorumState& state)
    {
        if (++Done == state.Proxies.size()) {
            if (Acked.get() == state.Proxies.size()) {
                state.LowWatermarkLsn.advance(Lsn);
            }
            state.InflightTracker.Remove(*this);
        }
    }
};

using TWriteStatePtr = std::shared_ptr<TWriteState>;

struct TWriteDispatchParams
{
    TQuorumStatePtr State;
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
    } else {
        params->Op->Acked.increment();
    }

    params->Op->Finish(state);
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

NProto::TError ValidateGroupConfigAndInitIfNeeded(
    TFastShardContext& ctx,
    const TQuorumState& state)
{
    return ForEachProxy(
        ctx,
        state,
        [&](TFastShardContext& ctx, ui32, TDeviceProxy& proxy)
        {
            auto error = ValidateDevice(
                ctx,
                state.Config,
                QuorumMirrorGroupType,
                proxy);
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
    const TQuorumState& state,
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
NProto::TError RebuildJournal(TFastShardContext& ctx, TQuorumState& state)
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

void PushLowWatermarkLoop(TFastShardContext& ctx, TQuorumState& state)
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
            PushLowWatermark(ctx, state.Health, proxy, watermark);
            return {};
        });
    state.LowWatermarkPushed = watermark;
}

////////////////////////////////////////////////////////////////////////////////

void RenewSessionsLoop(TFastShardContext& ctx, TQuorumState& state)
{
    ForEachProxy(
        ctx,
        state,
        [&](TFastShardContext& ctx, ui32, TDeviceProxy& proxy)
            -> NProto::TError
        {
            RenewSession(ctx, state.Health, proxy);
            return {};
        });
}

////////////////////////////////////////////////////////////////////////////////

class TQuorumMirroredStorageGroup final: public IStorageGroup
{
private:
    TQuorumStatePtr State;

public:
    TQuorumMirroredStorageGroup(
            TStorageGroupConfig config,
            TVector<TStorageDevice> devices,
            ITimerPtr timer)
        : State(std::make_shared<TQuorumState>(
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
        if (auto error = State->InflightTracker.Add(op); HasError(error)) {
            return error;
        }

        auto result = DoInit(op.Ctx);
        State->InflightTracker.Remove(op);
        return result;
    }

    void TearDown() override
    {
        State->Stop();

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
        if (auto error = State->CheckReady(); HasError(error)) {
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

        const ui32 threshold = State->Config.WriteBackpressureThreshold;
        ui32 quorum = State->WriteQuorum;
        if (threshold &&
            State->InflightTracker.GetInflightCount() > threshold)
        {
            quorum = State->Proxies.size();
        }

        return DoWrite(op, quorum);
    }

    NProto::TError ReadPages(
        NProto::TDeviceRequestHeaders headers,
        const TVector<TPageGroupRef>& pageGroupRefs,
        TVector<TPageGroup>* pageGroups) override
    {
        if (auto error = State->CheckReady(); HasError(error)) {
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

        // Torn down meanwhile: report the init as rejected.
        if (ctx.Cancel.isSet()) {
            return MakeError(E_REJECTED, "storage group is shutting down");
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

    NProto::TError DoWrite(const TWriteStatePtr& op, ui32 required)
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

        const int cancelled = op->Acked.wait(required);

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
