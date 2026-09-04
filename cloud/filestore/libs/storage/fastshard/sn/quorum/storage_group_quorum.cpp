#include "storage_group_quorum.h"

#include "storage_group_helpers.h"

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/mutex.h>
#include <silk/fibers/sequencer.h>
#include <silk/util/logger.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <atomic>
#include <memory>
#include <mutex>

namespace NCloud::NFileStore::NStorage::NFastShard {
namespace {

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
            TStorageGroupRetryPolicy retryPolicy,
            ITimerPtr timer)
        : DeviceUUID(std::move(device.DeviceUUID))
        , StorageNode(std::move(device.Node))
        , RetryPolicy(retryPolicy)
        , Timer(std::move(timer))
    {}

    bool CanServe(ui64 lsn) const
    {
        return Acked.get() >= lsn;
    }

    NProto::TError Write(const NProto::TWriteLogRecordRequest& request)
    {
        auto response = CallWithRetries(
            RetryPolicy,
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
            RetryPolicy,
            *Timer,
            [&]
            {
                NProto::TReadPagesRequest deviceRequest = request;
                deviceRequest.SetDeviceUUID(DeviceUUID);
                return StorageNode->ReadPages(std::move(deviceRequest));
            });

        return response->GetError();
    }

public:
    const TString DeviceUUID;
    const IStorageNodePtr StorageNode;

private:
    const TStorageGroupRetryPolicy RetryPolicy;
    const ITimerPtr Timer;

    silk::FiberSequencer Acked;
};

using TDeviceProxyPtr = std::shared_ptr<TDeviceProxy>;

////////////////////////////////////////////////////////////////////////////////

struct TGroupState
{
    TGroupHealth Health;
    TVector<TDeviceProxyPtr> Proxies;
    ui32 WriteQuorum = 0;

    std::atomic<ui32> Selector = 0;

    // Highest lsn the group has acked so far. Readers use it as a basic filter
    // for the devices.
    silk::FiberSequencer QuorumLsn;
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
    // Expect non-retriable code
    if (HasError(error)) {
        // Break the group first, so the writer this wakes finds it broken.
        state.Health.Fail(error, proxy.DeviceUUID);
        params->Op->Acks.stop();
        return 0;
    }

    params->Op->Acks.increment();
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

class TQuorumMirroredStorageGroup final: public IStorageGroup
{
public:
    TQuorumMirroredStorageGroup(
            TVector<TStorageDevice> devices,
            TStorageGroupRetryPolicy retryPolicy,
            ITimerPtr timer)
        : State(std::make_shared<TGroupState>())
    {
        // TODO(#5895): handle a bad device list gracefully instead of aborting.
        Y_ABORT_UNLESS(!devices.empty(), "storage group needs a device");

        State->WriteQuorum = devices.size() / 2 + 1;
        State->Proxies.reserve(devices.size());
        for (auto& device: devices) {
            State->Proxies.push_back(
                std::make_shared<TDeviceProxy>(
                    std::move(device),
                    retryPolicy,
                    timer));
        }

        RetryPolicy = retryPolicy;
        Timer = std::move(timer);
    }

    NProto::TError AcquireDevices() override
    {
        return MirrorRequest<NProto::TAcquireDevicesResponse>(
            CollectDeviceList(*State),
            RetryPolicy,
            *Timer,
            AcquireDevicesFiberMain,
            NProto::TAcquireDevicesRequest{});
    }

    NProto::TError ReleaseDevices() override
    {
        return MirrorRequest<NProto::TReleaseDevicesResponse>(
            CollectDeviceList(*State),
            RetryPolicy,
            *Timer,
            ReleaseDevicesFiberMain,
            NProto::TReleaseDevicesRequest{});
    }

    NProto::TError WriteLogRecord(
        NProto::TDeviceRequestHeaders headers,
        TVector<TPageGroup> pageGroups,
        ui64 lsn) override
    {
        if (State->Health.IsBroken()) {
            return State->Health.GetError();
        }

        if (!lsn) {
            return MakeError(E_ARGUMENT, "lsn must be positive");
        }

        auto op = std::make_shared<TWriteState>();
        op->Lsn = lsn;
        op->Request = MakeWriteLogRecordRequest(
            std::move(headers),
            pageGroups,
            lsn);

        SILK_DEBUG("sg write: %s", DebugMessage(op->Request).c_str());
        for (const auto& proxy: State->Proxies) {
            const int r = silk::FiberScheduler::run(
                WriteDispatchFiberMain,
                TWriteDispatchParams{
                    .State = State,
                    .Op = op,
                    .Proxy = proxy},
                nullptr);

            if (r) {
                State->Health.Fail(
                    MakeError(MAKE_SYSTEM_ERROR(r), "failed to spawn fiber"),
                    proxy->DeviceUUID);
                op->Acks.stop();
            }
        }

        const int cancelled = op->Acks.wait(State->WriteQuorum);

        // Check overall health first
        if (State->Health.IsBroken()) {
            return State->Health.GetError();
        }

        if (cancelled) {
            return MakeError(E_REJECTED, "write cancelled");   // unreachable
        }

        // Publish before acking the caller. Quorum is monotonic, so we
        // do not care about actual ordering here.
        State->QuorumLsn.advance(lsn);

        return {};
    }

    NProto::TError ReadPages(
        NProto::TDeviceRequestHeaders headers,
        const TVector<TPageGroupRef>& pageGroupRefs,
        TVector<TPageGroup>* pageGroups) override
    {
        if (State->Health.IsBroken()) {
            return State->Health.GetError();
        }

        pageGroups->clear();
        const auto request = MakeReadPagesRequest(
            std::move(headers),
            pageGroupRefs);

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
                return {};
            }

            lastError = std::move(error);
        }

        return lastError;
    }

private:
    TGroupStatePtr State;
    TStorageGroupRetryPolicy RetryPolicy;
    ITimerPtr Timer;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageGroupPtr CreateQuorumMirroredStorageGroup(
    TVector<TStorageDevice> devices,
    TStorageGroupRetryPolicy retryPolicy,
    ITimerPtr timer)
{
    return std::make_shared<TQuorumMirroredStorageGroup>(
        std::move(devices),
        std::move(retryPolicy),
        std::move(timer));
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
