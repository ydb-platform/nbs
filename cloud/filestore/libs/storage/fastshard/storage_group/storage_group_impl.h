#pragma once

#include "storage_group.h"
#include "storage_group_inflight.h"
#include "storage_group_proxy.h"

#include <cloud/fastshard/libs/common/context.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/event.h>
#include <silk/fibers/future.h>
#include <silk/fibers/mutex.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

#include <atomic>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

/**
 * Layout header for initialized groups. Pages 1-7 reserved for the future
 */
struct TStorageGroupHeader
{
    static constexpr ui64 Magic = 0x4653545348415244; // FSTSHARD
    static constexpr ui32 CurrentVersion = 1;
    static constexpr ui32 StorageGroupReservedPages = 8;

    ui64 MagicNumber = Magic;
    ui32 Version:8 = CurrentVersion;
    ui32 GroupType:24 = 0;
    ui32 PageSize = 0;
    ui64 DeviceUUIDHash = 0;
};

static_assert(sizeof(TStorageGroupHeader) == 24);
static_assert(std::is_trivially_copyable_v<TStorageGroupHeader>);

////////////////////////////////////////////////////////////////////////////////

// The header takes the first pages of a device: the caller's page numbers
// start past them.
NProto::TError ShiftToDevice(NProto::TWriteLogRecordRequest& request);
NProto::TError ShiftToDevice(NProto::TReadPagesRequest& request);
NProto::TError ShiftToClient(TVector<TPageGroup>* pageGroups);

// Checks the device's header, writing one to a blank device, and seeds the
// proxy with the lsn the device has acked.
NProto::TError ValidateDevice(
    NCloud::NFastShard::TFastShardContext& ctx,
    const TStorageGroupConfig& config,
    ui32 groupType,
    TDeviceProxy& proxy);

////////////////////////////////////////////////////////////////////////////////

class TGroupHealth
{
public:
    void Fail(const NProto::TError& error);

    bool IsBroken() const
    {
        return BrokenFlag.load(std::memory_order_acquire);
    }

    NProto::TError GetError() const;

private:
    mutable silk::FiberMutex Mutex;
    NProto::TError Error;
    std::atomic<bool> BrokenFlag{false};
};

////////////////////////////////////////////////////////////////////////////////

// A periodic job of the group, with sync stop in TearDown.
struct TLoop
{
    TDuration Period;
    // One round; false once the group is gone or broken.
    std::function<bool()> Tick;
    silk::FiberEvent Stop;
    silk::FiberFuture Done;
};

using TLoopPtr = std::shared_ptr<TLoop>;

TResultOrError<TLoopPtr> RunLoop(TDuration period, std::function<bool()> tick);

////////////////////////////////////////////////////////////////////////////////

// What every group keeps whatever its devices.
struct TGroupState
{
    TStorageGroupConfig Config;
    ITimerPtr Timer;
    TGroupHealth Health;

    TVector<TLoopPtr> Loops;
    TInflightTracker InflightTracker;

    std::atomic<bool> Initialized = false;
    std::atomic<ui64> RequestId = 1;

    TGroupState(TStorageGroupConfig config, ITimerPtr timer)
        : Config(std::move(config))
        , Timer(std::move(timer))
    {}

    NProto::TError CheckReady() const;

    // Cancels and drains the requests in flight, then stops the loops.
    void Stop();
};

// Runs @p body every @p period as a request of the group, while the group
// lives and is not broken.
template <typename TState, typename TBody>
TResultOrError<TLoopPtr> RunLoop(
    const std::shared_ptr<TState>& state,
    TDuration period,
    TBody body)
{
    return RunLoop(
        period,
        [ref = std::weak_ptr<TState>(state), body = std::move(body)]
        {
            auto state = ref.lock();
            if (!state || state->Health.IsBroken()) {
                return false;
            }

            TInflightRequest op(state->RequestId++);
            if (HasError(state->InflightTracker.Add(op))) {
                return false;
            }

            body(op.Ctx, *state);
            state->InflightTracker.Remove(op);
            return true;
        });
}

////////////////////////////////////////////////////////////////////////////////

// One device's part of the loops: an error the proxy could not retry away
// breaks the group, a retriable one waits for the next round.
void PushLowWatermark(
    NCloud::NFastShard::TFastShardContext& ctx,
    TGroupHealth& health,
    TDeviceProxy& proxy,
    ui64 lsn);

void RenewSession(
    NCloud::NFastShard::TFastShardContext& ctx,
    TGroupHealth& health,
    TDeviceProxy& proxy);

}   // namespace NCloud::NFileStore::NStorage::NFastShard
