#pragma once

#include "storage_group.h"

#include <cloud/fastshard/libs/common/context.h>
#include <cloud/fastshard/libs/common/error.h>
#include <cloud/fastshard/libs/fiber/event.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/logger.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/system/datetime.h>

#include <type_traits>

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

inline void FillHeaders(
    const TStorageGroupConfig& config,
    NProto::TDeviceRequestHeaders* headers)
{
    headers->SetClientId(config.ClientId);
}

////////////////////////////////////////////////////////////////////////////////

/**
 * Repeats the call until it succeeds, fails with a non-retriable error, is
 * cancelled or runs out of the policy's total timeout.
 */
template <typename TCall>
auto CallWithRetries(
    NCloud::NFastShard::TFastShardContext& ctx,
    const TStorageGroupRetryPolicy& policy,
    ITimer& timer,
    TCall call)
{
    const TInstant start = timer.Now();
    ui32 errorCount = 0;

    for (;;) {
        if (ctx.Cancel.isSet()) {
            decltype(call()) response;
            *response.MutableError() = MakeError(E_REJECTED, "cancelled");
            return response;
        }

        auto response = call();
        const auto& error = response.GetError();
        if (GetErrorKind(error) != EErrorKind::ErrorRetriable) {
            return response;
        }

        if (timer.Now() - start >= policy.TotalTimeout) {
            SILK_ERROR(
                "sg req %lu: retries timed out after %u errors: %s",
                ctx.RequestId,
                errorCount + 1,
                FormatError(error).c_str());

            return response;
        }

        ++errorCount;
        const TDuration backoff = policy.BackoffIncrement * errorCount;
        SILK_DEBUG(
            "sg req %lu: retry #%u, backoff: %luus, error: %s",
            ctx.RequestId,
            errorCount,
            backoff.MicroSeconds(),
            FormatError(error).c_str());

        const ui64 parked = GetCycleCount();
        NCloud::NFastShard::WaitFor(ctx.Cancel, backoff);
        ctx.BackoffCycles += GetCycleCount() - parked;
    }
}

////////////////////////////////////////////////////////////////////////////////

inline NProto::TError MakeSpawnError(int r)
{
    return MakeError(
        E_FAIL,
        TStringBuilder()
            << "failed to spawn fiber: "
            << NCloud::NFastShard::SystemErrorText(r));
}

////////////////////////////////////////////////////////////////////////////////

NProto::TWriteLogRecordRequest MakeWriteLogRecordRequest(
    NProto::TDeviceRequestHeaders headers,
    const TVector<TPageGroup>& pageGroups,
    TLsnLink link);

NProto::TWriteLogRecordRequest MakeReplayRequest(
    NProto::TDeviceRequestHeaders headers,
    const NProto::TJournalRecord& record);

NProto::TReadPagesRequest MakeReadPagesRequest(
    NProto::TDeviceRequestHeaders headers,
    const TVector<TPageGroupRef>& pageGroupRefs,
    ui32 pageSize);

void ExtractPageGroups(
    const NProto::TReadPagesResponse& response,
    TVector<TPageGroup>* pageGroups);

TString DebugMessage(const NProto::TWriteLogRecordRequest& request);

////////////////////////////////////////////////////////////////////////////////

struct TAcquireDevicesParams
{
    TStorageDevice Device;
    NProto::TAcquireDevicesRequest* Request;
    NProto::TAcquireDevicesResponse* Response;
    const TStorageGroupRetryPolicy* RetryPolicy;
    ITimer* Timer;
    NCloud::NFastShard::TFastShardContext* Context;
};

int AcquireDevicesFiberMain(TAcquireDevicesParams* params) noexcept;

struct TReleaseDevicesParams
{
    TStorageDevice Device;
    NProto::TReleaseDevicesRequest* Request;
    NProto::TReleaseDevicesResponse* Response;
    const TStorageGroupRetryPolicy* RetryPolicy;
    ITimer* Timer;
    NCloud::NFastShard::TFastShardContext* Context;
};

int ReleaseDevicesFiberMain(TReleaseDevicesParams* params) noexcept;

////////////////////////////////////////////////////////////////////////////////

/**
 * Sends @p request to every device and waits for all of them - an n/n fan-out
 * with no early return. Returns the first error observed, or an empty error if
 * every device acked. The per-device responses go to @p responses if given.
 *
 * Everything the spawned fibers touch lives on this frame, which is safe
 * precisely because the call joins all of them before returning. A fan-out that
 * returns early cannot be written this way.
 */
template <typename TResponse, typename TRequest, typename TParams>
NProto::TError MirrorRequest(
    NCloud::NFastShard::TFastShardContext& ctx,
    const TStorageGroupConfig& config,
    const TVector<TStorageDevice>& devices,
    ITimer& timer,
    int (*fiberMain)(TParams*) noexcept,
    TRequest request,
    TVector<TResponse>* responses = nullptr)
{
    FillHeaders(config, request.MutableHeaders());

    const ui32 count = devices.size();
    TVector<silk::FiberFuture> futures(count);
    TVector<TResponse> ownResponses;
    if (!responses) {
        responses = &ownResponses;
    }
    responses->assign(count, {});

    for (ui32 i = 0; i < count; ++i) {
        const int r = silk::FiberScheduler::run(
            fiberMain,
            TParams{
                .Device = devices[i],
                .Request = &request,
                .Response = &(*responses)[i],
                .RetryPolicy = &config.RetryPolicy,
                .Timer = &timer,
                .Context = &ctx},
            &futures[i]);
        if (r) {
            *(*responses)[i].MutableError() = MakeSpawnError(r);
            futures[i].set(0);
        }
    }

    NProto::TError error;
    for (ui32 i = 0; i < count; ++i) {
        const int r = futures[i].wait();
        if (r) {
            SILK_ERROR(
                "future error: %s",
                NCloud::NFastShard::SystemErrorText(r).c_str());
            if (!HasError(error)) {
                error = MakeError(MAKE_SYSTEM_ERROR(r));
            }
            continue;
        }

        auto& response = (*responses)[i];
        if (HasError(response.GetError())) {
            SILK_ERROR(
                "node error: %s",
                FormatError(response.GetError()).c_str());
            if (!HasError(error)) {
                error = response.GetError();
            }
        }
    }

    return error;
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
