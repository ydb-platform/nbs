#pragma once

#include "storage_group.h"

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/logger.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

inline void FillHeaders(
    const TStorageGroupConfig& config,
    NProto::TDeviceRequestHeaders* headers)
{
    headers->SetClientId(config.ClientId);
}

////////////////////////////////////////////////////////////////////////////////

/**
 * Repeats the call until it succeeds, fails with a non-retriable error or the
 * total timeout is reached. All time arithmetic goes through the provided
 * timer. See TStorageGroupRetryPolicy.
*/
template <typename TCall>
auto CallWithRetries(
    const TStorageGroupRetryPolicy& policy,
    ITimer& timer,
    TCall call)
{
    const TInstant start = timer.Now();
    ui32 errorCount = 0;

    for (;;) {
        auto response = call();
        const auto& error = response.GetError();
        if (GetErrorKind(error) != EErrorKind::ErrorRetriable) {
            return response;
        }

        if (timer.Now() - start >= policy.TotalTimeout) {
            SILK_ERROR(
                "sg retries timed out after %u errors: %s",
                errorCount + 1,
                FormatError(error).c_str());

            return response;
        }

        ++errorCount;
        const TDuration backoff = policy.BackoffIncrement * errorCount;
        SILK_DEBUG(
            "sg retry #%u, backoff: %luus, error: %s",
            errorCount,
            backoff.MicroSeconds(),
            FormatError(error).c_str());

        timer.Sleep(backoff);
    }
}

////////////////////////////////////////////////////////////////////////////////

NProto::TWriteLogRecordRequest MakeWriteLogRecordRequest(
    NProto::TDeviceRequestHeaders headers,
    const TVector<TPageGroup>& pageGroups,
    ui64 lsn);

NProto::TWriteLogRecordRequest MakeReplayRequest(
    NProto::TDeviceRequestHeaders headers,
    const NProto::TJournalRecord& record);

NProto::TReadPagesRequest MakeReadPagesRequest(
    NProto::TDeviceRequestHeaders headers,
    const TVector<TPageGroupRef>& pageGroupRefs);

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
};

int AcquireDevicesFiberMain(TAcquireDevicesParams* params) noexcept;

struct TReleaseDevicesParams
{
    TStorageDevice Device;
    NProto::TReleaseDevicesRequest* Request;
    NProto::TReleaseDevicesResponse* Response;
    const TStorageGroupRetryPolicy* RetryPolicy;
    ITimer* Timer;
};

int ReleaseDevicesFiberMain(TReleaseDevicesParams* params) noexcept;

////////////////////////////////////////////////////////////////////////////////

/**
 * Sends @p request to every device and waits for all of them - an n/n fan-out
 * with no early return. Returns the first error observed, or an empty error if
 * every device acked.
 *
 * Everything the spawned fibers touch lives on this frame, which is safe
 * precisely because the call joins all of them before returning. A fan-out that
 * returns early cannot be written this way.
 */
template <typename TResponse, typename TRequest, typename TParams>
NProto::TError MirrorRequest(
    const TStorageGroupConfig& config,
    const TVector<TStorageDevice>& devices,
    ITimer& timer,
    int (*fiberMain)(TParams*) noexcept,
    TRequest request)
{
    FillHeaders(config, request.MutableHeaders());

    const ui32 count = devices.size();
    TVector<silk::FiberFuture> futures(count);
    TVector<TResponse> responses(count);

    for (ui32 i = 0; i < count; ++i) {
        const int r = silk::FiberScheduler::run(
            fiberMain,
            TParams{
                .Device = devices[i],
                .Request = &request,
                .Response = &responses[i],
                .RetryPolicy = &config.RetryPolicy,
                .Timer = &timer},
            &futures[i]);
        Y_ABORT_UNLESS(r == 0, "failed to spawn fiber: %s", ::strerror(r));
    }

    NProto::TError error;
    for (ui32 i = 0; i < count; ++i) {
        const int r = futures[i].wait();
        if (r) {
            SILK_ERROR("future error: %s", ::strerror(r));
            if (!HasError(error)) {
                error = MakeError(MAKE_SYSTEM_ERROR(r));
            }
            continue;
        }

        auto& response = responses[i];
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
