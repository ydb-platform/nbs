#pragma once

#include "context.h"
#include "storage_group.h"

#include <cloud/storage/core/libs/common/error.h>

#include <silk/util/logger.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/system/types.h>

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

// Retries per the policy until the context's deadline or the stop flag.
template <typename TCall>
auto CallWithRetries(
    TFastShardContext& ctx,
    const TStorageGroupRetryPolicy& policy,
    TCall call)
{
    for (ui32 errorCount = 1;; ++errorCount) {
        auto response = call();
        const auto& error = response.GetError();
        if (GetErrorKind(error) != EErrorKind::ErrorRetriable) {
            return response;
        }

        const TDuration backoff = policy.BackoffIncrement * errorCount;
        if (backoff >= ctx.Deadline - ctx.Timer.Now()) {
            SILK_ERROR(
                "sg req %lu: out of time after %u errors: %s",
                ctx.RequestId,
                errorCount,
                FormatError(error).c_str());

            return response;
        }

        SILK_DEBUG(
            "sg req %lu: retry #%u, backoff: %luus, error: %s",
            ctx.RequestId,
            errorCount,
            backoff.MicroSeconds(),
            FormatError(error).c_str());

        const TInstant start = ctx.Timer.Now();
        ctx.Timer.Sleep(backoff, ctx.Stopped);
        ctx.AddBackoffTime(ctx.Timer.Now() - start);
        if (ctx.IsStopped()) {
            SILK_DEBUG(
                "sg req %lu: stopped: %s",
                ctx.RequestId,
                FormatError(error).c_str());

            return response;
        }
    }
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

}   // namespace NCloud::NFileStore::NStorage::NFastShard
