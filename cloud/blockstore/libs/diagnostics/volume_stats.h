#pragma once

#include "public.h"

#include "downtime_history.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/storage/core/libs/common/error.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <span>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

using TVolumePerfStatus = std::pair<TString, ui32>;
using TVolumePerfStatuses = TVector<TVolumePerfStatus>;

////////////////////////////////////////////////////////////////////////////////

enum class EVolumeStatsType
{
    EServerStats,
    EClientStats
};

////////////////////////////////////////////////////////////////////////////////

struct IVolumeInfo
{
    virtual ~IVolumeInfo() = default;

    virtual const NProto::TVolume& GetInfo() const = 0;
    virtual TDuration GetPossiblePostponeDuration() const = 0;

    virtual ui64 RequestStarted(
        EBlockStoreRequest requestType,
        ui64 requestBytes) = 0;

    virtual TDuration RequestCompleted(
        EBlockStoreRequest requestType,
        ui64 requestStarted,
        TDuration postponedTime,
        ui64 requestBytes,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags,
        bool unaligned,
        ui64 responseSent) = 0;

    virtual void AddIncompleteStats(
        EBlockStoreRequest requestType,
        TRequestTime requestTime) = 0;

    virtual void AddRetryStats(
        EBlockStoreRequest requestType,
        EDiagnosticsErrorKind errorKind,
        ui32 errorFlags) = 0;

    virtual void RequestPostponed(EBlockStoreRequest requestType) = 0;
    virtual void RequestPostponedServer(EBlockStoreRequest requestType) = 0;
    virtual void RequestAdvanced(EBlockStoreRequest requestType) = 0;
    virtual void RequestAdvancedServer(EBlockStoreRequest requestType) = 0;
    virtual void RequestFastPathHit(EBlockStoreRequest requestType) = 0;

    using TTimeBucket = std::pair<TDuration, ui64>;
    using TSizeBucket = std::pair<ui64, ui64>;

    virtual void BatchCompleted(
        EBlockStoreRequest requestType,
        ui64 count,
        ui64 bytes,
        ui64 errors,
        std::span<TTimeBucket> timeHist,
        std::span<TSizeBucket> sizeHist) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IVolumeStats
{
    virtual ~IVolumeStats() = default;

    virtual bool MountVolume(
        const NProto::TVolume& volume,
        const TString& clientId,
        const TString& instanceId) = 0;

    virtual void UnmountVolume(
        const TString& diskId,
        const TString& clientId) = 0;

    virtual void AlterVolume(
        const TString& diskId,
        const TString& cloudId,
        const TString& folderId) = 0;

    virtual IVolumeInfoPtr GetVolumeInfo(
        const TString& diskId,
        const TString& clientId) const = 0;

    virtual NProto::EStorageMediaKind GetStorageMediaKind(
        const TString& diskId) const = 0;

    virtual ui32 GetBlockSize(const TString& diskId) const = 0;

    virtual void TrimVolumes() = 0;

    virtual void UpdateStats(bool updateIntervalFinished) = 0;

    virtual TVolumePerfStatuses GatherVolumePerfStatuses() = 0;

    virtual NCloud::NStorage::IUserMetricsSupplierPtr GetUserCounters() const
    {
        return nullptr;
    }

    virtual TDowntimeHistory GetDowntimeHistory(const TString& diskId) const = 0;

    virtual bool HasStorageConfigPatch(const TString& diskId) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

IVolumeStatsPtr CreateVolumeStats(
    IMonitoringServicePtr monitoring,
    TDiagnosticsConfigPtr diagnosticsConfig,
    TDuration inactiveClientsTimeout,
    EVolumeStatsType type,
    ITimerPtr timer);

IVolumeStatsPtr CreateVolumeStats(
    IMonitoringServicePtr monitoring,
    TDuration inactiveClientsTimeout,
    EVolumeStatsType type,
    ITimerPtr timer);

IVolumeStatsPtr CreateVolumeStatsStub();

}   // namespace NCloud::NBlockStore
