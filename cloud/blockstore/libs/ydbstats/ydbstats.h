#pragma once

#include "public.h"
#include "ydbrow.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/generic/variant.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NYdbStats {

////////////////////////////////////////////////////////////////////////////////

struct IYdbVolumesStatsUploader
    : public IStartable
{
    virtual ~IYdbVolumesStatsUploader() = default;

    virtual NThreading::TFuture<NProto::TError> UploadStats(
        const TVector<TYdbRow>& stats,
        const TVector<TYdbBlobLoadMetricRow>& metrics,
        const TVector<TYdbGroupsInfoRow>& groups,
        const TVector<TYdbPartitionsRow>& partitions) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IYdbVolumesStatsUploaderPtr CreateYdbVolumesStatsUploader(
    TYdbStatsConfigPtr config,
    ILoggingServicePtr logging,
    IYdbStoragePtr dbStorage,
    TStatsTableSchemePtr statsTableScheme,
    TStatsTableSchemePtr historyTableScheme,
    TStatsTableSchemePtr archiveStatsTableScheme,
    TStatsTableSchemePtr metricsTableScheme,
    TStatsTableSchemePtr groupsTableScheme,
    TStatsTableSchemePtr partitionsTableScheme);

IYdbVolumesStatsUploaderPtr CreateVolumesStatsUploaderStub();

}   // namespace NCloud::NBlockStore::NYdbStats
