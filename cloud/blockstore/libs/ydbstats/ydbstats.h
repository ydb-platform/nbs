#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/iam/iface/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/generic/variant.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NYdbStats {

////////////////////////////////////////////////////////////////////////////////

struct TYDBTableSchemes
{
    TStatsTableSchemePtr Stats;
    TStatsTableSchemePtr History;
    TStatsTableSchemePtr Archive;
    TStatsTableSchemePtr Metrics;
    TStatsTableSchemePtr Groups;
    TStatsTableSchemePtr Partitions;

    TYDBTableSchemes(
        TStatsTableSchemePtr stats,
        TStatsTableSchemePtr history,
        TStatsTableSchemePtr archive,
        TStatsTableSchemePtr metrics,
        TStatsTableSchemePtr groups,
        TStatsTableSchemePtr partitions);
    TYDBTableSchemes(TDuration statsTtl, TDuration archiveTtl);
    ~TYDBTableSchemes();
};

struct IYdbVolumesStatsUploader
    : public IStartable
{
    virtual ~IYdbVolumesStatsUploader() = default;

    virtual NThreading::TFuture<NProto::TError> UploadStats(
        const TYdbRowData& rows) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IYdbStoragePtr CreateYdbStorage(
    TYdbStatsConfigPtr config,
    ILoggingServicePtr logging,
    NIamClient::IIamTokenClientPtr tokenProvider);

IStartable* AsStartable(IYdbStoragePtr storagePtr);

IYdbVolumesStatsUploaderPtr CreateYdbVolumesStatsUploader(
    TYdbStatsConfigPtr config,
    ILoggingServicePtr logging,
    IYdbStoragePtr dbStorage,
    TYDBTableSchemes tableSchemes);

IYdbVolumesStatsUploaderPtr CreateVolumesStatsUploaderStub();

}   // namespace NCloud::NBlockStore::NYdbStats

////////////////////////////////////////////////////////////////////////////////
