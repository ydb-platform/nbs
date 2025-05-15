#pragma once

#include "public.h"
#include "ydbrow.h"
#include "ydbscheme.h"

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
        const TYdbRowData& rows) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IYdbVolumesStatsUploaderPtr CreateYdbVolumesStatsUploader(
    TYdbStatsConfigPtr config,
    ILoggingServicePtr logging,
    IYdbStoragePtr dbStorage,
    TYDBTableSchemes tableSchemes);

IYdbVolumesStatsUploaderPtr CreateVolumesStatsUploaderStub();

}   // namespace NCloud::NBlockStore::NYdbStats

////////////////////////////////////////////////////////////////////////////////

struct TYDBTableNames
{
    TString Stats;
    TString History;
    TString Archive;
    TString Metrics;
    TString Groups;
    TString Partitions;

    TYDBTableNames() = default;
};
