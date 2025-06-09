#pragma once

#include "public.h"

#include <cloud/blockstore/config/ydbstats.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore::NYdbStats {

////////////////////////////////////////////////////////////////////////////////

class TYdbStatsConfig
{
private:
    NProto::TYdbStatsConfig YdbStatsConfig;

public:
    TYdbStatsConfig(NProto::TYdbStatsConfig statsUploadConfig = {});

    bool IsValid() const;

    TString GetStatsTableName() const;
    TString GetArchiveStatsTableName() const;
    TString GetBlobLoadMetricsTableName() const;
    TString GetHistoryTablePrefix() const;
    TString GetGroupsTableName() const;
    TString GetPartitionsTableName() const;
    TString GetDatabaseName() const;
    TString GetTokenFile() const;
    TString GetServerAddress() const;
    ui32 GetHistoryTableLifetimeDays() const;
    ui32 GetStatsTableRotationAfterDays() const;
    bool GetUseSsl() const;
    TDuration GetStatsTableTtl() const;
    TDuration GetArchiveStatsTableTtl() const;
};

}   // namespace NCloud::NBlockStore::NYdbStats
