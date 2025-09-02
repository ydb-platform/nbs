#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

namespace NCloud::NBlockStore::NYdbStats {

namespace {

////////////////////////////////////////////////////////////////////////////////

TDuration Seconds(ui32 value)
{
    return TDuration::Seconds(value);
}

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_YDBSTATS_CONFIG(xxx)                                        \
    xxx(StatsTableName,                   TString,          ""                )\
    xxx(HistoryTablePrefix,               TString,          ""                )\
    xxx(DatabaseName,                     TString,          ""                )\
    xxx(TokenFile,                        TString,          ""                )\
    xxx(ServerAddress,                    TString,          ""                )\
    xxx(HistoryTableLifetimeDays,         ui32,             3                 )\
    xxx(StatsTableRotationAfterDays,      ui32,             1                 )\
    xxx(ArchiveStatsTableName,            TString,          ""                )\
    xxx(BlobLoadMetricsTableName,         TString,          ""                )\
    xxx(GroupsTableName,                  TString,          ""                )\
    xxx(PartitionsTableName,              TString,          ""                )\
    xxx(UseSsl,                           bool,             false             )\
    xxx(StatsTableTtl,                    TDuration,        Seconds(0)        )\
    xxx(ArchiveStatsTableTtl,             TDuration,        Seconds(0)        )\
                                                                               \
    xxx(IamTokenRefreshTimeBeforeExpiration, TDuration,     Seconds(5)        )\

// BLOCKSTORE_YDBSTATS_CONFIG

#define BLOCKSTORE_YDBSTATS_DECLARE_CONFIG(name, type, value)                  \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// BLOCKSTORE_YDBSTATS_DECLARE_CONFIG

BLOCKSTORE_YDBSTATS_CONFIG(BLOCKSTORE_YDBSTATS_DECLARE_CONFIG)

#undef BLOCKSTORE_YDBSTATS_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

template <typename TTarget, typename TSource>
TTarget ConvertValue(TSource value)
{
    return static_cast<TTarget>(std::move(value));
}

template <>
TDuration ConvertValue<TDuration, ui32>(ui32 value)
{
    return TDuration::MilliSeconds(value);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TYdbStatsConfig::TYdbStatsConfig(NProto::TYdbStatsConfig ydbStatsConfig)
    : YdbStatsConfig(std::move(ydbStatsConfig))
{}

bool TYdbStatsConfig::IsValid() const
{
    return
        YdbStatsConfig.GetStatsTableName() &&
        YdbStatsConfig.GetHistoryTablePrefix() &&
        YdbStatsConfig.GetDatabaseName() &&
        YdbStatsConfig.GetServerAddress();
}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TYdbStatsConfig::Get##name() const                                        \
{                                                                              \
    auto value = YdbStatsConfig.Get##name();                                   \
    return value ? ConvertValue<type>(value) : Default##name;                  \
}                                                                              \
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_YDBSTATS_CONFIG(BLOCKSTORE_CONFIG_GETTER);

#undef BLOCKSTORE_CONFIG_GETTER

}   // namespace NCloud::NBlockStore::NYdbStats
