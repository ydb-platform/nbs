#include "ydbscheme.h"

#include "config.h"
#include "ydbrow.h"

#include <cloud/storage/core/libs/common/verify.h>

namespace NCloud::NBlockStore::NYdbStats {

using namespace NYdb;

namespace {

////////////////////////////////////////////////////////////////////////////////

TStatsTableSchemeBuilder BuildListOfColumns()
{
    TStatsTableSchemeBuilder out;

    static TVector<std::pair<TString, NYdb::EPrimitiveType>> columns = {
#define YDB_SIMPLE_STRING_FIELD(name , ...)                                    \
        { #name, NYdb::EPrimitiveType::String },                               \

    YDB_SIMPLE_STRING_COUNTERS(YDB_SIMPLE_STRING_FIELD)

#undef YDB_SIMPLE_STRING_FIELD

#define YDB_SIMPLE_UINT64_FIELD(name , ...)                                    \
        { #name,  NYdb::EPrimitiveType::Uint64 },                              \
// YDB_SIMPLE_UINT64_LABEL

    YDB_SIMPLE_UINT64_COUNTERS(YDB_SIMPLE_UINT64_FIELD)

#undef YDB_SIMPLE_UINT64_FIELD

#define YDB_CUMULATIVE_FIELD(name, ...)                                        \
        { #name ,        NYdb::EPrimitiveType::Uint64 },                       \
// YDB_CUMULATIVE_LABEL

    YDB_CUMULATIVE_COUNTERS(YDB_DEFINE_CUMULATIVE_COUNTER_STRING)

#undef YDB_CUMULATIVE_FIELD

#define YDB_PERCENTILE_FIELD(name, ...)                                        \
        {#name ,    NYdb::EPrimitiveType::Double },                            \
// YDB_PERCENTILE_LABEL

    YDB_PERCENTILE_COUNTERS(YDB_DEFINE_PERCENTILES_COUNTER_STRING)

#undef YDB_PERCENTILE_FIELD
    };

    STORAGE_VERIFY(
        out.AddColumns(columns),
        TWellKnownEntityTypes::YDB_TABLE,
        "unable to set up table fields");
    return out;
}

}    // namespace

////////////////////////////////////////////////////////////////////////////////

TStatsTableSchemePtr CreateStatsTableScheme(TDuration ttl)
{
    auto out = BuildListOfColumns();
    STORAGE_VERIFY_C(
        out.SetKeyColumns({"DiskId"}),
        TWellKnownEntityTypes::YDB_TABLE,
        "stats table",
        "unable to set key fields");
    if (ttl) {
        out.SetTtl(NTable::TTtlSettings{
            "Timestamp",
            NTable::TTtlSettings::EUnit::MilliSeconds,
            ttl});
    }
    return out.Finish();
}

TStatsTableSchemePtr CreateHistoryTableScheme()
{
    auto out = BuildListOfColumns();
    STORAGE_VERIFY_C(
        out.SetKeyColumns({"DiskId", "Timestamp"}),
        TWellKnownEntityTypes::YDB_TABLE,
        "history table",
        "unable to setup key fields");
    return out.Finish();
}

TStatsTableSchemePtr CreateArchiveStatsTableScheme(TDuration ttl)
{
    auto out = BuildListOfColumns();
    STORAGE_VERIFY_C(
        out.SetKeyColumns({"DiskId"}),
        TWellKnownEntityTypes::YDB_TABLE,
        "archive table",
        "unable to setup key fields");
    if (ttl) {
        out.SetTtl(NTable::TTtlSettings{
            "Timestamp",
            NTable::TTtlSettings::EUnit::MilliSeconds,
            ttl});
    }
    return out.Finish();
}

TStatsTableSchemePtr CreateBlobLoadMetricsTableScheme()
{
    using namespace NYdb;

    TStatsTableSchemeBuilder out;

    static const TVector<std::pair<TString, EPrimitiveType>> columns = {
        { TYdbBlobLoadMetricRow::HostNameName.data(),  EPrimitiveType::String },
        { TYdbBlobLoadMetricRow::TimestampName.data(), EPrimitiveType::Uint64 },
        { TYdbBlobLoadMetricRow::LoadDataName.data(),  EPrimitiveType::Json }
    };

    STORAGE_VERIFY_C(
        out.AddColumns(columns),
        TWellKnownEntityTypes::YDB_TABLE,
        "blob load metrics table",
        "unable to setup fields");

    STORAGE_VERIFY_C(
        out.SetKeyColumns({
            TYdbBlobLoadMetricRow::HostNameName.data(),
            TYdbBlobLoadMetricRow::TimestampName.data()}),
        TWellKnownEntityTypes::YDB_TABLE,
        "blob load metrics table",
        "unable to setup key fields");

    out.SetTtl(NTable::TTtlSettings{
        TYdbBlobLoadMetricRow::TimestampName.data(),
        NTable::TTtlSettings::EUnit::Seconds,
        TYdbBlobLoadMetricRow::TtlDuration });

    return out.Finish();
}

}   // namespace NCloud::NBlockStore::NYdbStats
