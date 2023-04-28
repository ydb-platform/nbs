#include "ydbscheme.h"
#include "ydbrow.h"

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

    Y_VERIFY(out.AddColumns(columns));
    return out;
}

}    // namespace

////////////////////////////////////////////////////////////////////////////////

TStatsTableSchemePtr CreateStatsTableScheme()
{
    auto out = BuildListOfColumns();
    Y_VERIFY(out.SetKeyColumns({"DiskId"}));
    return out.Finish();
}

TStatsTableSchemePtr CreateHistoryTableScheme()
{
    auto out = BuildListOfColumns();
    Y_VERIFY(out.SetKeyColumns({"DiskId", "Timestamp"}));
    return out.Finish();
}

TStatsTableSchemePtr CreateArchiveStatsTableScheme()
{
    auto out = BuildListOfColumns();
    Y_VERIFY(out.SetKeyColumns({"DiskId"}));
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

    Y_VERIFY(out.AddColumns(columns));

    Y_VERIFY(out.SetKeyColumns({
        TYdbBlobLoadMetricRow::HostNameName.data(),
        TYdbBlobLoadMetricRow::TimestampName.data()}));

    out.SetTtl(NTable::TTtlSettings{
        TYdbBlobLoadMetricRow::TimestampName.data(),
        NTable::TTtlSettings::EUnit::Seconds,
        TYdbBlobLoadMetricRow::TtlDuration });

    return out.Finish();
}

}   // namespace NCloud::NBlockStore::NYdbStats
