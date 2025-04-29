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

TStatsTableSchemePtr CreateGroupsTableScheme()
{
    using namespace NYdb;

    TStatsTableSchemeBuilder out;

    static const TVector<std::pair<TString, EPrimitiveType>> columns = {
        {TYdbGroupsRow::TabletIdName.data(), EPrimitiveType::Uint64},
        {TYdbGroupsRow::ChannelName.data(), EPrimitiveType::Uint32},
        {TYdbGroupsRow::GroupIdName.data(), EPrimitiveType::Uint32},
        {TYdbGroupsRow::GenerationName.data(), EPrimitiveType::Uint32},
        {TYdbGroupsRow::TimestampName.data(), EPrimitiveType::Uint64}};

    STORAGE_VERIFY_C(
        out.AddColumns(columns),
        TWellKnownEntityTypes::YDB_TABLE,
        "groups table",
        "unable to setup fields");

    STORAGE_VERIFY_C(
        out.SetKeyColumns({
            TYdbGroupsRow::TabletIdName.data(),
            TYdbGroupsRow::ChannelName.data(),
            TYdbGroupsRow::GroupIdName.data(),
            TYdbGroupsRow::GenerationName.data()}),
        TWellKnownEntityTypes::YDB_TABLE,
        "groups table",
        "unable to setup key fields");

    out.SetTtl(NTable::TTtlSettings{
        TYdbGroupsRow::TimestampName.data(),
        NTable::TTtlSettings::EUnit::Seconds,
        TYdbGroupsRow::TtlDuration});

    return out.Finish();
}

TStatsTableSchemePtr CreatePartitionsTableScheme()
{
    using namespace NYdb;

    TStatsTableSchemeBuilder out;

    static const TVector<std::pair<TString, EPrimitiveType>> columns = {
        {TYdbPartitionsRow::PartitionTabletIdName.data(), EPrimitiveType::Uint64},
        {TYdbPartitionsRow::VolumeTabletIdName.data(), EPrimitiveType::Uint64},
        {TYdbPartitionsRow::DiskIdName.data(), EPrimitiveType::String},
        {TYdbPartitionsRow::TimestampName.data(), EPrimitiveType::Uint64}};

    STORAGE_VERIFY_C(
        out.AddColumns(columns),
        TWellKnownEntityTypes::YDB_TABLE,
        "partitions table",
        "unable to setup fields");

    STORAGE_VERIFY_C(
        out.SetKeyColumns({TYdbPartitionsRow::PartitionTabletIdName.data()}),
        TWellKnownEntityTypes::YDB_TABLE,
        "partitions table",
        "unable to setup key fields");

    out.SetTtl(NTable::TTtlSettings{
        TYdbPartitionsRow::TimestampName.data(),
        NTable::TTtlSettings::EUnit::Seconds,
        TYdbPartitionsRow::TtlDuration});

    return out.Finish();
}

}   // namespace NCloud::NBlockStore::NYdbStats
