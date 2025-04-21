#include "ydbrow.h"

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NYdbStats {

using namespace NYdb;

////////////////////////////////////////////////////////////////////////////////

TValue TYdbRow::GetYdbValues() const
{
    NYdb::TValueBuilder value;
    value.BeginStruct();

#define YDB_SET_SIMPLE_STRING_FIELD(name, ...)                                 \
    value.AddMember(#name).String(name);                                       \

    YDB_SIMPLE_STRING_COUNTERS(YDB_SET_SIMPLE_STRING_FIELD)

#undef YDB_SET_SIMPLE_STRING_FIELD

#define YDB_SET_SIMPLE_UINT64_FIELD(name , ...)                                \
    value.AddMember(#name).Uint64(name);                                       \

    YDB_SIMPLE_UINT64_COUNTERS(YDB_SET_SIMPLE_UINT64_FIELD)

#undef YDB_SET_SIMPLE_UINT64_FIELD

#define YDB_SET_CUMULATIVE_FIELD(str , name , ...)                             \
    value.AddMember(#str).Uint64(name);                                        \

    YDB_CUMULATIVE_COUNTERS(YDB_SET_CUMULATIVE_COUNTER)
#undef YDB_SET_CUMULATIVE_FIELD

#define YDB_SET_PERCENTILE_FIELD(str, name,  ...)                              \
    value.AddMember(#str).Double(name);                                        \

    YDB_PERCENTILE_COUNTERS(YDB_SET_PERCENTILES_COUNTER)
#undef YDB_SET_PERCENTILE_FIELD

    value.EndStruct();

    return value.Build();
}

TStringBuf TYdbRow::GetYdbRowDefinition()
{
    static TStringBuf RowDefinition =
#define YDB_SIMPLE_STRING_FIELD(name , ...)                                    \
    "'" #name "'" ": String,"                                                  \

        YDB_SIMPLE_STRING_COUNTERS(YDB_SIMPLE_STRING_FIELD)

#undef YDB_SIMPLE_STRING_FIELD

#define YDB_SIMPLE_UINT64_FIELD(name , ...)                                    \
    "'" #name "'" ": Uint64,"                                                  \

        YDB_SIMPLE_UINT64_COUNTERS(YDB_SIMPLE_UINT64_FIELD)

#undef YDB_SIMPLE_UINT64_FIELD

#define YDB_CUMULATIVE_FIELD(name , ...)                                       \
    "'" #name "'" ": Uint64,"                                                  \

        YDB_CUMULATIVE_COUNTERS(YDB_DEFINE_CUMULATIVE_COUNTER_STRING)
#undef YDB_CUMULATIVE_FIELD

#define YDB_PERCENTILE_FIELD(name,  ...)                                       \
    "'" #name "'" ": Double,"                                                  \
// YDB_PERCENTILE_LABEL

        YDB_PERCENTILE_COUNTERS(YDB_DEFINE_PERCENTILES_COUNTER_STRING);
#undef YDB_PERCENTILE_FIELD

    return TStringBuf(RowDefinition, 0, RowDefinition.length()-1);
}


TValue TYdbBlobLoadMetricRow::GetYdbValues() const
{
    NYdb::TValueBuilder value;
    value.BeginStruct();

    value.AddMember(HostNameName.data()).String(HostName);
    value.AddMember(TimestampName.data()).Uint64(Timestamp.Seconds());
    value.AddMember(LoadDataName.data()).Json(LoadData);

    value.EndStruct();
    return value.Build();
}


TValue TYdbGroupsInfoRow::GetYdbValues() const
{
    NYdb::TValueBuilder value;
    value.BeginStruct();

    value.AddMember(PartitionTabletIdName.data()).Uint64(PartitionTabletId);
    value.AddMember(ChannelName.data()).Uint32(Channel);
    value.AddMember(GroupIdName.data()).Uint32(GroupId);
    value.AddMember(GenerationName.data()).Uint32(Generation);
    value.AddMember(TimestampName.data()).Uint64(Timestamp.Seconds());

    value.EndStruct();
    return value.Build();
}


TValue TYdbPartitionsRow::GetYdbValues() const
{
    NYdb::TValueBuilder value;
    value.BeginStruct();

    value.AddMember(DiskIdName.data()).String(DiskId);
    value.AddMember(VolumeTabletIdName.data()).Uint64(VolumeTabletId);
    value.AddMember(PartitionTabletIdName.data()).Uint64(PartitionTabletId);
    value.AddMember(TimestampName.data()).Uint64(Timestamp.Seconds());

    value.EndStruct();
    return value.Build();
}


TStringBuf TYdbBlobLoadMetricRow::GetYdbRowDefinition()
{
    static const TString RowDefinition =
        "'" + TString(HostNameName)  + "': String," +
        "'" + TString(TimestampName) + "': Uint64," +
        "'" + TString(LoadDataName)  + "': Json";

    return TStringBuf(RowDefinition.data());
}


}   // namespace NCloud::NBlockStore::NYdbStats
