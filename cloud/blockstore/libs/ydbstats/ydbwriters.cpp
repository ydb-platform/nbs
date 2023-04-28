#include "ydbwriters.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NYdbStats {

////////////////////////////////////////////////////////////////////////////////

TYdbReplaceWriter::TYdbReplaceWriter(
        TStringBuf tableName,
        TStringBuf itemName)
    : TableName(tableName)
    , ItemName(itemName)
{}

bool TYdbReplaceWriter::IsValid() const
{
    return !TableName.empty();
}

void TYdbReplaceWriter::Replace(TStringStream& out) const
{
    out << "REPLACE INTO `" << TableName << "` ";
    out << "SELECT * FROM AS_TABLE(" << ItemName << ");" << Endl;
}

////////////////////////////////////////////////////////////////////////////////

TYdbRowWriter::TYdbRowWriter(
        const TVector<TYdbRow>& data,
        TStringBuf tableName)
    : TYdbReplaceWriter(tableName, ItemName)
    , Data(data)
{}

bool TYdbRowWriter::IsValid() const
{
    return TYdbReplaceWriter::IsValid() && !Data.empty();
}

void TYdbRowWriter::Declare(TStringStream& out) const
{
    out << "DECLARE " << ItemName << " AS List<Struct<";
    out << TYdbRow::GetYdbRowDefinition() << ">>;" << Endl;
}

void TYdbRowWriter::PushData(NYdb::TParamsBuilder& out) const
{
    NYdb::TValueBuilder itemsAsList;
    itemsAsList.BeginList();
    for (const auto& r: Data) {
        itemsAsList.AddListItem(r.GetYdbValues());
    }
    itemsAsList.EndList();
    out.AddParam(ItemName.data(), itemsAsList.Build());
}

////////////////////////////////////////////////////////////////////////////////

TYdbBlobLoadMetricWriter::TYdbBlobLoadMetricWriter(
        const TVector<TYdbBlobLoadMetricRow>& data,
        TStringBuf tableName)
    : TYdbReplaceWriter(tableName, ItemName)
    , Data(data)
{}

bool TYdbBlobLoadMetricWriter::IsValid() const
{
    return TYdbReplaceWriter::IsValid() && !Data.empty();
}

void TYdbBlobLoadMetricWriter::Declare(TStringStream& out) const
{
    out << "DECLARE " << ItemName << " AS List<Struct<";
    out << TYdbBlobLoadMetricRow::GetYdbRowDefinition() << ">>;" << Endl;
}

void TYdbBlobLoadMetricWriter::PushData(NYdb::TParamsBuilder& out) const
{
    NYdb::TValueBuilder metricItemsAsList;
    metricItemsAsList.BeginList();
    for (const auto& r: Data) {
        metricItemsAsList.AddListItem(r.GetYdbValues());
    }
    metricItemsAsList.EndList();
    out.AddParam(ItemName.data(), metricItemsAsList.Build());
}

} // namespace NCloud::NBlockStore::NYdbStats
