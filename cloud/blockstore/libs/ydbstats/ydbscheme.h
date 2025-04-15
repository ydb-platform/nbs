#pragma once

#include "public.h"

#include <contrib/ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/variant.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>

namespace NCloud::NBlockStore::NYdbStats {

////////////////////////////////////////////////////////////////////////////////

struct TStatsTableScheme
{
    const TVector<NYdb::TColumn> Columns;
    const TVector<TString> KeyColumns;
    const TMaybe<NYdb::NTable::TTtlSettings> Ttl;

    TStatsTableScheme() = default;

    TStatsTableScheme(
            TVector<NYdb::TColumn> columns,
            TVector<TString> keyColumns,
            TMaybe<NYdb::NTable::TTtlSettings> ttl)
        : Columns(std::move(columns))
        , KeyColumns(std::move(keyColumns))
        , Ttl(std::move(ttl))
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TStatsTableSchemeBuilder
{
private:
    THashSet<TString> Names;
    TVector<NYdb::TColumn> Columns;
    TVector<TString> KeyColumns;
    TMaybe<NYdb::NTable::TTtlSettings> Ttl;

public:
    bool AddColumns(
        const TVector<std::pair<TString, NYdb::EPrimitiveType>>& columns)
    {
        for (const auto& column: columns) {
            bool inserted;
            std::tie(std::ignore, inserted) = Names.emplace(column.first);
            if (!inserted) {
                return false;
            }

            Columns.emplace_back(
                column.first,
                NYdb::TTypeBuilder().Primitive(column.second).Build());
        }

        return true;
    }

    const TVector<NYdb::TColumn>& GetColumns() const
    {
        return Columns;
    }

    bool SetKeyColumns(TVector<TString> columns)
    {
        for (const auto& column: columns) {
            if (Names.find(column) == Names.end()) {
                return false;
            }
        }

        KeyColumns = std::move(columns);
        return true;
    }

    const TVector<TString>& GetKeyColumns() const
    {
        return KeyColumns;
    }

    void SetTtl(NYdb::NTable::TTtlSettings ttl)
    {
        Ttl = std::move(ttl);
    }

    const auto& GetTtl() const
    {
        return Ttl;
    }

    TStatsTableSchemePtr Finish()
    {
        return std::make_shared<TStatsTableScheme>(
            std::move(Columns),
            std::move(KeyColumns),
            std::move(Ttl));
    }
};

////////////////////////////////////////////////////////////////////////////////

TStatsTableSchemePtr CreateStatsTableScheme(TDuration ttl);
TStatsTableSchemePtr CreateHistoryTableScheme();
TStatsTableSchemePtr CreateArchiveStatsTableScheme(TDuration ttl);
TStatsTableSchemePtr CreateBlobLoadMetricsTableScheme();
TStatsTableSchemePtr CreateGroupsTableScheme();
TStatsTableSchemePtr CreatePartitionsTableScheme();

}   // namespace NCloud::NBlockStore::NYdbStats
