#pragma once

#include <google/protobuf/message.h>

#include <util/generic/vector.h>

namespace NCloud::NFileStore::NClient::NTextTable {

struct TTextTable final
{
    struct TColumn
    {
        TString Name;
        ui64 MinWidth{0};

        TColumn() = default;
        explicit TColumn(TString name, ui64 minWidth = 0);

        void UpdateWidth(size_t value);
    };
    using TColumns = TVector<TColumn>;

    using TRow = TVector<TString>;
    using TRows = TVector<TRow>;

private:
    TColumns Columns;
    TRows Rows;

public:

    explicit TTextTable(TColumns columns, TRows rows = {});

    void AddRow(TRow row);

    const TRows& GetRows() const;
    const TColumns& GetColumns() const;
};

TTextTable::TColumns ToColumns(const TVector<TString>& columnNames);

}   // namespace NCloud::NFileStore::NClient::NTextTable
