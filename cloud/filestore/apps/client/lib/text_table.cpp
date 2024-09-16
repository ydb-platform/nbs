#include "text_table.h"

#include <google/protobuf/text_format.h>

#include <util/stream/format.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>

namespace NCloud::NFileStore::NClient::NTextTable {

namespace {

////////////////////////////////////////////////////////////////////////////////

size_t CalcColumnWidth(size_t value)
{
    return value > 0 ? value + 1 : 0;
}

void PrintColumns(IOutputStream& out, const TTextTable& table)
{
    const auto& columns = table.GetColumns();

    for (size_t i = 0; i < table.GetColumns().size() - 1; ++i) {
        const auto& column = columns[i];
        if (!column.Name.empty()) {
            out << RightPad(column.Name, CalcColumnWidth(column.MinWidth));
        }
    }
    const auto& lastColumn = columns.back();
    if (!lastColumn.Name.empty()) {
        out << lastColumn.Name << Endl;
    }
}

void PrintRows(IOutputStream& out, const TTextTable& table)
{
    auto printRow = [&out, &columns = table.GetColumns()] (
            const TTextTable::TRow& row) {
        for (size_t i = 0; i < columns.size() - 1; ++i) {
            const auto& column = columns[i];
            out << RightPad(row[i], CalcColumnWidth(column.MinWidth));
        }
        out << row.back();
    };
    const auto& rows = table.GetRows();
    if (rows.empty()) {
        return;
    }

    for (size_t i = 0; i < rows.size() - 1; ++i) {
        printRow(rows[i]);
        out << Endl;
    }
    printRow(rows.back());
}

}    // namespace

////////////////////////////////////////////////////////////////////////////////

TTextTable::TColumn::TColumn(TString name, ui64 minWidth)
    : Name(std::move(name))
    , MinWidth(Min(minWidth, Name.size()))
{
}

void TTextTable::TColumn::UpdateWidth(size_t value)
{
    if (MinWidth < value) {
        MinWidth = value;
    }
}

TTextTable::TTextTable(TTextTable::TColumns columns, TTextTable::TRows rows)
    : Columns(std::move(columns))
    , Rows(std::move(rows))
{
}

void TTextTable::AddRow(TTextTable::TRow row)
{
    Y_ENSURE(
        Columns.empty() || Columns.size() == row.size(),
        "Failed to add row: column's size is not matching row's size"
    );

    if (Columns.empty()) {
        Columns.resize(row.size());
    }

    for (size_t i = 0; i < Columns.size(); ++i) {
        Columns[i].UpdateWidth(row[i].size());
    }
    Rows.push_back(std::move(row));
}

const TTextTable::TRows& TTextTable::GetRows() const
{
    return Rows;
}

const TTextTable::TColumns& TTextTable::GetColumns() const
{
    return Columns;
}

TTextTable::TColumns ToColumns(const TVector<TString>& columnNames)
{
    TTextTable::TColumns columns;
    columns.reserve(columnNames.size());
    for (auto field: columnNames) {
        columns.emplace_back(field, field.size());
    }
    return columns;
}

}   // namespace NCloud::NFileStore::NClient::NTextTable

////////////////////////////////////////////////////////////////////////////////

template<>
void Out<NCloud::NFileStore::NClient::NTextTable::TTextTable>(
    IOutputStream& out,
    const NCloud::NFileStore::NClient::NTextTable::TTextTable& table)
{
    using namespace NCloud::NFileStore::NClient::NTextTable;

    const auto& rows = table.GetRows();
    Y_ENSURE(
        rows.empty() ||
            table.GetColumns().size() == table.GetRows().front().size(),
        "Failed to print, column's size is not matching row's size"
    );

    PrintColumns(out, table);
    PrintRows(out, table);
}
