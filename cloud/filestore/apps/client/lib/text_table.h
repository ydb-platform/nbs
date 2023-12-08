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

template <typename TProtobufMessage> requires std::is_base_of_v<::google::protobuf::Message, TProtobufMessage>
TTextTable::TColumns ToColumns()
{
    auto firstNodeDescriptor = TProtobufMessage::GetDescriptor();
    Y_ENSURE(
        firstNodeDescriptor != nullptr,
        "Failed to acquire proto node descriptor"
    );

    const int fieldsCount = firstNodeDescriptor->field_count();

    TTextTable::TColumns columns;
    columns.reserve(fieldsCount);
    for (int i = 0; i < fieldsCount; ++i) {
        auto field = firstNodeDescriptor->field(i);

        columns.emplace_back(field->name(), field->name().size());
    }
    return columns;
}

[[maybe_unused]] TTextTable::TRow ToRow(const ::google::protobuf::Message& message);

}   // namespace NCloud::NFileStore::NClient::NTextTable
