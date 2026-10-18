#include "executor.h"

#include <util/generic/algorithm.h>
#include <util/generic/ylimits.h>
#include <util/string/builder.h>

#include <type_traits>

namespace NCloud::NFileStore::NStorage::NQuery {
namespace {

using namespace NKikimr;

bool IsNumber(NScheme::TTypeId type)
{
    return type == NScheme::NTypeIds::Uint32 ||
           type == NScheme::NTypeIds::Uint64;
}

class TBinder
{
private:
    const NTable::TScheme::TTableInfo& Table;
    TBoundSelect& Bound;
    NProto::TError Error;

public:
    TBinder(const NTable::TScheme::TTableInfo& table, TBoundSelect& bound)
        : Table(table)
        , Bound(bound)
    {}

    const NProto::TError& GetError() const
    {
        return Error;
    }

    bool Fail(TString message)
    {
        Error = MakeError(E_ARGUMENT, std::move(message));
        return false;
    }

    TResultOrError<TBoundColumn> Resolve(const TString& name)
    {
        if (const auto* boundColumn = Bound.Columns.FindPtr(name)) {
            return *boundColumn;
        }

        const auto* id = Table.ColumnNames.FindPtr(name);
        if (!id) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Unknown column " << Table.Name << "." << name);
        }

        const auto& column = Table.Columns.at(*id);
        const auto type = column.PType.GetTypeId();
        if (!IsNumber(type) && type != NScheme::NTypeIds::String) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Unsupported type for column " << Table.Name
                                 << "." << name);
        }

        const size_t position = Bound.ReadColumnIds.size();
        Bound.ReadColumnIds.push_back(*id);
        const TBoundColumn boundColumn{*id, column.PType, position};
        Bound.Columns.emplace(name, boundColumn);
        return boundColumn;
    }

    bool ValidateValue(NScheme::TTypeId type, const TValue& value)
    {
        if (const auto* number = std::get_if<ui64>(&value)) {
            if (!IsNumber(type)) {
                return Fail("Numeric operand requires a numeric column");
            }
            if (type == NScheme::NTypeIds::Uint32 && *number > Max<ui32>()) {
                return Fail("Numeric literal is out of range for Uint32");
            }
            return true;
        }

        if (std::holds_alternative<TString>(value)) {
            return type == NScheme::NTypeIds::String ||
                   Fail("String operand requires a String column");
        }

        const auto other = Resolve(std::get<TColumnRef>(value).Name);
        if (HasError(other.GetError())) {
            Error = other.GetError();
            return false;
        }
        const auto otherType = other.GetResult().Type.GetTypeId();
        return type == otherType || (IsNumber(type) && IsNumber(otherType)) ||
               Fail("Incompatible column types in predicate");
    }

    bool Validate(const TExpression& expression)
    {
        if (const auto* binary =
                std::get_if<TBinaryOperatorNode>(&expression.Node))
        {
            return std::visit(
                [this](const auto& predicate)
                {
                    const auto column = Resolve(predicate.Column);
                    if (HasError(column.GetError())) {
                        Error = column.GetError();
                        return false;
                    }
                    const auto type = column.GetResult().Type.GetTypeId();
                    if constexpr (std::is_same_v<
                                      std::decay_t<decltype(predicate)>,
                                      TSubstrPredicate>)
                    {
                        if (type != NScheme::NTypeIds::String) {
                            return Fail("SUBSTR requires a String column");
                        }
                    }
                    return ValidateValue(type, predicate.Value);
                }, binary->Operator);
        }

        const auto& multi = std::get<TMultiOperatorNode>(expression.Node);
        if (const auto* in = std::get_if<TInPredicate>(&multi.Operator)) {
            const auto column = Resolve(in->Column);
            if (HasError(column.GetError())) {
                Error = column.GetError();
                return false;
            }
            const auto type = column.GetResult().Type.GetTypeId();
            if (in->Values.empty()) {
                return Fail("IN requires at least one value");
            }
            for (const auto& value: in->Values) {
                if (!ValidateValue(type, value)) {
                    return false;
                }
            }
            return true;
        }

        const auto& logical = std::get<TLogicalExpression>(multi.Operator);
        if (!logical.Left || !logical.Right) {
            return Fail("Logical expression requires two operands");
        }
        return Validate(*logical.Left) && Validate(*logical.Right);
    }
};

}   // namespace

TResultOrError<TBoundSelect> Bind(
    TSelect query, const NKikimr::NTable::TScheme& schema)
{
    const auto* tableId = schema.TableNames.FindPtr(query.Table);
    if (!tableId) {
        return MakeError(
            E_ARGUMENT, TStringBuilder() << "Unknown table " << query.Table);
    }

    TBoundSelect bound;
    bound.Query = std::move(query);
    bound.TableId = *tableId;
    const auto& table = schema.Tables.at(*tableId);
    TBinder binder(table, bound);

    TVector<TString> outputColumns = bound.Query.Columns;
    if (outputColumns.empty()) {
        TVector<ui32> ids;
        for (const auto& [id, column]: table.Columns) {
            ids.push_back(id);
        }
        // Output order should not depend on map iteration order
        Sort(ids);
        for (const auto id: ids) {
            outputColumns.push_back(table.Columns.at(id).Name);
        }
    }

    for (const auto& name: outputColumns) {
        const auto column = binder.Resolve(name);
        if (HasError(column.GetError())) {
            return column.GetError();
        }
        bound.OutputPositions.push_back(column.GetResult().ReadPosition);
    }

    if (bound.Query.Where && !binder.Validate(*bound.Query.Where)) {
        return binder.GetError();
    }

    return std::move(bound);
}

}   // namespace NCloud::NFileStore::NStorage::NQuery
