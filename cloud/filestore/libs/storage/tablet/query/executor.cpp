#include "executor.h"

#include <util/generic/algorithm.h>
#include <util/generic/ylimits.h>
#include <util/string/builder.h>

#include <type_traits>

namespace NCloud::NFileStore::NStorage::NQuery {
namespace {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

bool IsSupportedNumericType(NScheme::TTypeId type)
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

    TResultOrError<TBoundSelect> Run()
    {
        if (Bound.Query.Columns.empty()) {
            ExpandStar();
        }

        for (const auto& name: Bound.Query.Columns) {
            const auto column = Resolve(name);
            if (HasError(column.GetError())) {
                return column.GetError();
            }
            Bound.OutputPositions.push_back(
                column.GetResult().IndexInReadColumnIds);
        }

        if (Bound.Query.Where && !Validate(*Bound.Query.Where)) {
            return Error;
        }

        return std::move(Bound);
    }

    void ExpandStar()
    {
        // Hash-map iteration order is unspecified, so sort by column ID.
        TVector<std::pair<ui32, TString>> columns;
        for (const auto& [id, column]: Table.Columns) {
            columns.emplace_back(id, column.Name);
        }
        Sort(columns);
        for (const auto& column: columns) {
            Bound.Query.Columns.push_back(column.second);
        }
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
                TStringBuilder() << "Unknown column '" << name << "' in table '"
                                 << Table.Name << "'");
        }

        const auto* column = Table.Columns.FindPtr(*id);
        if (!column) {
            return MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "Missing schema metadata for column '" << name
                    << "' (id " << *id << ") in table '" << Table.Name << "'");
        }

        const size_t position = Bound.ReadColumnIds.size();
        Bound.ReadColumnIds.push_back(*id);
        const TBoundColumn boundColumn{*id, column->PType, position};
        Bound.Columns.emplace(name, boundColumn);
        return boundColumn;
    }

    bool ValidateValue(NScheme::TTypeId type, const TValue& value)
    {
        if (!IsSupportedNumericType(type) && type != NScheme::NTypeIds::String)
        {
            return Fail(
                "WHERE predicates support only Uint32, Uint64 and String "
                "columns");
        }

        if (const auto* number = std::get_if<ui64>(&value)) {
            if (!IsSupportedNumericType(type)) {
                return Fail("Numeric operand requires a numeric column");
            }
            if (type == NScheme::NTypeIds::Uint32 && *number > Max<ui32>()) {
                return Fail("Numeric literal is out of range for Uint32");
            }
            return true;
        }

        if (std::holds_alternative<TString>(value)) {
            if (type != NScheme::NTypeIds::String) {
                return Fail("String operand requires a String column");
            }
            return true;
        }

        const auto other = Resolve(std::get<TColumnRef>(value).Name);
        if (HasError(other.GetError())) {
            Error = other.GetError();
            return false;
        }
        const auto otherType = other.GetResult().Type.GetTypeId();
        if (type != otherType && !(IsSupportedNumericType(type) &&
                                   IsSupportedNumericType(otherType)))
        {
            return Fail("Incompatible column types in predicate");
        }
        return true;
    }

    template <typename TPredicate>
    bool ValidatePredicate(const TPredicate& predicate)
    {
        const auto column = Resolve(predicate.Column);
        if (HasError(column.GetError())) {
            Error = column.GetError();
            return false;
        }
        const auto type = column.GetResult().Type.GetTypeId();
        if constexpr (std::is_same_v<TPredicate, TSubstrPredicate>) {
            if (type != NScheme::NTypeIds::String) {
                return Fail("SUBSTR requires a String column");
            }
        }
        return ValidateValue(type, predicate.Value);
    }

    bool ValidatePredicate(const TInPredicate& predicate)
    {
        const auto column = Resolve(predicate.Column);
        if (HasError(column.GetError())) {
            Error = column.GetError();
            return false;
        }
        const auto type = column.GetResult().Type.GetTypeId();
        if (predicate.Values.empty()) {
            return Fail("IN requires at least one value");
        }
        for (const auto& value: predicate.Values) {
            if (!ValidateValue(type, value)) {
                return false;
            }
        }
        return true;
    }

    bool ValidatePredicate(const TLogicalExpression& logical)
    {
        if (!logical.Left || !logical.Right) {
            return Fail("Logical expression requires two operands");
        }
        return Validate(*logical.Left) && Validate(*logical.Right);
    }

    bool ValidateNode(const TBinaryOperatorNode& binary)
    {
        return std::visit(
            [this](const auto& predicate)
            { return ValidatePredicate(predicate); },
            binary.Operator);
    }

    bool ValidateNode(const TMultiOperatorNode& multi)
    {
        return std::visit(
            [this](const auto& predicate)
            { return ValidatePredicate(predicate); },
            multi.Operator);
    }

    bool Validate(const TExpression& expression)
    {
        return std::visit(
            [this](const auto& node) { return ValidateNode(node); },
            expression.Node);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TBoundSelect> Bind(
    TSelect query,
    const NKikimr::NTable::TScheme& schema)
{
    const auto* tableId = schema.TableNames.FindPtr(query.Table);
    if (!tableId) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "Unknown table " << query.Table);
    }

    const auto* table = schema.Tables.FindPtr(*tableId);
    if (!table) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder() << "Missing schema metadata for table '"
                             << query.Table << "' (id " << *tableId << ")");
    }

    TBoundSelect bound;
    bound.Query = std::move(query);
    bound.TableId = *tableId;
    TBinder binder(*table, bound);

    return binder.Run();
}

}   // namespace NCloud::NFileStore::NStorage::NQuery
