#pragma once

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>
#include <variant>

namespace NCloud::NFileStore::NStorage::NQuery {

////////////////////////////////////////////////////////////////////////////////

struct TColumnRef
{
    TString Name;
};

using TValue = std::variant<ui64, TString, TColumnRef>;

enum class EOperator
{
    Equal,
    NotEqual,
    Substr,
    In,
};

struct TPredicate
{
    TString Column;
    EOperator Operator = EOperator::Equal;
    TVector<TValue> Values;
};

enum class ELogicalOperator
{
    And,
    Or,
};

struct TExpression
{
    enum class EKind
    {
        Predicate,
        Logical,
    };

    EKind Kind = EKind::Predicate;
    TPredicate Predicate;
    ELogicalOperator Operator = ELogicalOperator::And;
    std::shared_ptr<TExpression> Left;
    std::shared_ptr<TExpression> Right;
};

struct TSelect
{
    TString Table;
    // Empty means SELECT *.
    TVector<TString> Columns;
    // Null when the query has no WHERE clause.
    std::shared_ptr<TExpression> Where;
    TMaybe<ui64> Limit;
};

struct TParseError
{
    size_t Offset = 0;
    TString Message;
};

TMaybe<TSelect> Parse(TStringBuf input, TParseError* error = nullptr);

}   // namespace NCloud::NFileStore::NStorage::NQuery
