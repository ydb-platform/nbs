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
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
    Substr,
    In,
};

struct TPredicateBase
{
    TString Column;
};

struct TEqualPredicate : TPredicateBase
{
    TValue Value;
};

struct TNotEqualPredicate : TPredicateBase
{
    TValue Value;
};

struct TGreaterPredicate : TPredicateBase
{
    TValue Value;
};

struct TGreaterOrEqualPredicate : TPredicateBase
{
    TValue Value;
};

struct TLessPredicate : TPredicateBase
{
    TValue Value;
};

struct TLessOrEqualPredicate : TPredicateBase
{
    TValue Value;
};

struct TSubstrPredicate : TPredicateBase
{
    TValue Value;
};

struct TInPredicate : TPredicateBase
{
    TVector<TValue> Values;
};

using TPredicate = std::variant<
    TEqualPredicate,
    TNotEqualPredicate,
    TGreaterPredicate,
    TGreaterOrEqualPredicate,
    TLessPredicate,
    TLessOrEqualPredicate,
    TSubstrPredicate,
    TInPredicate>;

enum class ELogicalOperator
{
    And,
    Or,
};

struct TExpression;

struct TLogicalExpression
{
    ELogicalOperator Operator = ELogicalOperator::And;
    std::shared_ptr<TExpression> Left;
    std::shared_ptr<TExpression> Right;
};

struct TExpression
{
    std::variant<TPredicate, TLogicalExpression> Node;
};

struct TSelect
{
    TString Table;
    // Empty means SELECT *.
    TVector<TString> Columns;
    std::optional<TExpression> Where;
    TMaybe<ui64> Limit;
};

struct TParseError
{
    size_t Offset = 0;
    TString Message;
};

TMaybe<TSelect> Parse(TStringBuf input, TParseError* error = nullptr);

}   // namespace NCloud::NFileStore::NStorage::NQuery
