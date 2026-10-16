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

struct TPredicateBase
{
    TString Column;
};

struct TSingleValuePredicate : TPredicateBase
{
    TValue Value;
};

struct TEqualPredicate : TSingleValuePredicate {};
struct TNotEqualPredicate : TSingleValuePredicate {};
struct TGreaterPredicate : TSingleValuePredicate {};
struct TGreaterOrEqualPredicate : TSingleValuePredicate {};
struct TLessPredicate : TSingleValuePredicate {};
struct TLessOrEqualPredicate : TSingleValuePredicate {};
struct TSubstrPredicate : TSingleValuePredicate {};

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
    std::unique_ptr<TExpression> Left;
    std::unique_ptr<TExpression> Right;
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
    TMaybe<TExpression> Where;
    TMaybe<ui64> Limit;
};

struct TParseError
{
    size_t Offset = 0;
    TString Message;
};

TMaybe<TSelect> Parse(TStringBuf input, TParseError* error = nullptr);

}   // namespace NCloud::NFileStore::NStorage::NQuery
