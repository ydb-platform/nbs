#pragma once

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <variant>

namespace NCloud::NFileStore::NStorage::NQuery {

////////////////////////////////////////////////////////////////////////////////

using TValue = std::variant<ui64, TString>;

enum class EPredicate
{
    Equal,
    In,
};

struct TCondition
{
    TString Column;
    EPredicate Predicate = EPredicate::Equal;
    TVector<TValue> Values;
};

struct TSelect
{
    TString Table;
    // Empty means SELECT *.
    TVector<TString> Columns;
    TVector<TCondition> Conditions;
    TMaybe<ui64> Limit;
};

struct TParseError
{
    size_t Offset = 0;
    TString Message;
};

TMaybe<TSelect> Parse(TStringBuf input, TParseError* error = nullptr);

}   // namespace NCloud::NFileStore::NStorage::NQuery
