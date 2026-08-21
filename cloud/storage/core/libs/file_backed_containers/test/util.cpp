#include "util.h"

////////////////////////////////////////////////////////////////////////////////

bool operator==(bool lhs, const NCloud::TResultOrError<bool>& rhs)
{
    return !HasError(rhs) && lhs == rhs.GetResult();
}

bool operator==(ui32 lhs, const NCloud::TResultOrError<ui32>& rhs)
{
    return !HasError(rhs) && lhs == rhs.GetResult();
}

bool operator==(
    TString lhs,
    const NCloud::TResultOrError<TStringBuf>& rhs)
{
    return !HasError(rhs) && lhs == TString(rhs.GetResult());
}

template <>
void Out<NCloud::TResultOrError<bool>>(
    IOutputStream& os,
    const NCloud::TResultOrError<bool>& value)
{
    if (HasError(value)) {
        os << value.GetError();
    } else {
        os << value.GetResult();
    }
}

template <>
void Out<NCloud::TResultOrError<ui32>>(
    IOutputStream& os,
    const NCloud::TResultOrError<ui32>& value)
{
    if (HasError(value)) {
        os << value.GetError();
    } else {
        os << value.GetResult();
    }
}

template <>
void Out<NCloud::TResultOrError<TStringBuf>>(
    IOutputStream& os,
    const NCloud::TResultOrError<TStringBuf>& value)
{
    if (HasError(value)) {
        os << value.GetError();
    } else {
        os << value.GetResult();
    }
}
