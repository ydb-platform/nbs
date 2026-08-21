#pragma once

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>

////////////////////////////////////////////////////////////////////////////////

bool operator==(bool lhs, const NCloud::TResultOrError<bool>& rhs);
bool operator==(ui32 lhs, const NCloud::TResultOrError<ui32>& rhs);
bool operator==(TString lhs, const NCloud::TResultOrError<TStringBuf>& rhs);

template <>
void Out<NCloud::TResultOrError<bool>>(
    IOutputStream& os,
    const NCloud::TResultOrError<bool>& value);

template <>
void Out<NCloud::TResultOrError<ui32>>(
    IOutputStream& os,
    const NCloud::TResultOrError<ui32>& value);

template <>
void Out<NCloud::TResultOrError<TStringBuf>>(
    IOutputStream& os,
    const NCloud::TResultOrError<TStringBuf>& value);
