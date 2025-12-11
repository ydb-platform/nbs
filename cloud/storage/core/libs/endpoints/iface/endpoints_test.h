#pragma once

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct IMutableEndpointStorage
{
    virtual ~IMutableEndpointStorage() = default;

    virtual NProto::TError Init() = 0;
    virtual NProto::TError Remove() = 0;

    virtual TResultOrError<TString> AddEndpoint(
        const TString& key,
        const TString& data) = 0;

    virtual NProto::TError RemoveEndpoint(const TString& key) = 0;
};

}   // namespace NCloud
