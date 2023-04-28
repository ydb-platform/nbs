#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct IEndpointStorage
{
    virtual ~IEndpointStorage() = default;

    virtual TResultOrError<TVector<ui32>> GetEndpointIds() = 0;

    virtual TResultOrError<TString> GetEndpoint(ui32 keyringId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IEndpointStoragePtr CreateKeyringEndpointStorage(
    TString rootKeyringDesc,
    TString endpointsKeyringDesc);

IEndpointStoragePtr CreateFileEndpointStorage(TString dirPath);

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest>
std::shared_ptr<TRequest> DeserializeEndpoint(const TString& data)
{
    auto request = std::make_shared<TRequest>();
    if (!request->ParseFromArray(data.data(), data.size())) {
        return nullptr;
    }
    return request;
}

}   // namespace NCloud
