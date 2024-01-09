#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct IEndpointStorage
{
    virtual ~IEndpointStorage() = default;

    virtual TResultOrError<TVector<ui32>> GetEndpointIds() = 0;

    virtual TResultOrError<TString> GetEndpoint(ui32 endpointId) = 0;

    virtual TResultOrError<ui32> AddEndpoint(const TString& endpointSpec) = 0;

    virtual NProto::TError RemoveEndpoint(ui32 endpointId) = 0;
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

template <typename TRequest>
TResultOrError<TString> SerializeEndpoint(const TRequest& request)
{
    auto data = TString::Uninitialized(request.ByteSize());

    if (!request.SerializeToArray(const_cast<char*>(data.data()), data.size())) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "Could not serialize endpoint: " << request.ShortDebugString());
    }

    return data;
}

}   // namespace NCloud
