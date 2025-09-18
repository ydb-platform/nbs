#pragma once

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/validation/validation.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

struct IClientFactory
{
    virtual ~IClientFactory() = default;

    virtual IBlockStorePtr CreateClient(
        TVector<ui32> nonretriableErrorCodes,
        const TString& clientId) = 0;

    virtual IBlockStorePtr CreateEndpointDataClient(
        NProto::EClientIpcType ipcType,
        const TString& socketPath,
        const TString& clientId,
        TVector<ui32> nonretriableErrorCodes) = 0;

    virtual NClient::IBlockStoreValidationClientPtr CreateValidationClient(
        IBlockStorePtr client,
        IValidationCallbackPtr callback,
        TString loggingTag,
        TBlockRange64 validationRange) = 0;

    virtual IBlockStorePtr CreateThrottlingClient(
        IBlockStorePtr client,
        NProto::TClientPerformanceProfile performanceProfile) = 0;

    virtual IBlockStorePtr CreateAndStartFilesystemClient() = 0;

    virtual const TString& GetEndpointStorageDir() const = 0;
};

}   // namespace NCloud::NBlockStore::NLoadTest
