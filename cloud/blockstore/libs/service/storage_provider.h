#pragma once

#include "public.h"

#include "request.h"

#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IStorageProvider
{
    virtual ~IStorageProvider() = default;

    virtual NThreading::TFuture<IStoragePtr> CreateStorage(
        const NProto::TVolume& volume,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) = 0;
};


struct IRemoteServiceProvider
{
    virtual ~IRemoteServiceProvider() = default;

    virtual NThreading::TFuture<IBlockStorePtr> CreateService(
        const TString& host,
        ui32 port) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IStorageProviderPtr CreateDefaultStorageProvider(
    IBlockStorePtr service);

IStorageProviderPtr CreateMultiStorageProvider(
    TVector<IStorageProviderPtr> storageProviders);

}   // namespace NCloud::NBlockStore
