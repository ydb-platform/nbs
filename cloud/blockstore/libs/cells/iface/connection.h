#pragma once

#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

#include <memory>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

struct ICellConnectionObserver
{
    virtual ~ICellConnectionObserver() = default;

    virtual void OnTabletHostChanged(TString fqdn) noexcept = 0;

    // On connect and after every move, one at a time, from the thread that
    // completed it; may wait on the volume stats lock.
    virtual void OnServingHostChanged(TString fqdn) noexcept
    {
        Y_UNUSED(fqdn);
    }
};

using ICellConnectionObserverPtr = std::shared_ptr<ICellConnectionObserver>;

////////////////////////////////////////////////////////////////////////////////

struct ICellConnection
{
    virtual ~ICellConnection() = default;

    [[nodiscard]] virtual TString GetHost() const = 0;

    // Not const: the returned objects co-own the connection, so handing them
    // out is not an observation of it.
    virtual IBlockStorePtr GetService() = 0;
    virtual IStoragePtr GetStorage() = 0;
};

using ICellConnectionPtr = std::shared_ptr<ICellConnection>;

using TCellConnectionFuture =
    NThreading::TFuture<TResultOrError<ICellConnectionPtr>>;

}   // namespace NCloud::NBlockStore::NCells
