#pragma once

#include "config.h"
#include "host_endpoint.h"

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

struct ICellHost
{
    using TStartResult = TResultOrError<TCellHostConfig>;

    virtual NThreading::TFuture<TStartResult> Start() = 0;
    virtual NThreading::TFuture<void> Stop() = 0;

    [[nodiscard]] virtual bool IsReady(
        NProto::ECellDataTransport transport) const = 0;

    [[nodiscard]] virtual TResultOrError<TCellHostEndpoint> GetHostEndpoint(
        const NClient::TClientAppConfigPtr& clientConfig,
        std::optional<NProto::ECellDataTransport> transport,
        bool allowGrpcFallback) = 0;

    virtual ~ICellHost() = default;
};

}   // namespace NCloud::NBlockStore::NCells
