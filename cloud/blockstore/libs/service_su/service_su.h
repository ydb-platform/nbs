#pragma once

#include "public.h"

#include <cloud/blockstore/config/server.pb.h>

#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/server/public.h>

#include <cloud/storage/core/libs/common/startable.h>

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/client/throttling.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/encryption/public.h>
#include <cloud/blockstore/libs/server/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateRemoteEndpoint(IBlockStorePtr endpoint);

NThreading::TFuture<NProto::TDescribeVolumeResponse> DescribeRemoteVolume(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TDescribeVolumeRequest> request,
    const IBlockStorePtr localService,
    const IRemoteStorageProviderPtr suProvider,
    const ILoggingServicePtr logging,
    NClient::TClientAppConfigPtr clientConfig);

}   // namespace NCloud::NBlockStore::NServer
