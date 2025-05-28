#pragma once

#include "public.h"

#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/server/public.h>

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////

using TDescribeFuture = NThreading::TFuture<NProto::TDescribeVolumeResponse>;

std::optional<TDescribeFuture> DescribeRemoteVolume(
    const TString& diskId,
    const NProto::THeaders& headers,
    const IBlockStorePtr& localService,
    const IRemoteStorageProviderPtr& remoteStorageProvider,
    const ILoggingServicePtr& logging,
    const NProto::TClientConfig& clientConfig);

}   // namespace NCloud::NBlockStore::NSharding
