#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/incomplete_requests.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <cloud/blockstore/public/api/protos/headers.pb.h>

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/grpc/public.h>

#include <util/generic/strbuf.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IServer
    : public IStartable
    , public IIncompleteRequestProvider
{
    virtual IClientStorageFactoryPtr GetClientStorageFactory() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TServerOptions
{
    TString CellId;
};

namespace NImpl {

////////////////////////////////////////////////////////////////////////////////

void PrepareRequestHeaders(
    NCloud::NProto::ERequestSource source,
    TStringBuf peer,
    TStringBuf authToken,
    NProto::THeaders& headers);

}   // namespace NImpl

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    TServerAppConfigPtr config,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    IBlockStorePtr service,
    IBlockStorePtr udsService,
    TServerOptions options,
    ICertificateProviderPtr certificateProvider);

}   // namespace NCloud::NBlockStore::NServer
