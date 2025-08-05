#pragma once

#include "public.h"

#include <cloud/blockstore/config/client.pb.h>
#include <cloud/blockstore/public/api/protos/endpoints.pb.h>

#include <cloud/blockstore/libs/rdma/iface/client.h>

#include <cloud/blockstore/libs/cells/iface/public.h>
#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/client/throttling.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/encryption/public.h>
#include <cloud/blockstore/libs/server/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/coroutine/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/variant.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TSessionInfo
{
    NProto::TVolume Volume;
    NClient::ISessionPtr Session;
};

////////////////////////////////////////////////////////////////////////////////

struct ISessionManager
{
    virtual ~ISessionManager() = default;

    using TSessionOrError = TResultOrError<TSessionInfo>;

    virtual NThreading::TFuture<TSessionOrError> CreateSession(
        TCallContextPtr callContext,
        const NProto::TStartEndpointRequest& request) = 0;

    virtual NThreading::TFuture<NProto::TError> RemoveSession(
        TCallContextPtr callContext,
        const TString& socketPath,
        const NProto::THeaders& headers) = 0;

    virtual NThreading::TFuture<NProto::TError> AlterSession(
        TCallContextPtr callContext,
        const TString& socketPath,
        NProto::EVolumeAccessMode accessMode,
        NProto::EVolumeMountMode mountMode,
        ui64 mountSeqNumber,
        const NProto::THeaders& headers) = 0;

    virtual NThreading::TFuture<TSessionOrError> GetSession(
        TCallContextPtr callContext,
        const TString& socketPath,
        const NProto::THeaders& headers) = 0;

    virtual TResultOrError<NProto::TClientPerformanceProfile> GetProfile(
        const TString& socketPath) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TSessionManagerOptions
{
    bool StrictContractValidation = false;
    bool TemporaryServer = false;
    bool DisableDurableClient = false;
    bool DisableClientThrottler = false;

    NProto::TClientConfig DefaultClientConfig;
    NClient::THostPerformanceProfile HostProfile;
};

////////////////////////////////////////////////////////////////////////////////

ISessionManagerPtr CreateSessionManager(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats,
    IServerStatsPtr serverStats,
    IBlockStorePtr service,
    NCells::ICellManagerPtr cellManager,
    IStorageProviderPtr storageProvider,
    NRdma::IClientPtr rdmaClient,
    IEncryptionClientFactoryPtr encryptionClientFactory,
    TExecutorPtr executor,
    TSessionManagerOptions options,
    const TServerAppConfigPtr config);

}   // namespace NCloud::NBlockStore::NServer
