#pragma once

#include "public.h"

#include <cloud/blockstore/config/client.pb.h>
#include <cloud/blockstore/public/api/protos/endpoints.pb.h>

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/blockstore/libs/client/throttling.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/encryption/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/coroutine/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/variant.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IEndpointSession
{
    virtual ~IEndpointSession() = default;

    virtual NThreading::TFuture<NProto::TError> Remove(
        TCallContextPtr callContext,
        NProto::THeaders headers) = 0;

    virtual NThreading::TFuture<NProto::TMountVolumeResponse> Alter(
        TCallContextPtr callContext,
        NProto::EVolumeAccessMode accessMode,
        NProto::EVolumeMountMode mountMode,
        ui64 mountSeqNumber,
        NProto::THeaders headers) = 0;

    virtual NThreading::TFuture<NProto::TMountVolumeResponse> Describe(
        TCallContextPtr callContext,
        NProto::THeaders headers) const = 0;

    virtual NClient::ISessionPtr GetSession() const = 0;
    virtual NProto::TClientPerformanceProfile GetProfile() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct ISessionFactory
{
    virtual ~ISessionFactory() = default;

    using TSessionOrError = TResultOrError<IEndpointSessionPtr>;

    virtual NThreading::TFuture<TSessionOrError> CreateSession(
        TCallContextPtr callContext,
        const NProto::TStartEndpointRequest& request,
        NProto::TVolume& volume) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TSessionFactoryOptions
{
    bool StrictContractValidation = false;
    bool TemporaryServer = false;
    bool DisableDurableClient = false;

    NProto::TClientConfig DefaultClientConfig;
    NClient::THostPerformanceProfile HostProfile;
};

////////////////////////////////////////////////////////////////////////////////

ISessionFactoryPtr CreateSessionFactory(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats,
    IServerStatsPtr serverStats,
    IBlockStorePtr service,
    IStorageProviderPtr storageProvider,
    IEncryptionClientFactoryPtr encryptionClientFactory,
    TExecutorPtr executor,
    TSessionFactoryOptions options);

}   // namespace NCloud::NBlockStore::NServer
