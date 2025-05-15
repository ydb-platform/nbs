#include "spdk_server.h"

#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/endpoints/endpoint_listener.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/spdk/iface/device.h>
#include <cloud/blockstore/libs/spdk/iface/env.h>
#include <cloud/blockstore/libs/spdk/iface/target.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/guid.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NServer {

using namespace NMonitoring;
using namespace NThreading;

using namespace NCloud::NBlockStore::NClient;
using namespace NCloud::NBlockStore::NSpdk;

namespace {

////////////////////////////////////////////////////////////////////////////////

static const TString IP4_ANY = "0.0.0.0";
static const TString IP6_ANY = "[::]";
static const TString IQN_ANY = "ANY";
static const TString NETMASK_ANY = "ANY";

////////////////////////////////////////////////////////////////////////////////

class TSpdkDeviceWrapper final
    : public ISpdkDevice
{
private:
    const ISessionPtr Session;
    const ui32 BlockSize;

public:
    TSpdkDeviceWrapper(ISessionPtr session, ui32 blockSize)
        : Session(std::move(session))
        , BlockSize(blockSize)
    {}

    void Start() override
    {
        // nothing to do
    }

    void Stop() override
    {
        // nothing to do
    }

    TFuture<void> StartAsync() override
    {
        // nothing to do
        return {};
    }

    TFuture<void> StopAsync() override
    {
        // nothing to do
        return {};
    }

    TFuture<NProto::TError> Read(
        void* buf,
        ui64 fileOffset,
        ui32 bytesCount) override
    {
        auto callContext = MakeIntrusive<TCallContext>(CreateRequestId());

        auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
        request->MutableHeaders()->SetRequestId(callContext->RequestId);
        request->MutableHeaders()->SetTimestamp(TInstant::Now().MicroSeconds());
        request->SetStartIndex(fileOffset / BlockSize);
        request->SetBlocksCount(bytesCount / BlockSize);
        request->BlockSize = BlockSize;

        auto sgListOrError = SgListNormalize(
            { (char *)buf, bytesCount },
            BlockSize);

        if (HasError(sgListOrError)) {
            return MakeFuture(sgListOrError.GetError());
        }

        auto guardedSgList = TGuardedSgList{ sgListOrError.ExtractResult() };
        request->Sglist = guardedSgList;

        auto future = Session->ReadBlocksLocal(
            std::move(callContext),
            std::move(request));

        return future.Apply([=] (auto future) mutable {
            guardedSgList.Close();

            auto response = ExtractResponse(future);
            return response.GetError();
        });
    }

    TFuture<NProto::TError> Read(
        TSgList sglist,
        ui64 fileOffset,
        ui32 bytesCount) override
    {
        auto callContext = MakeIntrusive<TCallContext>(CreateRequestId());

        auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
        request->MutableHeaders()->SetRequestId(callContext->RequestId);
        request->MutableHeaders()->SetTimestamp(TInstant::Now().MicroSeconds());
        request->SetStartIndex(fileOffset / BlockSize);
        request->SetBlocksCount(bytesCount / BlockSize);
        request->BlockSize = BlockSize;

        auto sgListOrError = SgListNormalize(std::move(sglist), BlockSize);
        if (HasError(sgListOrError)) {
            return MakeFuture(sgListOrError.GetError());
        }

        auto guardedSgList = TGuardedSgList{ sgListOrError.ExtractResult() };
        request->Sglist = guardedSgList;

        auto future = Session->ReadBlocksLocal(
            std::move(callContext),
            std::move(request));

        return future.Apply([=] (auto future) mutable {
            guardedSgList.Close();

            auto response = ExtractResponse(future);
            return response.GetError();
        });
    }

    TFuture<NProto::TError> Write(
        void* buf,
        ui64 fileOffset,
        ui32 bytesCount) override
    {
        auto callContext = MakeIntrusive<TCallContext>(CreateRequestId());

        auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request->MutableHeaders()->SetRequestId(callContext->RequestId);
        request->MutableHeaders()->SetTimestamp(TInstant::Now().MicroSeconds());
        request->SetStartIndex(fileOffset / BlockSize);
        request->BlocksCount = bytesCount / BlockSize;
        request->BlockSize = BlockSize;

        auto sgListOrError = SgListNormalize(
            { (char *)buf, bytesCount },
            BlockSize);

        if (HasError(sgListOrError)) {
            return MakeFuture(sgListOrError.GetError());
        }

        auto guardedSgList = TGuardedSgList{ sgListOrError.ExtractResult() };
        request->Sglist = guardedSgList;

        auto future = Session->WriteBlocksLocal(
            std::move(callContext),
            std::move(request));

        return future.Apply([=] (auto future) mutable {
            guardedSgList.Close();

            auto response = ExtractResponse(future);
            return response.GetError();
        });
    }

    TFuture<NProto::TError> Write(
        TSgList sglist,
        ui64 fileOffset,
        ui32 bytesCount) override
    {
        auto callContext = MakeIntrusive<TCallContext>(CreateRequestId());

        auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request->MutableHeaders()->SetRequestId(callContext->RequestId);
        request->MutableHeaders()->SetTimestamp(TInstant::Now().MicroSeconds());
        request->SetStartIndex(fileOffset / BlockSize);
        request->BlocksCount = bytesCount / BlockSize;
        request->BlockSize = BlockSize;

        auto sgListOrError = SgListNormalize(std::move(sglist), BlockSize);
        if (HasError(sgListOrError)) {
            return MakeFuture(sgListOrError.GetError());
        }

        auto guardedSgList = TGuardedSgList{ sgListOrError.ExtractResult() };
        request->Sglist = guardedSgList;

        auto future = Session->WriteBlocksLocal(
            std::move(callContext),
            std::move(request));

        return future.Apply([=] (auto future) mutable {
            guardedSgList.Close();

            auto response = ExtractResponse(future);
            return response.GetError();
        });
    }

    TFuture<NProto::TError> WriteZeroes(
        ui64 fileOffset,
        ui32 bytesCount) override
    {
        auto callContext = MakeIntrusive<TCallContext>(CreateRequestId());

        auto request = std::make_shared<NProto::TZeroBlocksRequest>();
        request->MutableHeaders()->SetRequestId(callContext->RequestId);
        request->MutableHeaders()->SetTimestamp(TInstant::Now().MicroSeconds());
        request->SetStartIndex(fileOffset / BlockSize);
        request->SetBlocksCount(bytesCount / BlockSize);

        auto future = Session->ZeroBlocks(
            std::move(callContext),
            std::move(request));

        return future.Apply([] (auto future) {
            auto response = ExtractResponse(future);
            return response.GetError();
        });
    }

    TFuture<NProto::TError> Erase(NProto::EDeviceEraseMethod method) override
    {
        return Session->EraseDevice(method);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSpdkEndpoint
{
    TString Device;
    ISpdkTargetPtr Target;
};

////////////////////////////////////////////////////////////////////////////////

class TNVMeEndpointListener final
    : public IEndpointListener
{
private:
    const ISpdkEnvPtr Env;
    const ILoggingServicePtr Logging;
    const IServerStatsPtr ServerStats;
    const TExecutorPtr Executor;
    const TNVMeEndpointConfig Config;

    bool Initialized = false;
    THashMap<TString, TSpdkEndpoint> Endpoints;

public:
    TNVMeEndpointListener(
            ISpdkEnvPtr env,
            ILoggingServicePtr logging,
            IServerStatsPtr serverStats,
            TExecutorPtr executor,
            const TNVMeEndpointConfig& config)
        : Env(std::move(env))
        , Logging(std::move(logging))
        , ServerStats(std::move(serverStats))
        , Executor(std::move(executor))
        , Config(config)
    {}

    TFuture<NProto::TError> StartEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        ISessionPtr session) override
    {
        return Executor->Execute([=, this] {
            return DoStartEndpoint(request, volume, session);
        });
    }

    TFuture<NProto::TError> AlterEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request, volume, session);

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> StopEndpoint(const TString& socketPath) override
    {
        return Executor->Execute([=, this] {
            return DoStopEndpoint(socketPath);
        });
    }

    NProto::TError RefreshEndpoint(
        const TString& socketPath,
        const NProto::TVolume& volume) override
    {
        Y_UNUSED(socketPath);
        Y_UNUSED(volume);
        return {};
    }

    TFuture<NProto::TError> SwitchEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request);
        Y_UNUSED(volume);
        Y_UNUSED(session);
        return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
    }

private:
    NProto::TError DoStartEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        ISessionPtr session)
    {
        if (!Initialized) {
            for (const auto& transportId: Config.TransportIDs) {
                auto transportRes = Executor->ResultOrError(
                    Env->AddTransport(transportId));
                if (HasError(transportRes)) {
                    return transportRes.GetError();
                }

                auto listenRes = Executor->ResultOrError(
                    Env->StartListen(transportId));
                if (HasError(listenRes)) {
                    return listenRes.GetError();
                }
            }

            Initialized = true;
        }

        // we use socket path for the endpoint key
        auto socketPath = request.GetUnixSocketPath();

        auto it = Endpoints.find(socketPath);
        if (it != Endpoints.end()) {
            return MakeError(S_ALREADY, TStringBuilder()
                << "endpoint already started " << socketPath.Quote());
        }

        auto device = std::make_shared<TSpdkDeviceWrapper>(
            std::move(session),
            volume.GetBlockSize());

        auto targetName = Config.Nqn + socketPath;

        auto registerRes = Executor->ResultOrError(
            Env->RegisterDeviceWrapper(
                std::move(device),
                CreateGuidAsString(),
                volume.GetBlocksCount(),
                volume.GetBlockSize()));
        if (HasError(registerRes)) {
            return registerRes.GetError();
        }

        auto deviceName = registerRes.GetResult();

        TVector<TString> devices = { deviceName };

        auto targetRes = Executor->ResultOrError(
            Env->CreateNVMeTarget(
                targetName,
                devices,
                Config.TransportIDs));
        if (HasError(targetRes)) {
            return targetRes.GetError();
        }

        auto target = targetRes.GetResult();

        auto startRes = Executor->ResultOrError(
            target->StartAsync());
        if (HasError(startRes)) {
            return startRes.GetError();
        }

        Endpoints.emplace(socketPath, TSpdkEndpoint { deviceName, target });
        return {};
    }

    NProto::TError DoStopEndpoint(const TString& socketPath)
    {
        auto it = Endpoints.find(socketPath);
        if (it == Endpoints.end()) {
            return MakeError(S_FALSE, TStringBuilder()
                << "endpoint not started " << socketPath.Quote());
        }

        auto& endpoint = it->second;

        auto stopRes = Executor->ResultOrError(
            endpoint.Target->StopAsync());
        if (HasError(stopRes)) {
            return stopRes.GetError();
        }

        auto unregisterRes = Executor->ResultOrError(
            Env->UnregisterDevice(endpoint.Device));
        if (HasError(unregisterRes)) {
            return unregisterRes.GetError();
        }

        Endpoints.erase(it);
        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSCSIEndpointListener final
    : public IEndpointListener
{
private:
    const ISpdkEnvPtr Env;
    const ILoggingServicePtr Logging;
    const IServerStatsPtr ServerStats;
    const TExecutorPtr Executor;
    const TSCSIEndpointConfig Config;

    bool Initialized = false;
    THashMap<TString, TSpdkEndpoint> Endpoints;

public:
    TSCSIEndpointListener(
            ISpdkEnvPtr env,
            ILoggingServicePtr logging,
            IServerStatsPtr serverStats,
            TExecutorPtr executor,
            const TSCSIEndpointConfig& config)
        : Env(std::move(env))
        , Logging(std::move(logging))
        , ServerStats(std::move(serverStats))
        , Executor(std::move(executor))
        , Config(config)
    {}

    TFuture<NProto::TError> StartEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        ISessionPtr session) override
    {
        return Executor->Execute([=, this] {
            return DoStartEndpoint(request, volume, session);
        });
    }

    TFuture<NProto::TError> AlterEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request, volume, session);

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> StopEndpoint(const TString& socketPath) override
    {
        return Executor->Execute([=, this] {
            return DoStopEndpoint(socketPath);
        });
    }

    NProto::TError RefreshEndpoint(
        const TString& socketPath,
        const NProto::TVolume& volume) override
    {
        Y_UNUSED(socketPath);
        Y_UNUSED(volume);
        return {};
    }

    TFuture<NProto::TError> SwitchEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) override
    {
        Y_UNUSED(request);
        Y_UNUSED(volume);
        Y_UNUSED(session);
        return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
    }

private:
    NProto::TError DoStartEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        ISessionPtr session)
    {
        if (!Initialized) {
            auto portal = ISpdkEnv::TPortal(
                Config.ListenAddress ? Config.ListenAddress : IP4_ANY,
                Config.ListenPort);

            auto initiator = ISpdkEnv::TInitiator(
                Config.InitiatorIqn ? Config.InitiatorIqn : IQN_ANY,
                Config.InitiatorMask ? Config.InitiatorMask : NETMASK_ANY);

            auto portalRes = Executor->ResultOrError(
                Env->CreatePortalGroup(1, { portal }));
            if (HasError(portalRes)) {
                return portalRes.GetError();
            }

            auto initiatorRes = Executor->ResultOrError(
                Env->CreateInitiatorGroup(1, { initiator }));
            if (HasError(initiatorRes)) {
                return initiatorRes.GetError();
            }

            Initialized = true;
        }

        // we use socket path for the endpoint key
        auto socketPath = request.GetUnixSocketPath();

        auto it = Endpoints.find(socketPath);
        if (it != Endpoints.end()) {
            return MakeError(S_ALREADY, TStringBuilder()
                << "endpoint already started " << socketPath.Quote());
        }

        auto device = std::make_shared<TSpdkDeviceWrapper>(
            std::move(session),
            volume.GetBlockSize());

        auto registerRes = Executor->ResultOrError(
            Env->RegisterDeviceWrapper(
                std::move(device),
                CreateGuidAsString(),
                volume.GetBlocksCount(),
                volume.GetBlockSize()));
        if (HasError(registerRes)) {
            return registerRes.GetError();
        }

        auto deviceName = registerRes.GetResult();

        TVector<ISpdkEnv::TDevice> devices = {
            { deviceName, 0 },
        };
        TVector<ISpdkEnv::TGroupMapping> groups = {
            { 1, 1 },
        };

        auto targetRes = Executor->ResultOrError(
            Env->CreateSCSITarget(
                socketPath,
                devices,
                groups));
        if (HasError(targetRes)) {
            return targetRes.GetError();
        }

        auto target = targetRes.GetResult();

        auto startRes = Executor->ResultOrError(
            target->StartAsync());
        if (HasError(startRes)) {
            return startRes.GetError();
        }

        Endpoints.emplace(socketPath, TSpdkEndpoint { deviceName, target });
        return {};
    }

    NProto::TError DoStopEndpoint(const TString& socketPath)
    {
        auto it = Endpoints.find(socketPath);
        if (it == Endpoints.end()) {
            return MakeError(S_FALSE, TStringBuilder()
                << "endpoint not started " << socketPath.Quote());
        }

        auto& endpoint = it->second;

        auto stopRes = Executor->ResultOrError(
            endpoint.Target->StopAsync());
        if (HasError(stopRes)) {
            return stopRes.GetError();
        }

        auto unregisterRes = Executor->ResultOrError(
            Env->UnregisterDevice(endpoint.Device));
        if (HasError(unregisterRes)) {
            return unregisterRes.GetError();
        }

        Endpoints.erase(it);
        return {};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointListenerPtr CreateNVMeEndpointListener(
    ISpdkEnvPtr env,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    const TNVMeEndpointConfig& config)
{
    return std::make_shared<TNVMeEndpointListener>(
        std::move(env),
        std::move(logging),
        std::move(serverStats),
        std::move(executor),
        config);
}

IEndpointListenerPtr CreateSCSIEndpointListener(
    ISpdkEnvPtr env,
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    const TSCSIEndpointConfig& config)
{
    return std::make_shared<TSCSIEndpointListener>(
        std::move(env),
        std::move(logging),
        std::move(serverStats),
        std::move(executor),
        config);
}

}   // namespace NCloud::NBlockStore::NServer
