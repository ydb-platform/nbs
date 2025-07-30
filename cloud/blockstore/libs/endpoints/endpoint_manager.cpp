#include "endpoint_manager.h"

#include "endpoint_events.h"
#include "endpoint_listener.h"
#include "session_manager.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/durable.h>
#include <cloud/blockstore/libs/client/metric.h>
#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/nbd/device.h>
#include <cloud/blockstore/libs/nbd/error_handler.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/endpoints/iface/endpoints.h>
#include <cloud/storage/core/protos/error.pb.h>

#include <contrib/ydb/core/protos/flat_tx_scheme.pb.h>

#include <util/generic/guid.h>
#include <util/generic/hash.h>
#include <util/generic/overloaded.h>
#include <util/generic/set.h>
#include <util/string/builder.h>
#include <util/system/fs.h>

namespace NCloud::NBlockStore::NServer {

using namespace NClient;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto MIN_RECONNECT_DELAY = TDuration::MilliSeconds(10);
constexpr auto MAX_RECONNECT_DELAY = TDuration::Minutes(10);

////////////////////////////////////////////////////////////////////////////////

template <typename T>
int GetFieldCount()
{
    return T::GetDescriptor()->field_count();
}

bool CompareRequests(
    const NProto::TKmsKey& left,
    const NProto::TKmsKey& right)
{
    Y_DEBUG_ABORT_UNLESS(3 == GetFieldCount<NProto::TKmsKey>());
    return left.GetKekId() == right.GetKekId()
        && left.GetEncryptedDEK() == right.GetEncryptedDEK()
        && left.GetTaskId() == right.GetTaskId();
}

bool CompareRequests(
    const NProto::TKeyPath& left,
    const NProto::TKeyPath& right)
{
    Y_DEBUG_ABORT_UNLESS(3 == GetFieldCount<NProto::TKeyPath>());
    return left.GetKeyringId() == right.GetKeyringId()
        && left.GetFilePath() == right.GetFilePath()
        && CompareRequests(left.GetKmsKey(), right.GetKmsKey());
}

bool CompareRequests(
    const NProto::TEncryptionSpec& left,
    const NProto::TEncryptionSpec& right)
{
    Y_DEBUG_ABORT_UNLESS(3 == GetFieldCount<NProto::TEncryptionSpec>());
    return left.GetMode() == right.GetMode()
        && CompareRequests(left.GetKeyPath(), right.GetKeyPath())
        && left.GetKeyHash() == right.GetKeyHash();
}

bool CompareRequests(
    const NProto::TClientProfile& left,
    const NProto::TClientProfile& right)
{
    Y_DEBUG_ABORT_UNLESS(2 == GetFieldCount<NProto::TClientProfile>());
    return left.GetCpuUnitCount() == right.GetCpuUnitCount()
        && left.GetHostType() == right.GetHostType();
}

bool CompareRequests(
    const NProto::TClientMediaKindPerformanceProfile& left,
    const NProto::TClientMediaKindPerformanceProfile& right)
{
    Y_DEBUG_ABORT_UNLESS(4 == GetFieldCount<NProto::TClientMediaKindPerformanceProfile>());
    return left.GetMaxReadIops() == right.GetMaxReadIops()
        && left.GetMaxWriteIops() == right.GetMaxWriteIops()
        && left.GetMaxReadBandwidth() == right.GetMaxReadBandwidth()
        && left.GetMaxWriteBandwidth() == right.GetMaxWriteBandwidth();
}

bool CompareRequests(
    const NProto::TClientPerformanceProfile& left,
    const NProto::TClientPerformanceProfile& right)
{
    Y_DEBUG_ABORT_UNLESS(7 == GetFieldCount<NProto::TClientPerformanceProfile>());
    return CompareRequests(left.GetHDDProfile(), right.GetHDDProfile())
        && CompareRequests(left.GetSSDProfile(), right.GetSSDProfile())
        && CompareRequests(left.GetNonreplProfile(), right.GetNonreplProfile())
        && CompareRequests(left.GetMirror2Profile(), right.GetMirror2Profile())
        && CompareRequests(left.GetMirror3Profile(), right.GetMirror3Profile())
        && CompareRequests(left.GetHddNonreplProfile(), right.GetHddNonreplProfile())
        && left.GetBurstTime() == right.GetBurstTime();
}

bool CompareRequests(
    const NProto::TStartEndpointRequest& left,
    const NProto::TStartEndpointRequest& right)
{
    Y_DEBUG_ABORT_UNLESS(24 == GetFieldCount<NProto::TStartEndpointRequest>());
    return left.GetUnixSocketPath() == right.GetUnixSocketPath()
        && left.GetDiskId() == right.GetDiskId()
        && left.GetInstanceId() == right.GetInstanceId()
        && left.GetVolumeAccessMode() == right.GetVolumeAccessMode()
        && left.GetVolumeMountMode() == right.GetVolumeMountMode()
        && left.GetIpcType() == right.GetIpcType()
        && left.GetClientVersionInfo() == right.GetClientVersionInfo()
        && left.GetThrottlingDisabled() == right.GetThrottlingDisabled()
        && left.GetMountSeqNumber() == right.GetMountSeqNumber()
        && left.GetClientId() == right.GetClientId()
        && CompareRequests(left.GetClientProfile(), right.GetClientProfile())
        && CompareRequests(left.GetClientPerformanceProfile(), right.GetClientPerformanceProfile())
        && left.GetVhostQueuesCount() == right.GetVhostQueuesCount()
        && left.GetUnalignedRequestsDisabled() == right.GetUnalignedRequestsDisabled()
        && CompareRequests(left.GetEncryptionSpec(), right.GetEncryptionSpec())
        && left.GetSendNbdMinBlockSize() == right.GetSendNbdMinBlockSize()
        && left.GetMountFlags() == right.GetMountFlags()
        && left.GetDeviceName() == right.GetDeviceName()
        && std::equal(
            left.GetClientCGroups().begin(),
            left.GetClientCGroups().end(),
            right.GetClientCGroups().begin(),
            right.GetClientCGroups().end())
        && left.GetPersistent() == right.GetPersistent()
        && left.GetNbdDeviceFile() == right.GetNbdDeviceFile()
        && left.GetUseFreeNbdDeviceFile() == right.GetUseFreeNbdDeviceFile()
        && left.GetVhostDiscardEnabled() == right.GetVhostDiscardEnabled();
}

bool CompareRequests(
    const NProto::TStopEndpointRequest& left,
    const NProto::TStopEndpointRequest& right)
{
    Y_DEBUG_ABORT_UNLESS(3 == GetFieldCount<NProto::TStopEndpointRequest>());
    auto doTie = [](const NProto::TStopEndpointRequest& r)
    {
        return std::tie(
            r.GetUnixSocketPath(),
            r.GetDiskId(),
            r.GetHeaders().GetClientId());
    };
    return doTie(left) == doTie(right);
}

bool CompareRequests(
    const NProto::TRefreshEndpointRequest& left,
    const NProto::TRefreshEndpointRequest& right)
{
    Y_DEBUG_ABORT_UNLESS(2 == GetFieldCount<NProto::TRefreshEndpointRequest>());
    return left.GetUnixSocketPath() == right.GetUnixSocketPath();
}

////////////////////////////////////////////////////////////////////////////////

class TDeviceManager
{
private:
    const TString DevicePrefix;
    TVector<bool> BusyDevices;

public:
    TDeviceManager(TString devicePrefix)
        : DevicePrefix(std::move(devicePrefix))
    {
        size_t num = 0;
        while (NFs::Exists(GetDeviceName(num))) {
            ++num;
        }
        BusyDevices.resize(num, false);
    }

    NProto::TError AcquireDevice(const TString& device)
    {
        size_t num = 0;
        if (!GetDeviceNum(device, num)) {
            return MakeError(E_ARGUMENT, TStringBuilder()
                << "Couldn't parse nbd device file: " << device);
        }

        if (!NFs::Exists(device)) {
            return MakeError(E_INVALID_STATE, TStringBuilder()
                << "No file for nbd device: " << device);
        }

        if (num < BusyDevices.size() && BusyDevices[num]) {
            return MakeError(E_INVALID_STATE, TStringBuilder()
                << "Nbd device file doesn't exist: " << device);
        }

        if (num >= BusyDevices.size()) {
            BusyDevices.resize(num + 1, false);
        }

        BusyDevices[num] = true;
        return {};
    }

    void ReleaseDevice(const TString& device)
    {
        if (device.empty()) {
            return;
        }

        size_t num = 0;
        bool res = GetDeviceNum(device, num);
        Y_ENSURE(res && num < BusyDevices.size() && BusyDevices[num]);

        BusyDevices[num] = false;
    }

    TString GetFreeDevice()
    {
        size_t num = 0;
        for (; num < BusyDevices.size(); ++num) {
            if (!BusyDevices[num]) {
                break;
            }
        }

        return GetDeviceName(num);
    }

private:
    bool GetDeviceNum(const TString& device, size_t& num)
    {
        if (!device.StartsWith(DevicePrefix)) {
            return false;
        }

        auto numStr = device.substr(DevicePrefix.size());
        return TryFromString(numStr, num);
    }

    TString GetDeviceName(size_t num)
    {
        return DevicePrefix + ToString(num);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBlockStoreNotImplemented
    : public IBlockStore
{
public:
    void Start() override
    {}

    void Stop() override
    {}

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(ctx);                                                         \
        Y_UNUSED(request);                                                     \
        return MakeFuture<NProto::T##name##Response>(TErrorResponse(           \
            E_NOT_IMPLEMENTED, "Unsupported request"));                        \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

class TEndpointManager;

class TRestoringClient final
    : public TBlockStoreNotImplemented
{
private:
    TEndpointManager& EndpointManager;

public:
    TRestoringClient(TEndpointManager& endpointManager)
        : EndpointManager(endpointManager)
    {}

    TFuture<NProto::TStartEndpointResponse> StartEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStartEndpointRequest> req) override;
};

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    struct T##name##Method                                                     \
    {                                                                          \
        using TRequest = NProto::T##name##Request;                             \
        using TResponse = NProto::T##name##Response;                           \
    };
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_ENDPOINT_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

class TSwitchEndpointRequest
{
private:
    TString DiskId;
    TString UnixSocketPath;
    TString Reason;

public:
    const TString& GetDiskId() const
    {
        return DiskId;
    }

    void SetDiskId(const TString& diskId)
    {
        DiskId = diskId;
    }

    const TString& GetUnixSocketPath() const
    {
        return UnixSocketPath;
    }

    void SetUnixSocketPath(const TString& socketPath)
    {
        UnixSocketPath = socketPath;
    }

    const TString& GetReason() const
    {
        return Reason;
    }

    void SetReason(const TString& reason)
    {
        Reason = reason;
    }
};

////////////////////////////////////////////////////////////////////////////////

bool CompareRequests(
    const TSwitchEndpointRequest& left,
    const TSwitchEndpointRequest& right)
{
    return left.GetDiskId() == right.GetDiskId()
        && left.GetUnixSocketPath() == right.GetUnixSocketPath()
        && left.GetReason() == right.GetReason();
}

////////////////////////////////////////////////////////////////////////////////

struct TSwitchEndpointMethod
{
    using TRequest = TSwitchEndpointRequest;
    using TResponse = NProto::TError;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
struct TRequestState
{
    typename TMethod::TRequest Request;
    TFuture<typename TMethod::TResponse> Result;
};

////////////////////////////////////////////////////////////////////////////////

struct TEndpoint
{
    std::shared_ptr<NProto::TStartEndpointRequest> Request;
    NBD::IDevicePtr Device;
    NProto::TVolume Volume;
    std::weak_ptr<NClient::ISession> Session;
    ui64 Generation = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TEndpointManager final
    : public IEndpointManager
    , public IEndpointEventHandler
    , public std::enable_shared_from_this<TEndpointManager>
{
private:
    struct TErrorHandler;

    struct TExceptionContext
    {
        std::weak_ptr<TEndpoint> Endpoint;
        ui64 Generation;
        TBackoffDelayProvider BackoffProvider;

        TExceptionContext(std::weak_ptr<TEndpoint> endpoint, ui64 generation)
            : Endpoint(std::move(endpoint))
            , Generation(generation)
            , BackoffProvider(MIN_RECONNECT_DELAY, MAX_RECONNECT_DELAY)
        {}
    };

    struct TErrorHandler
        : NBD::IErrorHandler
        , std::enable_shared_from_this<TErrorHandler>
    {
        std::weak_ptr<TEndpointManager> Manager;
        std::weak_ptr<TEndpoint> Endpoint;
        std::atomic<ui64> Generation;

        TErrorHandler(std::weak_ptr<TEndpointManager> manager)
            : Manager(std::move(manager))
        {}

        TErrorHandler(
                std::weak_ptr<TEndpointManager> manager,
                std::shared_ptr<TEndpoint> endpoint)
            : Manager(std::move(manager))
        {
            SetEndpoint(std::move(endpoint));
        }

        void SetEndpoint(std::shared_ptr<TEndpoint> endpoint)
        {
            Endpoint = endpoint;
            Generation = endpoint->Generation;
        }

        void ProcessException(std::exception_ptr e) override
        {
            Y_UNUSED(e);

            if (auto manager = Manager.lock()) {
                manager->ProcessException(
                    std::make_shared<TExceptionContext>(Endpoint, Generation));
            }
        }
    };

    ////////////////////////////////////////////////////////////////////////////

    const ILoggingServicePtr Logging;
    const IServerStatsPtr ServerStats;
    const ISchedulerPtr Scheduler;
    const TExecutorPtr Executor;
    const ISessionManagerPtr SessionManager;
    const IEndpointStoragePtr EndpointStorage;
    const THashMap<NProto::EClientIpcType, IEndpointListenerPtr> EndpointListeners;
    const NBD::IDeviceFactoryPtr NbdDeviceFactory;
    const IBlockStorePtr Service;
    const TString NbdSocketSuffix;
    const TString NbdDevicePrefix;
    const bool AutomaticNbdDeviceManagement;

    TDeviceManager NbdDeviceManager;
    NBD::IErrorHandlerMapPtr NbdErrorHandlerMap;
    TLog Log;

    using TRequestStateVariant = std::variant<
        TRequestState<TStartEndpointMethod>,
        TRequestState<TStopEndpointMethod>,
        TRequestState<TRefreshEndpointMethod>,
        TRequestState<TSwitchEndpointMethod>
    >;
    THashMap<TString, TRequestStateVariant> ProcessingSockets;

    THashMap<TString, std::shared_ptr<TEndpoint>> Endpoints;

    NClient::IMetricClientPtr RestoringClient;
    TSet<TString> RestoringEndpoints;

    enum {
        WaitingForRestoring = 0,
        ReadingStorage = 1,
        StartingEndpoints = 2,
        Completed = 3,
    };

    TAtomic RestoringStage = WaitingForRestoring;

public:
    TEndpointManager(
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            ILoggingServicePtr logging,
            IRequestStatsPtr requestStats,
            IVolumeStatsPtr volumeStats,
            IServerStatsPtr serverStats,
            TExecutorPtr executor,
            ISessionManagerPtr sessionManager,
            IEndpointStoragePtr endpointStorage,
            THashMap<NProto::EClientIpcType, IEndpointListenerPtr> listeners,
            NBD::IDeviceFactoryPtr nbdDeviceFactory,
            NBD::IErrorHandlerMapPtr nbdErrorHandlerMap,
            IBlockStorePtr service,
            TEndpointManagerOptions options)
        : Logging(std::move(logging))
        , ServerStats(std::move(serverStats))
        , Scheduler(std::move(scheduler))
        , Executor(std::move(executor))
        , SessionManager(std::move(sessionManager))
        , EndpointStorage(std::move(endpointStorage))
        , EndpointListeners(std::move(listeners))
        , NbdDeviceFactory(std::move(nbdDeviceFactory))
        , Service(std::move(service))
        , NbdSocketSuffix(options.NbdSocketSuffix)
        , NbdDevicePrefix(options.NbdDevicePrefix)
        , AutomaticNbdDeviceManagement(options.AutomaticNbdDeviceManagement)
        , NbdDeviceManager(options.NbdDevicePrefix)
        , NbdErrorHandlerMap(std::move(nbdErrorHandlerMap))
    {
        Log = Logging->CreateLog("BLOCKSTORE_SERVER");

        IBlockStorePtr client = std::make_shared<TRestoringClient>(*this);

        NProto::TClientAppConfig config;
        *config.MutableClientConfig() = options.ClientConfig;
        auto appConfig = std::make_shared<TClientAppConfig>(std::move(config));
        auto retryPolicy = CreateRetryPolicy(appConfig, std::nullopt);

        client = CreateDurableClient(
            std::move(appConfig),
            std::move(client),
            std::move(retryPolicy),
            Logging,
            std::move(timer),
            Scheduler,
            std::move(requestStats),
            std::move(volumeStats));

        RestoringClient = NClient::CreateMetricClient(
            std::move(client),
            Logging,
            ServerStats);
    }

    void Start() override
    {
        RestoringClient->Start();
    }

    void Stop() override
    {
        RestoringClient->Stop();
    }

    size_t CollectRequests(
        const TIncompleteRequestsCollector& collector) override
    {
        return RestoringClient->CollectRequests(collector);
    }

#define ENDPOINT_IMPLEMENT_METHOD(name, specifier, ...)                        \
    TFuture<T##name##Method::TResponse> name(                                  \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<T##name##Method::TRequest> request) specifier          \
    {                                                                          \
        return Executor->Execute([                                             \
            ctx = std::move(callContext),                                      \
            req = std::move(request),                                          \
            weakSelf = weak_from_this()] () mutable                            \
        -> T##name##Method::TResponse                                          \
        {                                                                      \
            auto self = weakSelf.lock();                                       \
            if (!self) {                                                       \
                return TErrorResponse(E_FAIL, "EndpointManager is destroyed"); \
            }                                                                  \
                                                                               \
            return self->Do##name(std::move(ctx), std::move(req));             \
        });                                                                    \
    }                                                                          \
                                                                               \
    T##name##Method::TResponse Do##name(                                       \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<T##name##Method::TRequest> req);                       \
// ENDPOINT_IMPLEMENT_METHOD

    BLOCKSTORE_ENDPOINT_SERVICE(ENDPOINT_IMPLEMENT_METHOD, override)
    ENDPOINT_IMPLEMENT_METHOD(SwitchEndpoint, )

#undef ENDPOINT_IMPLEMENT_METHOD

    TFuture<NProto::TError> SwitchEndpointIfNeeded(
        const TString& diskId,
        const TString& reason) override;

    TFuture<void> RestoreEndpoints() override
    {
        AtomicSet(RestoringStage, ReadingStorage);
        return Executor->Execute([weakSelf = weak_from_this()] () mutable {
            if (auto self = weakSelf.lock()) {
                auto future = self->DoRestoreEndpoints();
                AtomicSet(self->RestoringStage, self->StartingEndpoints);
                self->Executor->WaitFor(future);
                AtomicSet(self->RestoringStage, self->Completed);
            }
        });
    }

    TFuture<NProto::TStartEndpointResponse> RestoreSingleEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStartEndpointRequest> request);

    void ProcessException(std::shared_ptr<TExceptionContext> context);

private:
    void ProcessException(
        std::shared_ptr<TExceptionContext> context,
        TString prefix);

    void DoProcessException(std::shared_ptr<TExceptionContext> context);

    bool IsEndpointRestoring(const TString& socket)
    {
        switch (AtomicGet(RestoringStage)) {
            case WaitingForRestoring:
                return true;
            case ReadingStorage: return true;
            case StartingEndpoints: return RestoringEndpoints.contains(socket);
            case Completed: return false;
        }
        return false;
    }

    NProto::TStartEndpointResponse StartEndpointImpl(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStartEndpointRequest> request,
        bool restoring);

    NProto::TStopEndpointResponse StopEndpointFallback(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStopEndpointRequest> request);

    NProto::TStopEndpointResponse StopEndpointImpl(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStopEndpointRequest> request);

    NProto::TRefreshEndpointResponse RefreshEndpointImpl(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TRefreshEndpointRequest> request);

    NProto::TError SwitchEndpointImpl(
        TCallContextPtr ctx,
        std::shared_ptr<TSwitchEndpointRequest> request);

    NProto::TError AlterEndpoint(
        TCallContextPtr ctx,
        NProto::TStartEndpointRequest newReq,
        NProto::TStartEndpointRequest oldReq);

    NProto::TError RestartListenerEndpoint(
        TCallContextPtr ctx,
        const NProto::TStartEndpointRequest& request);

    NProto::TError OpenAllEndpointSockets(
        const NProto::TStartEndpointRequest& request,
        const TSessionInfo& sessionInfo);

    NProto::TError OpenEndpointSocket(
        const NProto::TStartEndpointRequest& request,
        const TSessionInfo& sessionInfo);

    void CloseAllEndpointSockets(const NProto::TStartEndpointRequest& request);
    void CloseEndpointSocket(const NProto::TStartEndpointRequest& request);

    std::shared_ptr<NProto::TStartEndpointRequest> CreateNbdStartEndpointRequest(
        const NProto::TStartEndpointRequest& request);

    TResultOrError<NBD::IDevicePtr> StartNbdDevice(
        std::shared_ptr<NProto::TStartEndpointRequest> request,
        bool restoring,
        const NProto::TVolume& volume);

    void ReleaseNbdDevice(const TString& device, bool restoring);

    template <typename T>
    void RemoveSession(TCallContextPtr ctx, const T& request)
    {
        auto future = SessionManager->RemoveSession(
            std::move(ctx),
            request.GetUnixSocketPath(),
            request.GetHeaders());

        if (const auto& error = Executor->WaitFor(future); HasError(error)) {
            STORAGE_ERROR("Failed to remove session: " << FormatError(error));
        }
    }

    TFuture<void> DoRestoreEndpoints();

    void HandleRestoredEndpoint(
        const TString& socket,
        const TString& endpointId,
        const NProto::TError& error);

    NProto::TError AddEndpointToStorage(
        const NProto::TStartEndpointRequest& request)
    {
        if (!request.GetPersistent()) {
            return {};
        }

        auto [data, error] = SerializeEndpoint(request);
        if (HasError(error)) {
            return error;
        }

        return EndpointStorage->AddEndpoint(request.GetUnixSocketPath(), data);
    }

    template <typename TMethod>
    TPromise<typename TMethod::TResponse> AddProcessingSocket(
        const typename TMethod::TRequest& request)
    {
        auto promise = NewPromise<typename TMethod::TResponse>();
        const auto& socketPath = request.GetUnixSocketPath();

        auto it = ProcessingSockets.find(socketPath);
        if (it != ProcessingSockets.end()) {
            const auto& st = it->second;
            auto* state = std::get_if<TRequestState<TMethod>>(&st);
            if (!state) {
                auto response = TErrorResponse(E_REJECTED, TStringBuilder()
                    << "endpoint " << socketPath.Quote()
                    << " is " << GetProcessName(st) << " now");
                promise.SetValue(std::move(response));
                return promise;
            }

            if (!CompareRequests(request, state->Request)) {
                auto response = TErrorResponse(E_REJECTED, TStringBuilder()
                    << "endpoint " << socketPath.Quote()
                    << " is " << GetProcessName(st) << " now with other args");
                promise.SetValue(std::move(response));
                return promise;
            }

            // we need a copy here because `it` may be invalidated during the
            // execution of WaitFor
            auto future = state->Result;
            promise.SetValue(Executor->WaitFor(future));
            return promise;
        }

        auto [_, inserted] = ProcessingSockets.emplace(
            socketPath,
            TRequestState<TMethod>{request, promise.GetFuture()});
        Y_ABORT_UNLESS(inserted);

        return promise;
    }

    void RemoveProcessingSocket(const TString& socketPath)
    {
        ProcessingSockets.erase(socketPath);
    }

    TString GetProcessName(const TRequestStateVariant& st)
    {
        return std::visit(TOverloaded{
            [] (const TRequestState<TStartEndpointMethod>&) {
                return "starting";
            },
            [] (const TRequestState<TStopEndpointMethod>&) {
                return "stopping";
            },
            [] (const TRequestState<TRefreshEndpointMethod>&) {
                return "refreshing";
            },
            [] (const TRequestState<TSwitchEndpointMethod>&) {
                return "switching";
            },
            [](const auto&) {
                return "busy (undefined process)";
            }
        }, st);
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TStartEndpointResponse> TEndpointManager::RestoreSingleEndpoint(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TStartEndpointRequest> request)
{
    return Executor->Execute(
        [weakSelf = weak_from_this(),
         ctx,
         request]() mutable -> NProto::TStartEndpointResponse
        {
            auto self = weakSelf.lock();
            if (!self) {
                return TErrorResponse(E_FAIL, "EndpointManager is destroyed");
            }

            auto promise =
                self->AddProcessingSocket<TStartEndpointMethod>(*request);
            if (promise.HasValue()) {
                return promise.ExtractValue();
            }

            auto socketPath = TFsPath(request->GetUnixSocketPath());
            socketPath.Parent().MkDirs();

            auto response = self->StartEndpointImpl(
                std::move(ctx),
                std::move(request),
                true);
            promise.SetValue(response);

            self->RemoveProcessingSocket(socketPath);
            return response;
        });
}

NProto::TStartEndpointResponse TEndpointManager::DoStartEndpoint(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TStartEndpointRequest> request)
{
    auto socketPath = request->GetUnixSocketPath();
    if (IsEndpointRestoring(socketPath)) {
        return TErrorResponse(E_REJECTED, "endpoint is restoring now");
    }

    // We can have concurrent StartEndpoint and RestoreEndpoint call when the
    // process starts. AddProcessingSocket should protect us from this race and
    // delay the StartEndpoint call.
    auto promise = AddProcessingSocket<TStartEndpointMethod>(*request);
    if (promise.HasValue()) {
        return promise.ExtractValue();
    }

    auto response =
        StartEndpointImpl(std::move(ctx), std::move(request), false);
    promise.SetValue(response);

    RemoveProcessingSocket(socketPath);
    return response;
}

NProto::TStartEndpointResponse TEndpointManager::StartEndpointImpl(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TStartEndpointRequest> request,
    bool restoring)
{
    const auto& socketPath = request->GetUnixSocketPath();

    auto it = Endpoints.find(socketPath);
    if (it != Endpoints.end()) {
        const auto& endpoint = *it->second;

        if (!NFs::Exists(socketPath)) {
            // restart listener endpoint to recreate the socket
            auto error = RestartListenerEndpoint(ctx, *endpoint.Request);
            if (HasError(error)) {
                return TErrorResponse(error);
            }

            if (!NFs::Exists(socketPath)) {
                return TErrorResponse(E_INVALID_STATE, TStringBuilder()
                    << "failed to recreate socket: " << socketPath.Quote());
            }
        }

        auto error = AlterEndpoint(std::move(ctx), *request, *endpoint.Request);
        if (HasError(error)) {
            return TErrorResponse(error);
        }

        NProto::TStartEndpointResponse response;
        response.MutableError()->CopyFrom(error);
        response.MutableVolume()->CopyFrom(endpoint.Volume);
        response.SetNbdDeviceFile(endpoint.Request->GetNbdDeviceFile());
        return response;
    }

    auto future = SessionManager->CreateSession(ctx, *request);
    auto [sessionInfo, error] = Executor->WaitFor(future);
    if (HasError(error)) {
        return TErrorResponse(error);
    }

    // additional NBD socket will be opened for gRPC endpoints
    auto errorHandler = std::make_shared<TErrorHandler>(weak_from_this());

    if (request->GetIpcType() == NProto::IPC_NBD ||
        request->GetIpcType() == NProto::IPC_GRPC && NbdSocketSuffix.size())
    {
        NbdErrorHandlerMap->Emplace(socketPath, errorHandler);
    }

    error = OpenAllEndpointSockets(*request, sessionInfo);
    if (HasError(error)) {
        RemoveSession(std::move(ctx), *request);
        NbdErrorHandlerMap->Erase(socketPath);
        return TErrorResponse(error);
    }

    auto deviceOrError = StartNbdDevice(request, restoring, sessionInfo.Volume);
    error = deviceOrError.GetError();
    if (HasError(error)) {
        CloseAllEndpointSockets(*request);
        NbdErrorHandlerMap->Erase(socketPath);
        RemoveSession(std::move(ctx), *request);
        return TErrorResponse(error);
    }
    auto device = deviceOrError.ExtractResult();

    if (!restoring) {
        error = AddEndpointToStorage(*request);
        if (HasError(error)) {
            auto stopFuture = device->Stop(true);
            const auto& stopError = Executor->WaitFor(stopFuture);
            if (HasError(stopError)) {
                return TErrorResponse(stopError);
            }
            ReleaseNbdDevice(request->GetNbdDeviceFile(), restoring);
            CloseAllEndpointSockets(*request);
            NbdErrorHandlerMap->Erase(socketPath);
            RemoveSession(std::move(ctx), *request);
            return TErrorResponse(error);
        }
    }

    auto endpoint = std::make_shared<TEndpoint>(
        request,
        device,
        sessionInfo.Volume,
        sessionInfo.Session);

    errorHandler->SetEndpoint(endpoint);

    if (auto c = ServerStats->GetEndpointCounter(request->GetIpcType())) {
        c->Inc();
    }
    auto [_, inserted] = Endpoints.emplace(socketPath, std::move(endpoint));
    STORAGE_VERIFY(inserted, TWellKnownEntityTypes::ENDPOINT, socketPath);

    NProto::TStartEndpointResponse response;
    response.MutableVolume()->CopyFrom(sessionInfo.Volume);
    response.SetNbdDeviceFile(request->GetNbdDeviceFile());
    return response;
}

NProto::TError TEndpointManager::AlterEndpoint(
    TCallContextPtr ctx,
    NProto::TStartEndpointRequest newReq,
    NProto::TStartEndpointRequest oldReq)
{
    const auto& socketPath = newReq.GetUnixSocketPath();

    // NBS-3018
    if (!CompareRequests(oldReq.GetClientProfile(), newReq.GetClientProfile())) {
        STORAGE_WARN("Modified ClientProfile will be ignored for endpoint: "
            << socketPath.Quote());

        oldReq.MutableClientProfile()->CopyFrom(newReq.GetClientProfile());
    }

    // CLOUD-98154
    if (oldReq.GetDeviceName() != newReq.GetDeviceName()) {
        STORAGE_WARN("Modified DeviceName will be ignored for endpoint: "
            << socketPath.Quote());

        oldReq.SetDeviceName(newReq.GetDeviceName());
    }

    if (newReq.GetUseFreeNbdDeviceFile() && oldReq.GetNbdDeviceFile()) {
        newReq.SetNbdDeviceFile(oldReq.GetNbdDeviceFile());
    }

    if (CompareRequests(newReq, oldReq)) {
        return MakeError(S_ALREADY, TStringBuilder()
            << "endpoint " << socketPath.Quote()
            << " has already been started");
    }

    oldReq.SetVolumeAccessMode(newReq.GetVolumeAccessMode());
    oldReq.SetVolumeMountMode(newReq.GetVolumeMountMode());
    oldReq.SetMountSeqNumber(newReq.GetMountSeqNumber());

    if (!CompareRequests(newReq, oldReq)) {
        return MakeError(E_INVALID_STATE, TStringBuilder()
            << "endpoint " << socketPath.Quote()
            << " has already been started with other args");
    }

    auto future = SessionManager->AlterSession(
        ctx,
        socketPath,
        newReq.GetVolumeAccessMode(),
        newReq.GetVolumeMountMode(),
        newReq.GetMountSeqNumber(),
        newReq.GetHeaders());

    if (const auto& error = Executor->WaitFor(future); HasError(error)) {
        return error;
    }

    auto getSessionFuture =
        SessionManager->GetSession(ctx, socketPath, newReq.GetHeaders());

    const auto& [sessionInfo, error] = Executor->WaitFor(getSessionFuture);

    if (HasError(error)) {
        return error;
    }

    auto listenerIt = EndpointListeners.find(oldReq.GetIpcType());
    STORAGE_VERIFY(
        listenerIt != EndpointListeners.end(),
        TWellKnownEntityTypes::ENDPOINT,
        socketPath);

    auto& listener = listenerIt->second;

    auto alterFuture = listener->AlterEndpoint(
        oldReq,
        sessionInfo.Volume,
        sessionInfo.Session);

    return Executor->WaitFor(alterFuture);
}

// FIXME: doesn't restart additional NBD socket opened for gPRC endpoints
NProto::TError TEndpointManager::RestartListenerEndpoint(
    TCallContextPtr ctx,
    const NProto::TStartEndpointRequest& request)
{
    STORAGE_INFO("Restart listener endpoint: " << request);

    auto sessionFuture = SessionManager->GetSession(
        ctx,
        request.GetUnixSocketPath(),
        request.GetHeaders());

    auto [sessionInfo, error] = Executor->WaitFor(sessionFuture);
    if (HasError(error)) {
        return error;
    }

    auto listenerIt = EndpointListeners.find(request.GetIpcType());
    STORAGE_VERIFY(
        listenerIt != EndpointListeners.end(),
        TWellKnownEntityTypes::ENDPOINT,
        request.GetUnixSocketPath());

    auto& listener = listenerIt->second;

    auto future = listener->StopEndpoint(request.GetUnixSocketPath());
    error = Executor->WaitFor(future);
    if (HasError(error)) {
        STORAGE_ERROR("Failed to stop endpoint while restarting it: "
            << FormatError(error));
    }

    future = listener->StartEndpoint(
        request,
        sessionInfo.Volume,
        sessionInfo.Session);
    error = Executor->WaitFor(future);
    if (HasError(error)) {
        STORAGE_ERROR("Failed to start endpoint while recreating it: "
            << FormatError(error));
    }

    return error;
}

NProto::TStopEndpointResponse TEndpointManager::DoStopEndpoint(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TStopEndpointRequest> request)
{
    auto socketPath = request->GetUnixSocketPath();
    if (IsEndpointRestoring(socketPath)) {
        return TErrorResponse(E_REJECTED, "endpoint is restoring now");
    }

    auto promise = AddProcessingSocket<TStopEndpointMethod>(*request);
    if (promise.HasValue()) {
        return promise.ExtractValue();
    }

    auto response = StopEndpointImpl(std::move(ctx), std::move(request));
    promise.SetValue(response);

    RemoveProcessingSocket(socketPath);
    return response;
}

NProto::TStopEndpointResponse TEndpointManager::StopEndpointFallback(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TStopEndpointRequest> request)
{
    Y_ABORT_UNLESS(request->GetDiskId() && request->GetHeaders().GetClientId());
    const auto& socketPath = request->GetUnixSocketPath();

    auto removeClientRequest =
        std::make_shared<NProto::TRemoveVolumeClientRequest>();
    removeClientRequest->SetDiskId(request->GetDiskId());
    removeClientRequest->MutableHeaders()->SetClientId(
        request->GetHeaders().GetClientId());

    auto removeClientFuture =
        Service->RemoveVolumeClient(ctx, std::move(removeClientRequest));
    const auto& removeClientResponse = Executor->WaitFor(removeClientFuture);
    const auto& error = removeClientResponse.GetError();

    if (HasError(error) &&
        error.GetCode() !=
            MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist))
    {
        return TErrorResponse(removeClientResponse.GetError());
    }

    // The vhost server deletes socket files when an endpoint starts or stops.
    // We don't have a vhost server here, so we need to delete the socket file
    // manually. Compute assumes we should do this.
    TFsPath(socketPath).DeleteIfExists();

    return {};
}

NProto::TStopEndpointResponse TEndpointManager::StopEndpointImpl(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TStopEndpointRequest> request)
{
    const auto& socketPath = request->GetUnixSocketPath();

    auto it = Endpoints.find(socketPath);
    if (it == Endpoints.end()) {
        if (request->GetDiskId() && request->GetHeaders().GetClientId()) {
            return StopEndpointFallback(ctx, request);
        }

        return TErrorResponse(S_FALSE, TStringBuilder()
            << "endpoint " << socketPath.Quote()
            << " hasn't been started yet");
    }

    auto endpoint = std::move(it->second);
    Endpoints.erase(it);
    if (auto c =
            ServerStats->GetEndpointCounter(endpoint->Request->GetIpcType()))
    {
        c->Dec();
    }

    if (endpoint->Device) { // could have been already stopped by an error handler
        auto stopFuture = endpoint->Device->Stop(true);
        const auto& stopError = Executor->WaitFor(stopFuture);
        if (HasError(stopError)) {
            return TErrorResponse(stopError);
        }
    }
    NbdErrorHandlerMap->Erase(socketPath);
    ReleaseNbdDevice(endpoint->Request->GetNbdDeviceFile(), false);
    CloseAllEndpointSockets(*endpoint->Request);
    RemoveSession(std::move(ctx), *request);

    if (auto error = EndpointStorage->RemoveEndpoint(socketPath);
        HasError(error) && !HasProtoFlag(error.GetFlags(), NProto::EF_SILENT))
    {
        STORAGE_ERROR(
            "Failed to remove endpoint from storage: " << FormatError(error));
    }

    return {};
}

NProto::TListEndpointsResponse TEndpointManager::DoListEndpoints(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TListEndpointsRequest> request)
{
    Y_UNUSED(ctx);
    Y_UNUSED(request);

    NProto::TListEndpointsResponse response;
    auto& responseEndpoints = *response.MutableEndpoints();
    responseEndpoints.Reserve(Endpoints.size());

    for (const auto& [_, endpoint]: Endpoints) {
        responseEndpoints.Add()->CopyFrom(*endpoint->Request);
    }

    response.SetEndpointsWereRestored(
        AtomicGet(RestoringStage) == Completed);
    return response;
}

NProto::TKickEndpointResponse TEndpointManager::DoKickEndpoint(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TKickEndpointRequest> request)
{
    auto keyringId = ToString(request->GetKeyringId());
    auto [str, error] = EndpointStorage->GetEndpoint(keyringId);
    if (HasError(error)) {
        return TErrorResponse(error);
    }

    auto startReq = DeserializeEndpoint<NProto::TStartEndpointRequest>(str);
    if (!startReq) {
        return TErrorResponse(E_INVALID_STATE, TStringBuilder()
            << "Failed to deserialize endpoint with key "
            << request->GetKeyringId());
    }

    startReq->MutableHeaders()->MergeFrom(request->GetHeaders());
    startReq->SetPersistent(false);

    STORAGE_INFO("Kick StartEndpoint request: " << *startReq);
    auto response = DoStartEndpoint(
        std::move(ctx),
        std::move(startReq));

    return TErrorResponse(response.GetError());
}

NProto::TListKeyringsResponse TEndpointManager::DoListKeyrings(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TListKeyringsRequest> request)
{
    Y_UNUSED(ctx);
    Y_UNUSED(request);

    auto [storedIds, error] = EndpointStorage->GetEndpointIds();
    if (HasError(error)) {
        return TErrorResponse(error);
    }

    NProto::TListKeyringsResponse response;
    auto& endpoints = *response.MutableEndpoints();

    endpoints.Reserve(storedIds.size());

    for (const auto& keyringId: storedIds) {
        auto& endpoint = *endpoints.Add();
        endpoint.SetKeyringId(keyringId);

        auto [str, error] = EndpointStorage->GetEndpoint(keyringId);
        if (HasError(error)
                && !HasProtoFlag(error.GetFlags(), NProto::EF_SILENT))
        {
            STORAGE_WARN("Failed to get endpoint from storage, ID: "
                << keyringId << ", error: " << FormatError(error));
            continue;
        }

        auto req = DeserializeEndpoint<NProto::TStartEndpointRequest>(str);
        if (!req) {
            STORAGE_WARN("Failed to deserialize endpoint from storage, ID: "
                << keyringId);
            continue;
        }

        endpoint.MutableRequest()->CopyFrom(*req);
    }

    return response;
}

NProto::TDescribeEndpointResponse TEndpointManager::DoDescribeEndpoint(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TDescribeEndpointRequest> request)
{
    Y_UNUSED(ctx);

    auto socketPath = request->GetUnixSocketPath();
    if (IsEndpointRestoring(socketPath)) {
        return TErrorResponse(E_REJECTED, "endpoint is restoring now");
    }

    NProto::TDescribeEndpointResponse response;

    auto [profile, err] = SessionManager->GetProfile(socketPath);
    if (HasError(err)) {
        response.MutableError()->CopyFrom(err);
    } else {
        response.MutablePerformanceProfile()->CopyFrom(profile);
    }

    return response;
}

NProto::TRefreshEndpointResponse TEndpointManager::DoRefreshEndpoint(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TRefreshEndpointRequest> request)
{
    auto socketPath = request->GetUnixSocketPath();
    if (IsEndpointRestoring(socketPath)) {
        return TErrorResponse(E_REJECTED, "endpoint is restoring now");
    }

    auto promise = AddProcessingSocket<TRefreshEndpointMethod>(*request);
    if (promise.HasValue()) {
        return promise.ExtractValue();
    }

    auto response = RefreshEndpointImpl(std::move(ctx), std::move(request));
    promise.SetValue(response);

    RemoveProcessingSocket(socketPath);
    return response;
}

NProto::TRefreshEndpointResponse TEndpointManager::RefreshEndpointImpl(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TRefreshEndpointRequest> request)
{
    const auto& socketPath = request->GetUnixSocketPath();
    const auto& headers = request->GetHeaders();

    auto it = Endpoints.find(socketPath);
    if (it == Endpoints.end()) {
        return TErrorResponse(S_FALSE, TStringBuilder()
            << "endpoint " << socketPath.Quote() << " not started");
    }

    auto ipcType = it->second->Request->GetIpcType();
    auto listenerIt = EndpointListeners.find(ipcType);
    STORAGE_VERIFY(
        listenerIt != EndpointListeners.end(),
        TWellKnownEntityTypes::ENDPOINT,
        socketPath);
    const auto& listener = listenerIt->second;

    auto future = SessionManager->GetSession(std::move(ctx), socketPath, headers);
    const auto& [sessionInfo, getSessionError] = Executor->WaitFor(future);

    if (HasError(getSessionError)) {
        return TErrorResponse(getSessionError);
    }

    auto error = it->second->Device->Resize(
        sessionInfo.Volume.GetBlocksCount() *
        sessionInfo.Volume.GetBlockSize()).GetValueSync();
    if (HasError(error)) {
        return TErrorResponse(error);
    }

    const auto refreshError = listener->RefreshEndpoint(socketPath, sessionInfo.Volume);
    return TErrorResponse(refreshError);
}

void TEndpointManager::ProcessException(
    std::shared_ptr<TExceptionContext> context)
{
    auto endpoint = context->Endpoint.lock();
    if (!endpoint) {
        STORAGE_WARN("endpoint is down");
        return;
    }

    auto prefix = TStringBuilder()
        << "[socket=" << endpoint->Request->GetUnixSocketPath()
        << " device=" << endpoint->Request->GetNbdDeviceFile() << "] ";

    ProcessException(std::move(context), std::move(prefix));
}

void TEndpointManager::ProcessException(
    std::shared_ptr<TExceptionContext> context,
    TString prefix)
{
    auto deadline =
        TInstant::Now() + context->BackoffProvider.GetDelayAndIncrease();

    auto func = [weakSelf = weak_from_this(), context = std::move(context)]() {
        if (auto self = weakSelf.lock()) {
            self->DoProcessException(std::move(context));
        }
    };

    STORAGE_INFO(prefix << "schedule restart at " << deadline);
    Scheduler->Schedule(Executor.get(), deadline, std::move(func));
}

void TEndpointManager::DoProcessException(
    std::shared_ptr<TExceptionContext> context)
{
    auto endpoint = context->Endpoint.lock();
    if (!endpoint) {
        STORAGE_WARN("endpoint is down, cancel restart");
        return;
    }

    auto prefix = TStringBuilder()
        << "[socket=" << endpoint->Request->GetUnixSocketPath()
        << " device=" << endpoint->Request->GetNbdDeviceFile() << "] ";

    auto state = ProcessingSockets.find(endpoint->Request->GetUnixSocketPath());
    if (state != ProcessingSockets.end() &&
        std::holds_alternative<TRequestState<TStopEndpointMethod>>(state->second))
    {
        STORAGE_WARN(prefix << "endpoint is stopping, cancel restart");
        return;
    }

    auto session = endpoint->Session.lock();
    if (!session) {
        STORAGE_WARN(prefix << "session is down, cancel restart");
        return;
    }

    if (endpoint->Generation != context->Generation) {
        STORAGE_WARN(
            prefix << "generation mismatch (" << endpoint->Generation
                   << " != " << context->Generation << "), cancel restart");
        return;
    }
    endpoint->Generation++;

    STORAGE_INFO(prefix << "restart endpoint");

    bool hasDevice =
        endpoint->Request->HasNbdDeviceFile() &&
        endpoint->Request->GetNbdDeviceFile() &&
        endpoint->Request->GetPersistent();

    if (hasDevice && endpoint->Device) {
        STORAGE_INFO(prefix << "stop device");
        auto future = endpoint->Device->Stop(false);
        auto error = Executor->WaitFor(future);
        if (HasError(error)) {
            STORAGE_ERROR(
                prefix << "failed to stop device: " << error.GetMessage());
        }
        endpoint->Device.reset();
    }

    STORAGE_INFO(prefix << "close socket");
    CloseAllEndpointSockets(*endpoint->Request);

    auto socketPath = endpoint->Request->GetUnixSocketPath();

    STORAGE_INFO(prefix << "update error handler");
    NbdErrorHandlerMap->Erase(socketPath);
    NbdErrorHandlerMap->Emplace(
        socketPath,
        std::make_shared<TErrorHandler>(weak_from_this(), endpoint));

    STORAGE_INFO(prefix << "open socket");
    auto error = OpenAllEndpointSockets(
        *endpoint->Request,
        TSessionInfo(endpoint->Volume, session));
    if (HasError(error)) {
        STORAGE_ERROR(
            prefix << "failed to open socket: " << error.GetMessage());
        context->Generation++;
        ProcessException(std::move(context), std::move(prefix));
        return;
    }

    if (hasDevice) {
        STORAGE_INFO(prefix << "start device");
        auto device = NbdDeviceFactory->Create(
            TNetworkAddress(TUnixSocketPath(socketPath)),
            endpoint->Request->GetNbdDeviceFile(),
            endpoint->Volume.GetBlocksCount(),
            endpoint->Volume.GetBlockSize());
        auto startDeviceFuture = device->Start();
        error = Executor->WaitFor(startDeviceFuture);
        if (HasError(error)) {
            STORAGE_ERROR(
                prefix << "failed to start device: " << error.GetMessage());
            context->Generation++;
            ProcessException(std::move(context), std::move(prefix));
            return;
        }
        endpoint->Device.swap(device);
    }
}

// opens secondary NBD socket for gRPC endpoints
NProto::TError TEndpointManager::OpenAllEndpointSockets(
    const NProto::TStartEndpointRequest& request,
    const TSessionInfo& sessionInfo)
{
    auto error = OpenEndpointSocket(request, sessionInfo);
    if (HasError(error)) {
        return error;
    }

    auto nbdRequest = CreateNbdStartEndpointRequest(request);
    if (nbdRequest) {
        STORAGE_INFO("Start additional endpoint: " << *nbdRequest);
        auto error = OpenEndpointSocket(*nbdRequest, sessionInfo);

        if (HasError(error)) {
            CloseEndpointSocket(request);
            return error;
        }
    }
    return {};
}

NProto::TError TEndpointManager::OpenEndpointSocket(
    const NProto::TStartEndpointRequest& request,
    const TSessionInfo& sessionInfo)
{
    auto ipcType = request.GetIpcType();
    auto listenerIt = EndpointListeners.find(ipcType);
    if (listenerIt == EndpointListeners.end()) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "unsupported endpoint type: " << static_cast<ui32>(ipcType));
    }
    auto listener = listenerIt->second;

    if (request.GetUnixSocketPath().size() > UnixSocketPathLengthLimit) {
        return MakeError(E_ARGUMENT, TStringBuilder()
            << "Length of socket path should not be more than "
            << UnixSocketPathLengthLimit
            << ", socket: " << request.GetUnixSocketPath());
    }

    auto future = listener->StartEndpoint(
        request,
        sessionInfo.Volume,
        sessionInfo.Session);

    return Executor->WaitFor(future);
}

// closes additional NBD socket opened for gRPC endpoints
void TEndpointManager::CloseAllEndpointSockets(
    const NProto::TStartEndpointRequest& request)
{
    CloseEndpointSocket(request);

    auto nbdRequest = CreateNbdStartEndpointRequest(request);
    if (nbdRequest) {
        STORAGE_INFO("Stop additional endpoint: "
            << nbdRequest->GetUnixSocketPath().Quote());
        CloseEndpointSocket(*nbdRequest);
    }
}

void TEndpointManager::CloseEndpointSocket(
    const NProto::TStartEndpointRequest& request)
{
    auto ipcType = request.GetIpcType();
    const auto& socketPath = request.GetUnixSocketPath();

    auto listenerIt = EndpointListeners.find(ipcType);
    STORAGE_VERIFY(
        listenerIt != EndpointListeners.end(),
        TWellKnownEntityTypes::ENDPOINT,
        socketPath);
    const auto& listener = listenerIt->second;

    auto future = listener->StopEndpoint(socketPath);
    if (const auto& error = Executor->WaitFor(future); HasError(error)) {
        STORAGE_ERROR("Failed to close socket " << socketPath.Quote()
            << ", error: " << FormatError(error));
    }
}

using TStartEndpointRequestPtr = std::shared_ptr<NProto::TStartEndpointRequest>;

TStartEndpointRequestPtr TEndpointManager::CreateNbdStartEndpointRequest(
    const NProto::TStartEndpointRequest& request)
{
    if (request.GetIpcType() != NProto::IPC_GRPC || NbdSocketSuffix.empty()) {
        return nullptr;
    }

    auto socketPath = request.GetUnixSocketPath() + NbdSocketSuffix;

    auto nbdRequest = std::make_shared<NProto::TStartEndpointRequest>(request);
    nbdRequest->SetIpcType(NProto::IPC_NBD);
    nbdRequest->SetUnixSocketPath(socketPath);
    nbdRequest->SetUnalignedRequestsDisabled(true);
    nbdRequest->SetSendNbdMinBlockSize(true);
    return nbdRequest;
}

NProto::TError TEndpointManager::DoSwitchEndpoint(
    TCallContextPtr ctx,
    std::shared_ptr<TSwitchEndpointRequest> request)
{
    const auto& diskId = request->GetDiskId();

    auto it = FindIf(Endpoints, [&] (auto& v) {
        const auto& [_, endpoint] = v;
        return endpoint->Request->GetDiskId() == diskId
            && endpoint->Request->GetIpcType() == NProto::IPC_VHOST;
    });

    if (it == Endpoints.end()) {
        return MakeError(S_OK);
    }

    auto socketPath = it->first;
    if (IsEndpointRestoring(socketPath)) {
        return MakeError(E_REJECTED, "endpoint is restoring now");
    }

    request->SetUnixSocketPath(socketPath);

    auto promise = AddProcessingSocket<TSwitchEndpointMethod>(*request);
    if (promise.HasValue()) {
        return promise.ExtractValue();
    }

    auto response = SwitchEndpointImpl(std::move(ctx), std::move(request));
    promise.SetValue(response);

    RemoveProcessingSocket(socketPath);
    return response;
}

NProto::TError TEndpointManager::SwitchEndpointImpl(
    TCallContextPtr ctx,
    std::shared_ptr<TSwitchEndpointRequest> request)
{
    const auto& socketPath = request->GetUnixSocketPath();

    auto it = Endpoints.find(socketPath);
    if (it == Endpoints.end()) {
        return MakeError(S_FALSE, TStringBuilder()
            << "endpoint " << socketPath.Quote() << " not started");
    }

    auto startRequest = it->second->Request;
    auto listenerIt = EndpointListeners.find(startRequest->GetIpcType());
    STORAGE_VERIFY(
        listenerIt != EndpointListeners.end(),
        TWellKnownEntityTypes::ENDPOINT,
        socketPath);
    IEndpointListenerPtr listener = listenerIt->second;

    auto getSessionFuture = SessionManager->GetSession(
        std::move(ctx),
        startRequest->GetUnixSocketPath(),
        startRequest->GetHeaders());

    const auto& [sessionInfo, getSessionError] = Executor->WaitFor(getSessionFuture);
    if (HasError(getSessionError)) {
        return getSessionError;
    }

    STORAGE_INFO("Switching endpoint"
        << ", reason=" << request->GetReason()
        << ", volume=" << sessionInfo.Volume.GetDiskId()
        << ", IsFastPathEnabled=" << sessionInfo.Volume.GetIsFastPathEnabled()
        << ", Migrations=" << sessionInfo.Volume.GetMigrations().size());

    auto switchFuture = listener->SwitchEndpoint(
        *startRequest,
        sessionInfo.Volume,
        sessionInfo.Session);

    const auto& switchError = Executor->WaitFor(switchFuture);
    if (HasError(switchError)) {
        ReportEndpointSwitchFailure(TStringBuilder()
            << "Failed to switch endpoint for volume "
            << sessionInfo.Volume.GetDiskId()
            << ", " << switchError.GetMessage());
    }

    return switchError;
}

TResultOrError<NBD::IDevicePtr> TEndpointManager::StartNbdDevice(
    std::shared_ptr<NProto::TStartEndpointRequest> request,
    bool restoring,
    const NProto::TVolume& volume)
{
    if (request->GetIpcType() != NProto::IPC_NBD) {
        return NBD::CreateDeviceStub();
    }

    if (request->HasUseFreeNbdDeviceFile() &&
        request->GetUseFreeNbdDeviceFile())
    {
        if (restoring) {
            return MakeError(E_ARGUMENT,
                "Forbidden 'FreeNbdDeviceFile' flag in restoring endpoints");
        }

        if (!AutomaticNbdDeviceManagement) {
            auto nbdDevice = NbdDeviceManager.GetFreeDevice();
            request->SetUseFreeNbdDeviceFile(false);
            request->SetNbdDeviceFile(nbdDevice);
        }
    } else if (!request->HasNbdDeviceFile() || !request->GetNbdDeviceFile()) {
        return NBD::CreateDeviceStub();
    }

    if (!restoring) {
        if (!request->GetPersistent()) {
            return MakeError(E_ARGUMENT,
                "Only persistent endpoints can connect to nbd device");
        }

        if (!AutomaticNbdDeviceManagement) {
            const auto& nbdDevice = request->GetNbdDeviceFile();
            auto error = NbdDeviceManager.AcquireDevice(nbdDevice);
            if (HasError(error)) {
                return error;
            }
        }
    }

    NBD::IDevicePtr device;
    try {
        TNetworkAddress address(TUnixSocketPath(request->GetUnixSocketPath()));

        if (request->HasUseFreeNbdDeviceFile() &&
            request->GetUseFreeNbdDeviceFile())
        {
            device = NbdDeviceFactory->CreateFree(
                address,
                NbdDevicePrefix,
                volume.GetBlocksCount(),
                volume.GetBlockSize());
        } else {
            device = NbdDeviceFactory->Create(
                address,
                request->GetNbdDeviceFile(),
                volume.GetBlocksCount(),
                volume.GetBlockSize());
        }

        auto startFuture = device->Start();
        const auto& startError = Executor->WaitFor(startFuture);
        if (HasError(startError)) {
            return startError;
        }

        if (request->HasUseFreeNbdDeviceFile() &&
            request->GetUseFreeNbdDeviceFile())
        {
            request->SetUseFreeNbdDeviceFile(false);
            request->SetNbdDeviceFile(device->GetPath());
        }
    } catch (...) {
        ReleaseNbdDevice(request->GetNbdDeviceFile(), restoring);
        return MakeError(E_FAIL, CurrentExceptionMessage());
    }

    return device;
}

TFuture<NProto::TError> TEndpointManager::SwitchEndpointIfNeeded(
    const TString& diskId,
    const TString& reason)
{
    auto ctx = MakeIntrusive<TCallContext>();
    auto request = std::make_shared<TSwitchEndpointRequest>();
    request->SetDiskId(diskId);
    request->SetReason(reason);

    return SwitchEndpoint(std::move(ctx), std::move(request));
}

TFuture<void> TEndpointManager::DoRestoreEndpoints()
{
    auto [endpointIds, error] = EndpointStorage->GetEndpointIds();
    if (HasError(error) && !HasProtoFlag(error.GetFlags(), NProto::EF_SILENT)) {
        ReportEndpointRestoringError(
            TStringBuilder()
            << "Failed to get endpoints from storage: " << FormatError(error));
        return MakeFuture();
    }

    STORAGE_INFO("Found " << endpointIds.size() << " endpoints in storage");

    TString clientId = CreateGuidAsString() + "_bootstrap";

    TVector<TFuture<void>> futures;

    for (const auto& endpointId: endpointIds) {
        auto [str, error] = EndpointStorage->GetEndpoint(endpointId);
        if (HasError(error)
                && !HasProtoFlag(error.GetFlags(), NProto::EF_SILENT))
        {
            // NBS-3678
            STORAGE_WARN("Failed to restore endpoint. ID: " << endpointId
                << ", error: " << FormatError(error));
            continue;
        }

        auto request = DeserializeEndpoint<NProto::TStartEndpointRequest>(str);

        if (!request) {
            ReportEndpointRestoringError(
                TStringBuilder()
                << "Failed to deserialize request. ID: " << endpointId
                << ", error: " << FormatError(error));
            continue;
        }

        if (request->HasNbdDeviceFile() && request->GetNbdDeviceFile()) {
            const auto& nbdDevice = request->GetNbdDeviceFile();

            if (!AutomaticNbdDeviceManagement) {
                auto error = NbdDeviceManager.AcquireDevice(nbdDevice);

                if (HasError(error)) {
                    ReportEndpointRestoringError(
                        TStringBuilder()
                        << "Failed to acquire nbd device, endpoint: "
                        << request->GetUnixSocketPath().Quote()
                        << ", DiskId: " << request->GetDiskId().Quote()
                        << ", error: " << FormatError(error));
                    continue;
                }
            }
        }

        auto& headers = *request->MutableHeaders();

        if (!headers.GetClientId()) {
            headers.SetClientId(clientId);
        }

        auto requestId = headers.GetRequestId();
        if (!requestId) {
            headers.SetRequestId(CreateRequestId());
        }

        auto socketPath = request->GetUnixSocketPath();
        RestoringEndpoints.insert(socketPath);

        auto future = RestoringClient->StartEndpoint(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request));

        auto weakPtr = weak_from_this();
        future.Subscribe([weakPtr, socketPath, endpointId] (const auto& f) {
            const auto& response = f.GetValue();
            if (HasError(response)) {
                ReportEndpointRestoringError(
                    TStringBuilder()
                    << "Endpoint restoring error occurred for endpoint ID: "
                    << endpointId << ", socket path: " << socketPath);
            }

            if (auto ptr = weakPtr.lock()) {
                ptr->HandleRestoredEndpoint(
                    socketPath,
                    endpointId,
                    response.GetError());
            }
        });

        futures.push_back(future.IgnoreResult());
    }

    return WaitAll(futures);
}

void TEndpointManager::HandleRestoredEndpoint(
    const TString& socketPath,
    const TString& endpointId,
    const NProto::TError& error)
{
    if (HasError(error)) {
        STORAGE_ERROR("Failed to start endpoint " << socketPath.Quote()
            << ", error:" << FormatError(error));
        if (error.GetCode() ==
            MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist))
        {
            STORAGE_INFO(
                "Remove endpoint for non-existing volume. endpoint id: "
                << endpointId.Quote());
            EndpointStorage->RemoveEndpoint(endpointId);
        }
    }

    Executor->Execute([socketPath, weakSelf = weak_from_this()] () mutable {
        if (auto self = weakSelf.lock()) {
            self->RestoringEndpoints.erase(socketPath);
        }
    });
}

void TEndpointManager::ReleaseNbdDevice(const TString& device, bool restoring)
{
    if (restoring) {
        return;
    }

    if (!AutomaticNbdDeviceManagement) {
        NbdDeviceManager.ReleaseDevice(device);
    }
}

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TStartEndpointResponse> TRestoringClient::StartEndpoint(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TStartEndpointRequest> req)
{
    return EndpointManager.RestoreSingleEndpoint(
        std::move(ctx),
        std::move(req));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointManagerPtr CreateEndpointManager(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    IEndpointEventProxyPtr eventProxy,
    ISessionManagerPtr sessionManager,
    IEndpointStoragePtr endpointStorage,
    THashMap<NProto::EClientIpcType, IEndpointListenerPtr> listeners,
    NBD::IDeviceFactoryPtr nbdDeviceFactory,
    NBD::IErrorHandlerMapPtr nbdErrorHandlerMap,
    IBlockStorePtr service,
    TEndpointManagerOptions options)
{
    auto manager = std::make_shared<TEndpointManager>(
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(requestStats),
        std::move(volumeStats),
        std::move(serverStats),
        std::move(executor),
        std::move(sessionManager),
        std::move(endpointStorage),
        std::move(listeners),
        std::move(nbdDeviceFactory),
        std::move(nbdErrorHandlerMap),
        std::move(service),
        std::move(options));
    eventProxy->Register(manager);
    return manager;
}

bool AreSameStartEndpointRequests(
    const NProto::TStartEndpointRequest& left,
    const NProto::TStartEndpointRequest& right)
{
    return CompareRequests(left, right);
}

}   // namespace NCloud::NBlockStore::NServer
