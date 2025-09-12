#include "session.h"

#include "config.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/ptr.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TReadBlocksLocalMethod
{
    static constexpr EBlockStoreRequest RequestType = EBlockStoreRequest::ReadBlocks;

    using TRequest = std::shared_ptr<NProto::TReadBlocksLocalRequest>;
    using TResponse = NProto::TReadBlocksLocalResponse;
};

struct TWriteBlocksMethod
{
    static constexpr EBlockStoreRequest RequestType = EBlockStoreRequest::WriteBlocks;

    using TRequest = std::shared_ptr<NProto::TWriteBlocksLocalRequest>;
    using TResponse = NProto::TWriteBlocksLocalResponse;
};

struct TZeroBlocksMethod
{
    static constexpr EBlockStoreRequest RequestType = EBlockStoreRequest::ZeroBlocks;

    using TRequest = std::shared_ptr<NProto::TZeroBlocksRequest>;
    using TResponse = NProto::TZeroBlocksResponse;
};

////////////////////////////////////////////////////////////////////////////////

enum class EMountState
{
    Uninitialized,
    MountCompleted,
    MountRequested,
    MountInProgress,
    UnmountInProgress
};

////////////////////////////////////////////////////////////////////////////////

struct TSessionInfo
{
    ui32 BlockSize = 0;
    ui64 BlocksCount = 0;
    TString SessionId;

    TMutex MountLock;
    EMountState MountState = EMountState::Uninitialized;

    TPromise<NProto::TMountVolumeResponse> MountResponse;
    TPromise<NProto::TUnmountVolumeResponse> UnmountResponse;

    TDuration RemountPeriod;
    ui64 RemountTimerEpoch = 0;
    TInstant RemountTime;

    TString ServiceVersion;

    NProto::THeaders MountHeaders;

    TVector<TMountChangeListener> ChangeListeners;

    void ResetState()
    {
        BlockSize = 0;
        BlocksCount = 0;
        SessionId = {};

        MountState = EMountState::Uninitialized;

        MountResponse = {};
        UnmountResponse = {};

        RemountPeriod = {};
        RemountTime = {};
        ++RemountTimerEpoch;

        MountHeaders.Clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSession final
    : public std::enable_shared_from_this<TSession>
    , public ISession
{
    enum class EMountKind
    {
        INITIAL,
        REMOUNT
    };

private:
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ILoggingServicePtr Logging;
    const IRequestStatsPtr RequestStats;
    const IVolumeStatsPtr VolumeStats;
    const IBlockStorePtr Client;
    const TClientAppConfigPtr Config;
    TSessionConfig SessionConfig;

    TLog Log;
    TSessionInfo SessionInfo;

public:
    TSession(
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging,
        IRequestStatsPtr requestStats,
        IVolumeStatsPtr volumeStats,
        IBlockStorePtr client,
        TClientAppConfigPtr config,
        const TSessionConfig& sessionConfig);

    ui32 GetMaxTransfer() const override;

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        NProto::EVolumeAccessMode accessMode,
        NProto::EVolumeMountMode mountMode,
        ui64 mountSeqNumber,
        TCallContextPtr callContext,
        const NProto::THeaders& headers) override;

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        const NProto::THeaders& headers) override;

    TFuture<NProto::TUnmountVolumeResponse> UnmountVolume(
        TCallContextPtr callContext,
        const NProto::THeaders& headers) override;

    TFuture<NProto::TMountVolumeResponse> EnsureVolumeMounted() override;

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override;

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override;

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override;

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override;

    TStorageBuffer AllocateBuffer(size_t bytesCount) override;

    void OnMountChange(TMountChangeListener fn) override;

    void ReportIOError() override;

private:
    void SendMountRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TMountVolumeRequest> request);
    std::shared_ptr<NProto::TMountVolumeRequest> PrepareMountRequest(
        const NProto::THeaders& headers,
        EMountKind mountKind = EMountKind::INITIAL) const;
    std::shared_ptr<NProto::TUnmountVolumeRequest> PrepareUnmountRequest(
        const NProto::THeaders& headers) const;

    void ProcessMountResponse(ui64 requestId, NProto::TMountVolumeResponse response);
    void ProcessUnmountResponse(ui64 requestId, NProto::TUnmountVolumeResponse response);

    void ForceVolumeRemount(const TString& sessionId);

    void ScheduleVolumeRemount(const NProto::TMountVolumeResponse& response);
    void RemountVolume(ui64 epoch);

    template <typename T>
    void HandleRequest(
        TCallContextPtr callContext,
        typename T::TRequest request,
        TPromise<typename T::TResponse> response);

    template <typename T>
    void HandleRequestAfterMount(
        TCallContextPtr callContext,
        typename T::TRequest request,
        const TFuture<NProto::TMountVolumeResponse>& mountResponse,
        TPromise<typename T::TResponse> response);

    template <typename T>
    void HandleResponse(
        TCallContextPtr callContext,
        typename T::TRequest request,
        const TString& sessionId,
        const TFuture<typename T::TResponse>& future,
        TPromise<typename T::TResponse> response);

    template <typename T>
    TFuture<typename T::TResponse> SendRequest(
        TCallContextPtr callContext,
        typename T::TRequest request);

    static void SetupRequestHeaders(
        const NProto::THeaders& in,
        NProto::THeaders& out)
    {
        if (const auto& idempotenceId = in.GetIdempotenceId()) {
            out.SetIdempotenceId(idempotenceId);
        }

        if (const auto& traceId = in.GetTraceId()) {
            out.SetTraceId(traceId);
        }

        if (const auto& requestId = in.GetRequestId()) {
            out.SetRequestId(requestId);
        }

        if (const auto& requestTimeout = in.GetRequestTimeout()) {
            out.SetRequestTimeout(requestTimeout);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TSession::TSession(
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging,
        IRequestStatsPtr requestStats,
        IVolumeStatsPtr volumeStats,
        IBlockStorePtr client,
        TClientAppConfigPtr config,
        const TSessionConfig& sessionConfig)
    : Timer(std::move(timer))
    , Scheduler(std::move(scheduler))
    , Logging(std::move(logging))
    , RequestStats(std::move(requestStats))
    , VolumeStats(std::move(volumeStats))
    , Client(std::move(client))
    , Config(std::move(config))
    , SessionConfig(sessionConfig)
    , Log(Logging->CreateLog("BLOCKSTORE_CLIENT"))
{}

ui32 TSession::GetMaxTransfer() const
{
    return Config->GetMaxRequestSize();
}

TFuture<NProto::TMountVolumeResponse> TSession::MountVolume(
    NProto::EVolumeAccessMode accessMode,
    NProto::EVolumeMountMode mountMode,
    ui64 mountSeqNumber,
    TCallContextPtr callContext,
    const NProto::THeaders& headers)
{
    std::shared_ptr<NProto::TMountVolumeRequest> request;
    TFuture<NProto::TMountVolumeResponse> response;

    with_lock (SessionInfo.MountLock) {
        if (SessionInfo.MountState == EMountState::MountInProgress) {
            auto weak_ptr = weak_from_this();
            return SessionInfo.MountResponse.GetFuture().Apply(
                [=, weak_ptr = std::move(weak_ptr)] (const auto& future) mutable {
                Y_UNUSED(future);
                if (auto p = weak_ptr.lock()) {
                    return p->MountVolume(
                        accessMode,
                        mountMode,
                        mountSeqNumber,
                        std::move(callContext),
                        headers);
                }
                return MakeFuture<NProto::TMountVolumeResponse>(
                    TErrorResponse(E_REJECTED, "Session is destroyed"));
            });
        }

        if (SessionInfo.MountState == EMountState::UnmountInProgress) {
            auto weak_ptr = weak_from_this();
            return SessionInfo.UnmountResponse.GetFuture().Apply(
                [=, weak_ptr = std::move(weak_ptr)] (const auto& future) mutable {
                Y_UNUSED(future);
                if (auto p = weak_ptr.lock()) {
                    return p->MountVolume(
                        accessMode,
                        mountMode,
                        mountSeqNumber,
                        std::move(callContext),
                        headers);
                }
                return MakeFuture<NProto::TMountVolumeResponse>(
                    TErrorResponse(E_REJECTED, "Session is destroyed"));
            });

        }

        SessionConfig.AccessMode = accessMode;
        SessionConfig.MountMode = mountMode;
        SessionConfig.MountSeqNumber = mountSeqNumber;

        SessionInfo.MountHeaders = headers;
        SessionInfo.MountState = EMountState::MountInProgress;
        SessionInfo.MountResponse = NewPromise<NProto::TMountVolumeResponse>();

        request = PrepareMountRequest(SessionInfo.MountHeaders);
        response = SessionInfo.MountResponse;
    }

    SendMountRequest(std::move(callContext), std::move(request));
    return response;
}

TFuture<NProto::TMountVolumeResponse> TSession::MountVolume(
    TCallContextPtr callContext,
    const NProto::THeaders& headers)
{
    std::shared_ptr<NProto::TMountVolumeRequest> request;
    TFuture<NProto::TMountVolumeResponse> response;

    with_lock (SessionInfo.MountLock) {
        if (SessionInfo.MountState == EMountState::MountInProgress) {
            auto weak_ptr = weak_from_this();
            return SessionInfo.MountResponse.GetFuture().Apply(
                [=, weak_ptr = std::move(weak_ptr)] (const auto& future) mutable{
                Y_UNUSED(future);
                if (auto p = weak_ptr.lock()) {
                    return p->MountVolume(std::move(callContext), headers);
                }
                return MakeFuture<NProto::TMountVolumeResponse>(
                    TErrorResponse(E_REJECTED, "Session is destroyed"));
            });
        }

        if (SessionInfo.MountState == EMountState::UnmountInProgress) {
            auto weak_ptr = weak_from_this();
            return SessionInfo.UnmountResponse.GetFuture().Apply(
                [=, weak_ptr = std::move(weak_ptr)] (const auto& future) mutable {
                Y_UNUSED(future);
                if (auto p = weak_ptr.lock()) {
                    return p->MountVolume(std::move(callContext), headers);
                }
                return MakeFuture<NProto::TMountVolumeResponse>(
                    TErrorResponse(E_REJECTED, "Session is destroyed"));
            });

        }

        SessionInfo.MountHeaders = headers;

        SessionInfo.MountState = EMountState::MountInProgress;
        SessionInfo.MountResponse = NewPromise<NProto::TMountVolumeResponse>();

        request = PrepareMountRequest(SessionInfo.MountHeaders);
        response = SessionInfo.MountResponse;
    }

    SendMountRequest(std::move(callContext), std::move(request));
    return response;
}

TFuture<NProto::TUnmountVolumeResponse> TSession::UnmountVolume(
    TCallContextPtr callContext,
    const NProto::THeaders& headers)
{
    std::shared_ptr<NProto::TUnmountVolumeRequest> request;
    TFuture<NProto::TUnmountVolumeResponse> response;

    with_lock (SessionInfo.MountLock) {
        if (SessionInfo.MountState == EMountState::Uninitialized) {
            return MakeFuture<NProto::TUnmountVolumeResponse>(
                TErrorResponse(S_ALREADY, "Volume is not mounted"));
        }

        if (SessionInfo.MountState == EMountState::MountInProgress) {
            auto weak_ptr = weak_from_this();
            return SessionInfo.MountResponse.GetFuture().Apply(
                [=, weak_ptr = std::move(weak_ptr)] (const auto& future) mutable {
                Y_UNUSED(future);
                if (auto p = weak_ptr.lock()) {
                    return p->UnmountVolume(std::move(callContext), headers);
                }
                return MakeFuture<NProto::TUnmountVolumeResponse>(
                    TErrorResponse(E_REJECTED, "Session is destroyed"));
            });
        }

        if (SessionInfo.MountState == EMountState::UnmountInProgress) {
            auto weak_ptr = weak_from_this();
            return SessionInfo.UnmountResponse.GetFuture().Apply(
                [=, weak_ptr = std::move(weak_ptr)] (const auto& future) mutable {
                Y_UNUSED(future);
                if (auto p = weak_ptr.lock()) {
                    return p->UnmountVolume(std::move(callContext), headers);
                }
                return MakeFuture<NProto::TUnmountVolumeResponse>(
                    TErrorResponse(E_REJECTED, "Session is destroyed"));
            });
        }

        SessionInfo.MountState = EMountState::UnmountInProgress;
        SessionInfo.UnmountResponse = NewPromise<NProto::TUnmountVolumeResponse>();

        request = PrepareUnmountRequest(headers);
        response = SessionInfo.UnmountResponse;
    }

    auto requestId = GetRequestId(*request);
    if (!callContext->RequestId) {
        callContext->RequestId = requestId;
    }

    STORAGE_INFO(
        TRequestInfo(
            EBlockStoreRequest::UnmountVolume,
            requestId,
            request->GetDiskId(),
            {},
            SessionConfig.InstanceId)
        << " submit request");

    auto weak_ptr = weak_from_this();
    Client->UnmountVolume(std::move(callContext), std::move(request))
        .Subscribe([=, weak_ptr = std::move(weak_ptr)] (const auto& future) {
            if (auto p = weak_ptr.lock()) {
                p->ProcessUnmountResponse(requestId, future.GetValue());
            }
        });

    return response;
}

TFuture<NProto::TReadBlocksLocalResponse> TSession::ReadBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
{
    request->SetDiskId(SessionConfig.DiskId);

    auto response = NewPromise<NProto::TReadBlocksLocalResponse>();
    HandleRequest<TReadBlocksLocalMethod>(
        std::move(callContext),
        std::move(request),
        response);
    return response;
}

TFuture<NProto::TWriteBlocksLocalResponse> TSession::WriteBlocksLocal(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    request->SetDiskId(SessionConfig.DiskId);

    auto response = NewPromise<NProto::TWriteBlocksLocalResponse>();
    HandleRequest<TWriteBlocksMethod>(
        std::move(callContext),
        std::move(request),
        response);
    return response;
}

TFuture<NProto::TZeroBlocksResponse> TSession::ZeroBlocks(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TZeroBlocksRequest> request)
{
    request->SetDiskId(SessionConfig.DiskId);

    auto response = NewPromise<NProto::TZeroBlocksResponse>();
    HandleRequest<TZeroBlocksMethod>(
        std::move(callContext),
        std::move(request),
        response);
    return response;
}

TFuture<NProto::TError> TSession::EraseDevice(NProto::EDeviceEraseMethod method)
{
    Y_UNUSED(method);
    return MakeFuture(MakeError(E_NOT_IMPLEMENTED));
}

TStorageBuffer TSession::AllocateBuffer(size_t bytesCount)
{
    return Client->AllocateBuffer(bytesCount);
}

void TSession::ReportIOError()
{}

void TSession::OnMountChange(TMountChangeListener fn)
{
    with_lock (SessionInfo.MountLock) {
        SessionInfo.ChangeListeners.emplace_back(std::move(fn));
    };
}

////////////////////////////////////////////////////////////////////////////////

void TSession::SendMountRequest(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TMountVolumeRequest> request)
{
    auto requestId = GetRequestId(*request);
    if (!callContext->RequestId) {
        callContext->RequestId = requestId;
    }

    bool readWrite = IsReadWriteMode(request->GetVolumeAccessMode());
    bool mountLocal = (request->GetVolumeMountMode() == NProto::VOLUME_MOUNT_LOCAL);

    STORAGE_INFO(
        TRequestInfo(
            EBlockStoreRequest::MountVolume,
            requestId,
            request->GetDiskId(),
            {},
            SessionConfig.InstanceId)
        << " submit request: "
        << (readWrite ? "read-write" : "read-only") << " access, "
        << (mountLocal ? "local" : "remote") << " mount");

    auto weak_ptr = weak_from_this();
    Client->MountVolume(std::move(callContext), std::move(request))
        .Subscribe([=, weak_ptr = std::move(weak_ptr)] (const auto& future) {
            if (auto p = weak_ptr.lock()) {
                p->ProcessMountResponse(requestId, future.GetValue());
            }
        });
}

std::shared_ptr<NProto::TMountVolumeRequest> TSession::PrepareMountRequest(
    const NProto::THeaders& headers,
    EMountKind mountKind) const
{
    auto request = std::make_shared<NProto::TMountVolumeRequest>();
    request->SetDiskId(SessionConfig.DiskId);
    request->SetVolumeAccessMode(SessionConfig.AccessMode);
    request->SetVolumeMountMode(SessionConfig.MountMode);
    request->SetMountFlags(SessionConfig.MountFlags);
    request->SetIpcType(SessionConfig.IpcType);
    request->SetInstanceId(SessionConfig.InstanceId);
    request->SetToken(SessionConfig.MountToken);
    request->SetClientVersionInfo(SessionConfig.ClientVersionInfo);
    request->SetMountSeqNumber(SessionConfig.MountSeqNumber);
    request->MutableEncryptionSpec()->CopyFrom(SessionConfig.EncryptionSpec);

    SetupRequestHeaders(headers, *request->MutableHeaders());

    if (mountKind == EMountKind::REMOUNT) {
        // Create new RequestId to make this MountRequest distinct from the
        // initial one
        request->MutableHeaders()->SetRequestId(CreateRequestId());
    }

    return request;
}

std::shared_ptr<NProto::TUnmountVolumeRequest> TSession::PrepareUnmountRequest(
    const NProto::THeaders& headers) const
{
    auto request = std::make_shared<NProto::TUnmountVolumeRequest>();
    request->SetDiskId(SessionConfig.DiskId);
    request->SetSessionId(SessionInfo.SessionId);

    SetupRequestHeaders(headers, *request->MutableHeaders());
    return request;
}

void TSession::ProcessMountResponse(
    ui64 requestId,
    NProto::TMountVolumeResponse response)
{
    TPromise<NProto::TMountVolumeResponse> promise;
    TVector<TMountChangeListener> changeListeners;

    with_lock (SessionInfo.MountLock) {
        if (!HasError(response) && SessionInfo.BlockSize != 0) {
            const auto& info = response.GetVolume();
            if (SessionInfo.BlockSize != info.GetBlockSize()) {
                auto& error = *response.MutableError();
                error.SetCode(E_INVALID_STATE);
                error.SetMessage("Volume geometry changed");
            }
        }

        if (HasError(response)) {
            STORAGE_ERROR(
                TRequestInfo(
                    EBlockStoreRequest::MountVolume,
                    requestId,
                    SessionConfig.DiskId,
                    {},
                    SessionConfig.InstanceId)
                << " request failed: " << FormatError(response.GetError()));

            // we do not want to hide mount failure from client,
            // so request will NOT be automatically retried.
            // just force remount on next request
            SessionInfo.MountState = EMountState::MountRequested;
            SessionInfo.RemountTime = {};
        } else {
            if (const auto& version = response.GetServiceVersionInfo()) {
                if (version != SessionInfo.ServiceVersion) {
                    STORAGE_INFO("NBS server version: " << version);
                    SessionInfo.ServiceVersion = version;
                } else {
                    STORAGE_DEBUG("NBS server version: " << version);
                }
            }

            STORAGE_TRACE(
                TRequestInfo(
                    EBlockStoreRequest::MountVolume,
                    requestId,
                    SessionConfig.DiskId,
                    {},
                    SessionConfig.InstanceId)
                << " complete request");

            SessionInfo.MountState = EMountState::MountCompleted;
            SessionInfo.BlockSize = response.GetVolume().GetBlockSize();
            SessionInfo.SessionId = response.GetSessionId();

            auto newBlocksCount = response.GetVolume().GetBlocksCount();
            if (SessionInfo.BlocksCount != newBlocksCount) {
                STORAGE_INFO(
                    TRequestInfo(
                        EBlockStoreRequest::MountVolume,
                        requestId,
                        SessionConfig.DiskId,
                        {},
                        SessionConfig.InstanceId)
                    << " new disk size: " << newBlocksCount);
                SessionInfo.BlocksCount = newBlocksCount;
            }

            ScheduleVolumeRemount(response);
        }

        promise = SessionInfo.MountResponse;
        changeListeners = std::move(SessionInfo.ChangeListeners);
    }

    for (const auto& listener: changeListeners) {
        listener(response);
    }

    // callbacks should be invoked outside of lock
    promise.SetValue(std::move(response));
}

void TSession::ProcessUnmountResponse(
    ui64 requestId,
    NProto::TUnmountVolumeResponse response)
{
    TPromise<NProto::TUnmountVolumeResponse> promise;

    with_lock (SessionInfo.MountLock) {
        promise = SessionInfo.UnmountResponse;

        if (HasError(response)) {
            STORAGE_ERROR(
                TRequestInfo(
                    EBlockStoreRequest::UnmountVolume,
                    requestId,
                    SessionConfig.DiskId,
                    {},
                    SessionConfig.InstanceId)
                << " request failed: " << FormatError(response.GetError()));
            SessionInfo.MountState = EMountState::MountCompleted;
        } else {
            STORAGE_TRACE(
                TRequestInfo(
                    EBlockStoreRequest::UnmountVolume,
                    requestId,
                    SessionConfig.DiskId,
                    {},
                    SessionConfig.InstanceId)
                << " complete request");
            SessionInfo.ResetState();
        }
    }

    // callbacks should be invoked outside of lock
    promise.SetValue(std::move(response));
}

void TSession::ForceVolumeRemount(const TString& sessionId)
{
    with_lock (SessionInfo.MountLock) {
        if (SessionInfo.MountState == EMountState::MountCompleted) {
            if (SessionInfo.SessionId == sessionId) {
                SessionInfo.MountState = EMountState::MountRequested;
            }
        }
    }
}

TFuture<NProto::TMountVolumeResponse> TSession::EnsureVolumeMounted()
{
    std::shared_ptr<NProto::TMountVolumeRequest> request;
    TFuture<NProto::TMountVolumeResponse> response;

    with_lock (SessionInfo.MountLock) {
        if (SessionInfo.MountState == EMountState::Uninitialized) {
            return MakeFuture<NProto::TMountVolumeResponse>(
                TErrorResponse(E_INVALID_STATE, "Volume is not mounted"));
        }

        if (SessionInfo.MountState == EMountState::MountRequested) {
            SessionInfo.MountState = EMountState::MountInProgress;
            SessionInfo.MountResponse = NewPromise<NProto::TMountVolumeResponse>();

            request = PrepareMountRequest(SessionInfo.MountHeaders, EMountKind::REMOUNT);
        }

        response = SessionInfo.MountResponse;
    }

    if (request) {
        auto requestId = GetRequestId(*request);
        auto callContext = MakeIntrusive<TCallContext>(requestId);

        STORAGE_WARN(
            TRequestInfo(
                EBlockStoreRequest::MountVolume,
                requestId,
                request->GetDiskId(),
                {},
                SessionConfig.InstanceId)
            << " submit request (remount)");

        auto weak_ptr = weak_from_this();
        Client->MountVolume(std::move(callContext), std::move(request))
            .Subscribe([=, weak_ptr = std::move(weak_ptr)] (const auto& future) {
                if (auto p = weak_ptr.lock()) {
                    p->ProcessMountResponse(requestId, future.GetValue());
                }
            });
    }

    return response;
}

void TSession::ScheduleVolumeRemount(
    const NProto::TMountVolumeResponse& response)
{
    // disable previous timer
    auto epoch = ++SessionInfo.RemountTimerEpoch;

    SessionInfo.RemountPeriod =
        TDuration::MilliSeconds(response.GetInactiveClientsTimeout());

    if (!SessionInfo.RemountPeriod) {
        // This means the service doesn't timeout inactive clients
        // so remount is not needed
        return;
    }
    SessionInfo.RemountTime = TInstant::Now() + SessionInfo.RemountPeriod;

    auto weak_ptr = weak_from_this();

    Scheduler->Schedule(
        Timer->Now() + SessionInfo.RemountPeriod,
        [=, weak_ptr = std::move(weak_ptr)] {
            if (auto p = weak_ptr.lock()) {
                p->RemountVolume(epoch);
            }
        });
}

void TSession::RemountVolume(ui64 epoch)
{
    std::shared_ptr<NProto::TMountVolumeRequest> request;

    with_lock (SessionInfo.MountLock) {
        if (epoch == SessionInfo.RemountTimerEpoch &&
            SessionInfo.MountState != EMountState::UnmountInProgress &&
            SessionInfo.MountState != EMountState::MountInProgress)
        {
            SessionInfo.MountState = EMountState::MountInProgress;
            SessionInfo.MountResponse = NewPromise<NProto::TMountVolumeResponse>();

            request = PrepareMountRequest(SessionInfo.MountHeaders, EMountKind::REMOUNT);

            if (SessionInfo.RemountTime) {
                auto delta = TInstant::Now() - SessionInfo.RemountTime;
                if (delta > Config->GetRemountDeadline()) {
                    STORAGE_WARN(
                        TRequestInfo(
                            EBlockStoreRequest::MountVolume,
                            {},
                            SessionConfig.DiskId,
                            ToString(request->MutableHeaders()->GetRequestId()),
                            SessionConfig.InstanceId)
                            << " late remount by timer: delta " << delta);
                }
            }
        }
    }

    if (request) {
        auto requestId = GetRequestId(*request);
        auto callContext = MakeIntrusive<TCallContext>(requestId);

        STORAGE_DEBUG(
            TRequestInfo(
                EBlockStoreRequest::MountVolume,
                requestId,
                request->GetDiskId(),
                {},
                SessionConfig.InstanceId)
            << " submit request (remount by timer)");

        auto weak_ptr = weak_from_this();
        Client->MountVolume(std::move(callContext), std::move(request))
            .Subscribe([=, weak_ptr = std::move(weak_ptr)] (const auto& future) {
                if (auto p = weak_ptr.lock()) {
                    p->ProcessMountResponse(requestId, future.GetValue());
                }
            });
    }
}

template <typename T>
void TSession::HandleRequest(
    TCallContextPtr callContext,
    typename T::TRequest request,
    TPromise<typename T::TResponse> response)
{
    NProto::TError error;

    try {
        auto mountResponse = EnsureVolumeMounted();
        if (mountResponse.HasValue()) {
            HandleRequestAfterMount<T>(
                std::move(callContext),
                std::move(request),
                mountResponse,
                response);
        } else {
            auto weak_ptr = weak_from_this();
            mountResponse.Subscribe(
                [=, weak_ptr = std::move(weak_ptr)] (const auto& future) mutable {
                    if (auto p = weak_ptr.lock()) {
                        p->HandleRequestAfterMount<T>(
                            std::move(callContext),
                            std::move(request),
                            future,
                            response);
                    }
                });
        }
        return;
    } catch (const TServiceError& e) {
        error.SetCode(e.GetCode());
        error.SetMessage(e.what());
    } catch (...) {
        error.SetCode(E_FAIL);
        error.SetMessage(CurrentExceptionMessage());
    }

    response.SetValue(TErrorResponse(error));
}

template <typename T>
void TSession::HandleRequestAfterMount(
    TCallContextPtr callContext,
    typename T::TRequest request,
    const TFuture<NProto::TMountVolumeResponse>& mountResponse,
    TPromise<typename T::TResponse> response)
{
    NProto::TError error;
    try {
        const auto& mountRes = mountResponse.GetValue();
        if (!HasError(mountRes)) {
            const auto& sessionId = mountRes.GetSessionId();
            request->SetSessionId(sessionId);

            auto weak_ptr = weak_from_this();
            SendRequest<T>(callContext, request).Subscribe(
                [=, weak_ptr = std::move(weak_ptr)] (const auto& future) mutable {
                    if (auto p = weak_ptr.lock()) {
                        p->HandleResponse<T>(
                            std::move(callContext),
                            std::move(request),
                            sessionId,
                            future,
                            response);
                    }
                });
            return;
        }
        error = mountRes.GetError();
    } catch (const TServiceError& e) {
        error.SetCode(e.GetCode());
        error.SetMessage(e.what());
    } catch (...) {
        error.SetCode(E_FAIL);
        error.SetMessage(CurrentExceptionMessage());
    }

    response.SetValue(TErrorResponse(error));
}

template <typename T>
void TSession::HandleResponse(
    TCallContextPtr callContext,
    typename T::TRequest request,
    const TString& sessionId,
    const TFuture<typename T::TResponse>& future,
    TPromise<typename T::TResponse> response)
{
    NProto::TError error;
    try {
        const auto& res = future.GetValue();
        if (!HasError(res)) {
            response.SetValue(res);
            return;
        }
        error = res.GetError();
    } catch (const TServiceError& e) {
        error.SetCode(e.GetCode());
        error.SetMessage(e.what());
    } catch (...) {
        error.SetCode(E_FAIL);
        error.SetMessage(CurrentExceptionMessage());
    }

    if (error.GetCode() == E_BS_INVALID_SESSION) {
        const auto errorKind = GetDiagnosticsErrorKind(error);
        const auto errorFlags = error.GetFlags();

        auto volumeInfo = VolumeStats->GetVolumeInfo(
            request->GetDiskId(),
            Config->GetClientId());
        if (volumeInfo) {
            volumeInfo->AddRetryStats(T::RequestType, errorKind, errorFlags);
        }

        RequestStats->AddRetryStats(
            VolumeStats->GetStorageMediaKind(request->GetDiskId()),
            T::RequestType,
            errorKind,
            errorFlags);

        // request should be retried after volume remounted
        const auto requestId = GetRequestId(*request);
        STORAGE_DEBUG(
            TRequestInfo(
                T::RequestType,
                requestId,
                request->GetDiskId(),
                {},
                SessionConfig.InstanceId)
            << " session deleted");

        ForceVolumeRemount(sessionId);
        HandleRequest<T>(std::move(callContext), std::move(request), response);
        return;
    }

    response.SetValue(TErrorResponse(error));
}

////////////////////////////////////////////////////////////////////////////////

template <>
TFuture<NProto::TReadBlocksLocalResponse>
TSession::SendRequest<TReadBlocksLocalMethod>(
    TCallContextPtr callContext,
    TReadBlocksLocalMethod::TRequest request)
{
    return Client->ReadBlocksLocal(std::move(callContext), std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

template <>
TFuture<NProto::TWriteBlocksResponse> TSession::SendRequest<TWriteBlocksMethod>(
    TCallContextPtr callContext,
    TWriteBlocksMethod::TRequest request)
{
    // prevent NBS-420
    request->MutableBlocks()->Clear();

    return Client->WriteBlocksLocal(
        std::move(callContext),
        std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

template <>
TFuture<NProto::TZeroBlocksResponse> TSession::SendRequest<TZeroBlocksMethod>(
    TCallContextPtr callContext,
    TZeroBlocksMethod::TRequest request)
{
    return Client->ZeroBlocks(
        std::move(callContext),
        std::move(request));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ISessionPtr CreateSession(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats,
    IBlockStorePtr client,
    TClientAppConfigPtr config,
    const TSessionConfig& sessionConfig)
{
    return std::make_shared<TSession>(
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(requestStats),
        std::move(volumeStats),
        std::move(client),
        std::move(config),
        sessionConfig);
}

}   // namespace NCloud::NBlockStore::NClient
