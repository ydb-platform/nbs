#include "rdma_target.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/protocol.h>
#include <cloud/blockstore/libs/rdma/iface/server.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service_local/rdma_protocol.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_client.h>
#include <cloud/blockstore/libs/storage/disk_agent/recent_blocks_tracker.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/threading/synchronized/synchronized.h>

#include <util/generic/hash.h>
#include <util/generic/list.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;
using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration LOG_THROTTLER_PERIOD = TDuration::MilliSeconds(500);

////////////////////////////////////////////////////////////////////////////////

enum class ECheckRange
{
    NotOverlapped,
    DelayRequest,
    ResponseAlready,
    ResponseRejected,
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestDetails
{
    void* Context = nullptr;
    TStringBuf Out;
    TStringBuf DataBuffer; // if non empty, zero copy is possible
    TString DeviceUUID;
    TString ClientId;

    ui64 VolumeRequestId = 0;
    TBlockRange64 Range = {};
    bool IsMultideviceRequest = false;
};

template <typename TRequest>
struct TExecutionData
{
    TCallContextPtr CallContext;
    ui32 BlockSize = 0;
    std::shared_ptr<TRequest> Request;
    TStorageAdapterPtr Device;
};

template <typename TRequest>
struct TContinuationData
{
    TRequestDetails RequestDetails;
    TExecutionData<TRequest> ExecutionData;
};

using TWriteRequestContinuationData =
    TContinuationData<NProto::TWriteBlocksRequest>;
using TZeroRequestContinuationData =
    TContinuationData<NProto::TZeroBlocksRequest>;


struct TSynchronizedData
{
    TRecentBlocksTracker RecentBlocksTracker;
    TOldRequestCounters OldRequestCounters;
    TList<TWriteRequestContinuationData> PostponedWriteRequests = {};
    TList<TZeroRequestContinuationData> PostponedZeroRequests = {};
    bool SecureEraseInProgress = false;
};

struct TThreadSafeData: public TSynchronized<TSynchronizedData, TAdaptiveLock>
{
    TAccess operator->() = delete;
};

struct TDeviceData
{
    const TStorageAdapterPtr Device;
    mutable TThreadSafeData ThreadSafeData;
};

////////////////////////////////////////////////////////////////////////////////

THashMap<TString, TDeviceData> MakeDevices(
    THashMap<TString, TStorageAdapterPtr> devices,
    const TRdmaTargetConfig& rdmaTargetConfig)
{
    THashMap<TString, TDeviceData> result;
    for (auto& [deviceUUID, storageAdapter]: devices) {
        TSynchronizedData synchronizedData{
            TRecentBlocksTracker{deviceUUID},
            rdmaTargetConfig.OldRequestCounters};

        TDeviceData device{
            std::move(storageAdapter),
            TThreadSafeData{std::move(synchronizedData)}};

        result.insert({deviceUUID, std::move(device)});
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

// Thread-safe. After Init() public method HandleRequest() can be called
// from any thread.
class TRequestHandler final
    : public NRdma::IServerHandler
    , public std::enable_shared_from_this<TRequestHandler>
{
private:
    const THashMap<TString, TDeviceData> Devices;
    const ITaskQueuePtr TaskQueue;

    mutable TLogThrottler LogThrottler{LOG_THROTTLER_PERIOD};
    TLog Log;

    const TDeviceClientPtr DeviceClient;

    std::weak_ptr<NRdma::IServerEndpoint> Endpoint;
    const NRdma::TProtoMessageSerializer* Serializer =
        TBlockStoreProtocol::Serializer();

    const bool RejectLateRequestsAtDiskAgentEnabled;

public:
    TRequestHandler(
            THashMap<TString, TStorageAdapterPtr> devices,
            ITaskQueuePtr taskQueue,
            TDeviceClientPtr deviceClient,
            TRdmaTargetConfig rdmaTargetConfig)
        : Devices(MakeDevices(std::move(devices), rdmaTargetConfig))
        , TaskQueue(std::move(taskQueue))
        , DeviceClient(std::move(deviceClient))
        , RejectLateRequestsAtDiskAgentEnabled(
              rdmaTargetConfig.RejectLateRequestsAtDiskAgentEnabled)
    {}

    void Init(NRdma::IServerEndpointPtr endpoint, TLog log)
    {
        Endpoint = std::move(endpoint);
        Log = std::move(log);
    }

    void HandleRequest(
        void* context,
        TCallContextPtr callContext,
        TStringBuf in,
        TStringBuf out) override
    {
        auto doHandleRequest = [self = shared_from_this(),
                                context = context,
                                callContext = std::move(callContext),
                                in = in,
                                out = out]() mutable -> NProto::TError
        {
            return self
                ->DoHandleRequest(context, std::move(callContext), in, out);
        };

        auto safeHandleRequest =
            [endpoint = Endpoint,
             context = context,
             doHandleRequest = std::move(doHandleRequest)]() mutable
        {
            auto error =
                SafeExecute<NProto::TError>(std::move(doHandleRequest));

            if (error.GetCode()) {
                if (auto ep = endpoint.lock()) {
                    ep->SendError(context, error.GetCode(), error.GetMessage());
                }
            }
        };

        TaskQueue->ExecuteSimple(std::move(safeHandleRequest));
    }

    NProto::TError DeviceSecureEraseStart(const TString& deviceUUID)
    {
        auto token = GetAccessToken(deviceUUID);
        if (token->RecentBlocksTracker.HasInflight() ||
            token->PostponedWriteRequests.size() ||
            token->PostponedZeroRequests.size())
        {
            ReportDiskAgentSecureEraseDuringIo();
            return MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "SecureErase with inflight ios present for device "
                    << deviceUUID);
        }

        token->SecureEraseInProgress = true;
        return MakeError(S_OK);
    }

    void DeviceSecureEraseFinish(
        const TString& deviceUUID,
        const NProto::TError& error)
    {
        auto token = GetAccessToken(deviceUUID);

        token->SecureEraseInProgress = false;

        if (HasError(error)) {
            return;
        }

        token->RecentBlocksTracker.Reset();
    }

private:
    NProto::TError DoHandleRequest(
        void* context,
        TCallContextPtr callContext,
        TStringBuf in,
        TStringBuf out) const
    {
        auto resultOrError = Serializer->Parse(in);

        if (HasError(resultOrError)) {
            return resultOrError.GetError();
        }

        const auto& request = resultOrError.GetResult();

        const bool isZeroCopyDataSupported =
            HasProtoFlag(request.Flags, NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);

        switch (request.MsgId) {
            case TBlockStoreProtocol::ReadDeviceBlocksRequest:
                return HandleReadBlocksRequest(
                    context,
                    std::move(callContext),
                    static_cast<NProto::TReadDeviceBlocksRequest&>(
                        *request.Proto),
                    isZeroCopyDataSupported,
                    request.Data,
                    out);

            case TBlockStoreProtocol::WriteDeviceBlocksRequest:
                return HandleWriteBlocksRequest(
                    context,
                    std::move(callContext),
                    static_cast<NProto::TWriteDeviceBlocksRequest&>(*request.Proto),
                    isZeroCopyDataSupported,
                    request.Data,
                    out);

            case TBlockStoreProtocol::ZeroDeviceBlocksRequest:
                return HandleZeroBlocksRequest(
                    context,
                    std::move(callContext),
                    static_cast<NProto::TZeroDeviceBlocksRequest&>(*request.Proto),
                    request.Data,
                    out);

            case TBlockStoreProtocol::ChecksumDeviceBlocksRequest:
                return HandleChecksumBlocksRequest(
                    context,
                    std::move(callContext),
                    static_cast<NProto::TChecksumDeviceBlocksRequest&>(*request.Proto),
                    request.Data,
                    out);

            default:
                return MakeError(E_NOT_IMPLEMENTED);
        }
    }

    TStorageAdapterPtr GetDevice(
        const TString& uuid,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) const
    {
        if (DeviceClient->IsDeviceDisabled(uuid)) {
            STORAGE_ERROR_T(
                LogThrottler,
                "[" << uuid << "/" << clientId << "] Device disabled. Drop request.");

            if (auto* deviceData = Devices.FindPtr(uuid)) {
                deviceData->Device->ReportIOError();
            }
            ythrow TServiceError(MakeError(E_IO, "Device disabled"));
        }

        NProto::TError error =
            DeviceClient->AccessDevice(uuid, clientId, accessMode);

        if (HasError(error)) {
            ythrow TServiceError(error);
        }

        auto it = Devices.find(uuid);

        if (it == Devices.cend()) {
            ythrow TServiceError(E_NOT_FOUND);
        }

        return it->second.Device;
    }

    TThreadSafeData::TAccess GetAccessToken(const TString& uuid) const
    {
        if (const auto* deviceData = Devices.FindPtr(uuid)) {
            return deviceData->ThreadSafeData.Access();
        }
        ythrow TServiceError(E_NOT_FOUND);
    }

    template <typename Future, typename THandleResponseMethod>
    void SubscribeForResponse(
        Future future,
        TRequestDetails requestDetails,
        THandleResponseMethod handleResponseMethod) const
    {
        auto handleResponse =
            [self = shared_from_this(),
             requestDetails = std::move(requestDetails),
             handleResponseMethod = handleResponseMethod](Future future) mutable
        {
            self->TaskQueue->ExecuteSimple(
                [self = self,
                 future = std::move(future),
                 requestDetails = std::move(requestDetails),
                 handleResponseMethod = handleResponseMethod]() mutable
                {
                    if (requestDetails.VolumeRequestId) {
                        const bool success = future.HasValue() &&
                                             !HasError(future.GetValueSync());
                        self->OnRequestFinished(
                            requestDetails.VolumeRequestId,
                            requestDetails.Range,
                            requestDetails.DeviceUUID,
                            success);
                    }

                    const TRequestHandler* obj = self.get();
                    // Call TRequestHandler::HandleXXXBlocksResponse()
                    (obj->*handleResponseMethod)(
                        requestDetails,
                        std::move(future));
                });
        };
        future.Subscribe(std::move(handleResponse));
    }

    ECheckRange CheckRangeIntersection(
        const TRequestDetails& requestDetails,
        TSynchronizedData& synchronizedData,
        TString* overlapDetails) const
    {
        if (synchronizedData.SecureEraseInProgress) {
            ReportDiskAgentIoDuringSecureErase(
                TStringBuilder()
                << " Device=" << requestDetails.DeviceUUID
                << ", ClientId=" << requestDetails.ClientId
                << ", StartIndex=" << requestDetails.Range.Start
                << ", BlocksCount=" << requestDetails.Range.Size()
                << ", IsWrite=1"
                << ", IsRdma=1");
            *overlapDetails = "Secure erase in progress";
            return ECheckRange::ResponseRejected;
        }

        const bool overlapsWithInflightRequests =
            synchronizedData.RecentBlocksTracker.CheckInflight(
                requestDetails.VolumeRequestId,
                requestDetails.Range);
        if (overlapsWithInflightRequests) {
            synchronizedData.OldRequestCounters.Delayed->Inc();
            if (!RejectLateRequestsAtDiskAgentEnabled) {
                // Monitoring mode. Don't change the behavior.
                return ECheckRange::NotOverlapped;
            }
            return ECheckRange::DelayRequest;
        }

        auto result = OverlapStatusToResult(
            synchronizedData.RecentBlocksTracker.CheckRecorded(
                requestDetails.VolumeRequestId,
                requestDetails.Range,
                overlapDetails),
            requestDetails.IsMultideviceRequest);
        if (result != S_OK) {
            if (result == E_REJECTED) {
                synchronizedData.OldRequestCounters.Rejected->Inc();
            } else if (result == S_ALREADY) {
                synchronizedData.OldRequestCounters.Already->Inc();
            } else {
                Y_DEBUG_ABORT_UNLESS(false);
            }

            if (!RejectLateRequestsAtDiskAgentEnabled) {
                // Monitoring mode. Don't change the behavior.
                return ECheckRange::NotOverlapped;
            }

            if (result == E_REJECTED) {
                return ECheckRange::ResponseRejected;
            }
            return ECheckRange::ResponseAlready;
        }

        // Here we add request to inflight list. Caller should execute request
        // and call RecentBlocksTracker.RemoveInflight() after request finished.
        synchronizedData.RecentBlocksTracker.AddInflight(
            requestDetails.VolumeRequestId,
            requestDetails.Range);
        return ECheckRange::NotOverlapped;
    }

    template <typename T>
    TList<T> ProcessPostponedRequests(
        TThreadSafeData::TAccess& token,
        TList<T>& postponedRequests) const
    {
        TList<T> readyToExecute;
        auto executeNotOverlappedRequests = [&](T& postponedRequest) {
            TString overlapDetails;
            const ECheckRange checkResult = CheckRangeIntersection(
                postponedRequest.RequestDetails,
                *token,
                &overlapDetails);

            switch (checkResult) {
                case ECheckRange::NotOverlapped:
                    readyToExecute.push_back(std::move(postponedRequest));
                    return true;
                case ECheckRange::ResponseAlready:
                    FinishHandleRequest(
                        postponedRequest,
                        S_ALREADY,
                        overlapDetails);
                    return true;
                case ECheckRange::ResponseRejected:
                    FinishHandleRequest(
                        postponedRequest,
                        E_REJECTED,
                        overlapDetails);
                    return true;
                case ECheckRange::DelayRequest: {
                    return false;
                }
            }
            STORAGE_VERIFY(
                false,
                TWellKnownEntityTypes::DEVICE,
                token->RecentBlocksTracker.GetDeviceUUID());
            return false;
        };

        std::erase_if(postponedRequests, executeNotOverlappedRequests);
        return readyToExecute;
    }

    void OnRequestFinished(
        ui64 volumeRequestId,
        TBlockRange64 range,
        const TString& uuid,
        bool success) const
    {
        TList<TWriteRequestContinuationData> readyToExecuteWriteRequests;
        TList<TZeroRequestContinuationData> readyToExecuteZeroRequests;

        {
            auto token = GetAccessToken(uuid);
            token->RecentBlocksTracker.RemoveInflight(volumeRequestId);
            if (success) {
                token->RecentBlocksTracker.AddRecorded(volumeRequestId, range);
            }
            readyToExecuteWriteRequests =
                ProcessPostponedRequests(token, token->PostponedWriteRequests);
            readyToExecuteZeroRequests =
                ProcessPostponedRequests(token, token->PostponedZeroRequests);
        }

        for (auto& continuationData: readyToExecuteWriteRequests) {
            ContinueHandleRequest(std::move(continuationData));
        }
        for (auto& continuationData: readyToExecuteZeroRequests) {
            ContinueHandleRequest(std::move(continuationData));
        }
    }

    NProto::TError HandleReadBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TReadDeviceBlocksRequest& request,
        bool isZeroCopyDataSupported,
        TStringBuf requestData,
        TStringBuf out) const
    {
        if (Y_UNLIKELY(requestData.length() != 0)) {
            return MakeError(E_ARGUMENT);
        }

        auto device = GetDevice(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::VOLUME_ACCESS_READ_ONLY);

        auto token = GetAccessToken(request.GetDeviceUUID());
        if (token->SecureEraseInProgress) {
            const auto& clientId = request.GetHeaders().GetClientId();
            if (clientId != CheckHealthClientId) {
                ReportDiskAgentIoDuringSecureErase(
                    TStringBuilder()
                    << " Device=" << request.GetDeviceUUID()
                    << ", ClientId=" << clientId
                    << ", StartIndex=" << request.GetStartIndex()
                    << ", BlocksCount=" << request.GetBlocksCount()
                    << ", IsWrite=0"
                    << ", IsRdma=1");
            }
            return MakeError(E_REJECTED, "Secure erase in progress");
        }

        auto req = std::make_shared<NProto::TReadBlocksRequest>();

        req->SetStartIndex(request.GetStartIndex());
        req->SetBlocksCount(request.GetBlocksCount());

        TStringBuf dataBuffer;
        if (isZeroCopyDataSupported) {
            dataBuffer = out;
            dataBuffer.RSeek(request.GetBlocksCount() * request.GetBlockSize());
        }

        auto future = device->ReadBlocks(
            Now(),
            std::move(callContext),
            std::move(req),
            request.GetBlockSize(),
            dataBuffer);

        SubscribeForResponse(
            std::move(future),
            TRequestDetails{
                context,
                out,
                dataBuffer,
                request.GetDeviceUUID(),
                request.GetHeaders().GetClientId()},
            &TRequestHandler::HandleReadBlocksResponse);

        return {};
    }

    void HandleReadBlocksResponse(
        const TRequestDetails& requestDetails,
        TFuture<NProto::TReadBlocksResponse> future) const
    {
        const auto& response = future.GetValue();
        const auto& blocks = response.GetBlocks();
        const auto& error = response.GetError();

        NProto::TReadDeviceBlocksResponse proto;
        if (error.GetCode()) {
            *proto.MutableError() = error;
        }
        if (HasError(error)) {
            STORAGE_ERROR_T(
                LogThrottler,
                "[" << requestDetails.DeviceUUID << "/"
                    << requestDetails.ClientId << "] read error: "
                    << error.GetMessage() << " (" << error.GetCode() << ")");
        }

        size_t bytes;
        ui32 flags = 0;

        if (requestDetails.DataBuffer.size()) {
            SetProtoFlag(flags, NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);
            NRdma::TProtoMessageSerializer::SerializeWithDataLength(
                requestDetails.Out,
                TBlockStoreProtocol::ReadDeviceBlocksResponse,
                flags,
                proto,
                requestDetails.DataBuffer.size());
            bytes = requestDetails.Out.size();
        } else {
            TStackVec<TBlockDataRef> parts;
            parts.reserve(blocks.BuffersSize());

            for (const auto& buffer: blocks.GetBuffers()) {
                parts.emplace_back(TBlockDataRef(buffer.data(), buffer.size()));
            }

            bytes = NRdma::TProtoMessageSerializer::SerializeWithData(
                requestDetails.Out,
                TBlockStoreProtocol::ReadDeviceBlocksResponse,
                flags,
                proto,
                parts);
        }

        if (auto ep = Endpoint.lock()) {
            ep->SendResponse(requestDetails.Context, bytes);
        }
    }

    NProto::TError HandleWriteBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TWriteDeviceBlocksRequest& request,
        bool isZeroCopyDataSupported,
        TStringBuf requestData,
        TStringBuf out) const
    {
        if (Y_UNLIKELY(requestData.length() == 0)) {
            return MakeError(E_ARGUMENT);
        }

        auto device = GetDevice(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::VOLUME_ACCESS_READ_WRITE);

        auto req = std::make_shared<NProto::TWriteBlocksRequest>();
        TStringBuf dataBuffer;

        req->SetStartIndex(request.GetStartIndex());
        if (isZeroCopyDataSupported) {
            dataBuffer = requestData;
        } else {
            req->MutableBlocks()->AddBuffers(
                requestData.data(),
                requestData.length());
        }

        const ui32 blockCount = requestData.length() / request.GetBlockSize();

        TWriteRequestContinuationData continuationData{
            {context,
             out,
             dataBuffer,
             request.GetDeviceUUID(),
             request.GetHeaders().GetClientId(),
             request.GetVolumeRequestId(),
             TBlockRange64::WithLength(request.GetStartIndex(), blockCount),
             request.GetMultideviceRequest()},
            {std::move(callContext),
             request.GetBlockSize(),
             std::move(req),
             std::move(device)}};

        if (continuationData.RequestDetails.VolumeRequestId) {
            auto token =
                GetAccessToken(continuationData.RequestDetails.DeviceUUID);
            TString overlapDetails;
            const ECheckRange checkResult = CheckRangeIntersection(
                continuationData.RequestDetails,
                *token,
                &overlapDetails);

            switch (checkResult) {
                case ECheckRange::NotOverlapped:
                    break;
                case ECheckRange::ResponseAlready:
                    return TErrorResponse(S_ALREADY, overlapDetails);
                case ECheckRange::ResponseRejected:
                    return TErrorResponse(E_REJECTED, overlapDetails);
                case ECheckRange::DelayRequest: {
                    token->PostponedWriteRequests.push_back(
                        std::move(continuationData));
                    return {};
                }
            }
        }

        ContinueHandleRequest(std::move(continuationData));
        return {};
    }

    void ContinueHandleRequest(
        TWriteRequestContinuationData continuationData) const
    {
        auto future = continuationData.ExecutionData.Device->WriteBlocks(
            Now(),
            std::move(continuationData.ExecutionData.CallContext),
            std::move(continuationData.ExecutionData.Request),
            continuationData.ExecutionData.BlockSize,
            continuationData.RequestDetails.DataBuffer);

        SubscribeForResponse(
            std::move(future),
            std::move(continuationData.RequestDetails),
            &TRequestHandler::HandleWriteBlocksResponse);
    }

    void FinishHandleRequest(
        const TWriteRequestContinuationData& continuationData,
        EWellKnownResultCodes resultCode,
        const TString& overlapDetails) const
    {
        HandleWriteBlocksResponse(
            continuationData.RequestDetails,
            MakeFuture<NProto::TWriteBlocksResponse>(
                TErrorResponse(resultCode, overlapDetails)));
    }

    void HandleWriteBlocksResponse(
        const TRequestDetails& requestDetails,
        TFuture<NProto::TWriteBlocksResponse> future) const
    {
        const auto& response = future.GetValue();
        const auto& error = response.GetError();

        NProto::TWriteDeviceBlocksResponse proto;
        if (error.GetCode()) {
            *proto.MutableError() = error;
        }

        if (HasError(error)) {
            STORAGE_ERROR_T(
                LogThrottler,
                "[" << requestDetails.DeviceUUID << "/"
                    << requestDetails.ClientId << "] write error: "
                    << error.GetMessage() << " (" << error.GetCode() << ")");
        }

        size_t bytes = NRdma::TProtoMessageSerializer::Serialize(
            requestDetails.Out,
            TBlockStoreProtocol::WriteDeviceBlocksResponse,
            0,   // flags
            proto);

        if (auto ep = Endpoint.lock()) {
            ep->SendResponse(requestDetails.Context, bytes);
        }
    }

    NProto::TError HandleZeroBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TZeroDeviceBlocksRequest& request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        if (Y_UNLIKELY(requestData.length() != 0)) {
            return MakeError(E_ARGUMENT);
        }

        auto device = GetDevice(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::VOLUME_ACCESS_READ_WRITE);

        auto req = std::make_shared<NProto::TZeroBlocksRequest>();
        req->SetStartIndex(request.GetStartIndex());
        req->SetBlocksCount(request.GetBlocksCount());

        TZeroRequestContinuationData continuationData{
            {context,
             out,
             {},   // no data buffer
             request.GetDeviceUUID(),
             request.GetHeaders().GetClientId(),
             request.GetVolumeRequestId(),
             TBlockRange64::WithLength(
                 request.GetStartIndex(),
                 GetBlocksCount(request)),
             request.GetMultideviceRequest()},
            {std::move(callContext),
             request.GetBlockSize(),
             std::move(req),
             std::move(device)}};

        if (continuationData.RequestDetails.VolumeRequestId) {
            auto token =
                GetAccessToken(continuationData.RequestDetails.DeviceUUID);
            TString overlapDetails;
            const ECheckRange checkResult = CheckRangeIntersection(
                continuationData.RequestDetails,
                *token,
                &overlapDetails);

            switch (checkResult) {
                case ECheckRange::NotOverlapped:
                    break;
                case ECheckRange::ResponseAlready:
                    return TErrorResponse(S_ALREADY, overlapDetails);
                case ECheckRange::ResponseRejected:
                    return TErrorResponse(E_REJECTED, overlapDetails);
                case ECheckRange::DelayRequest: {
                    token->PostponedZeroRequests.push_back(
                        std::move(continuationData));
                    return {};
                }
            }
        }

        ContinueHandleRequest(std::move(continuationData));
        return {};
    }

    void ContinueHandleRequest(
        TZeroRequestContinuationData continuationData) const
    {
        auto future = continuationData.ExecutionData.Device->ZeroBlocks(
            Now(),
            std::move(continuationData.ExecutionData.CallContext),
            std::move(continuationData.ExecutionData.Request),
            continuationData.ExecutionData.BlockSize);

        SubscribeForResponse(
            std::move(future),
            std::move(continuationData.RequestDetails),
            &TRequestHandler::HandleZeroBlocksResponse);
    }

    void FinishHandleRequest(
        const TZeroRequestContinuationData& continuationData,
        EWellKnownResultCodes resultCode,
        const TString& overlapDetails) const
    {
        HandleZeroBlocksResponse(
            continuationData.RequestDetails,
            MakeFuture<NProto::TZeroBlocksResponse>(
                TErrorResponse(resultCode, overlapDetails)));
    }

    void HandleZeroBlocksResponse(
        const TRequestDetails& requestDetails,
        TFuture<NProto::TZeroBlocksResponse> future) const
    {
        const auto& response = future.GetValue();
        const auto& error = response.GetError();

        NProto::TZeroDeviceBlocksResponse proto;
        if (error.GetCode()) {
            *proto.MutableError() = error;
        }
        if (HasError(error)) {
            STORAGE_ERROR_T(
                LogThrottler,
                "[" << requestDetails.DeviceUUID << "/"
                    << requestDetails.ClientId << "] zero error: "
                    << error.GetMessage() << " (" << error.GetCode() << ")");
        }

        size_t bytes = NRdma::TProtoMessageSerializer::Serialize(
            requestDetails.Out,
            TBlockStoreProtocol::ZeroDeviceBlocksResponse,
            0,   // flags
            proto);

        if (auto ep = Endpoint.lock()) {
            ep->SendResponse(requestDetails.Context, bytes);
        }
    }

    NProto::TError HandleChecksumBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        NProto::TChecksumDeviceBlocksRequest& request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        if (Y_UNLIKELY(requestData.length() != 0)) {
            return MakeError(E_ARGUMENT);
        }

        auto device = GetDevice(
            request.GetDeviceUUID(),
            request.GetHeaders().GetClientId(),
            NProto::VOLUME_ACCESS_READ_ONLY);

        auto req = std::make_shared<NProto::TReadBlocksRequest>();

        req->SetStartIndex(request.GetStartIndex());
        req->SetBlocksCount(request.GetBlocksCount());

        auto future = device->ReadBlocks(
            Now(),
            std::move(callContext),
            std::move(req),
            request.GetBlockSize(),
            {}   // no data buffer
        );

        SubscribeForResponse(
            std::move(future),
            TRequestDetails{
                context,
                out,
                {},   // no data buffer
                request.GetDeviceUUID(),
                request.GetHeaders().GetClientId()},
            &TRequestHandler::HandleChecksumBlocksResponse);

        return {};
    }

    void HandleChecksumBlocksResponse(
        const TRequestDetails& requestDetails,
        TFuture<NProto::TReadBlocksResponse> future) const
    {
        const auto& response = future.GetValue();
        const auto& blocks = response.GetBlocks();
        const auto& error = response.GetError();

        NProto::TChecksumDeviceBlocksResponse proto;
        if (error.GetCode()) {
            *proto.MutableError() = error;
        }
        if (HasError(error)) {
            STORAGE_ERROR_T(
                LogThrottler,
                "[" << requestDetails.DeviceUUID << "/"
                    << requestDetails.ClientId << "] checksum(read) error: "
                    << error.GetMessage() << " (" << error.GetCode() << ")");
        }

        TBlockChecksum checksum;
        for (const auto& buffer: blocks.GetBuffers()) {
            checksum.Extend(buffer.Data(), buffer.Size());
        }
        proto.SetChecksum(checksum.GetValue());

        size_t bytes = NRdma::TProtoMessageSerializer::Serialize(
            requestDetails.Out,
            TBlockStoreProtocol::ChecksumDeviceBlocksResponse,
            0,   // flags
            proto);

        if (auto ep = Endpoint.lock()) {
            ep->SendResponse(requestDetails.Context, bytes);
        }
    }
};

///////////////////////////////////////////////////////////////////////////////

class TRdmaTarget final
    : public IRdmaTarget
{
private:
    const NProto::TRdmaEndpoint Config;

    std::shared_ptr<TRequestHandler> Handler;
    ILoggingServicePtr Logging;
    NRdma::IServerPtr Server;
    ITaskQueuePtr TaskQueue;

    TLog Log;

public:
    TRdmaTarget(
            NProto::TRdmaEndpoint config,
            ILoggingServicePtr logging,
            NRdma::IServerPtr server,
            TDeviceClientPtr deviceClient,
            THashMap<TString, TStorageAdapterPtr> devices,
            ITaskQueuePtr taskQueue,
            TRdmaTargetConfig rdmaTargetConfig)
        : Config(std::move(config))
        , Logging(std::move(logging))
        , Server(std::move(server))
        , TaskQueue(taskQueue)
    {
        Handler = std::make_shared<TRequestHandler>(
            std::move(devices),
            std::move(taskQueue),
            std::move(deviceClient),
            std::move(rdmaTargetConfig));
    }

    void Start() override
    {
        Log = Logging->CreateLog("BLOCKSTORE_DISK_AGENT");

        auto endpoint = Server->StartEndpoint(
            Config.GetHost(),
            Config.GetPort(),
            Handler);

        if (endpoint == nullptr) {
            STORAGE_ERROR("unable to set up RDMA endpoint");
            return;
        }

        Handler->Init(std::move(endpoint), std::move(Log));
    }

    void Stop() override
    {
        Server->Stop();
        TaskQueue->Stop();
    }

    NProto::TError DeviceSecureEraseStart(const TString& deviceUUID) override
    {
        return Handler->DeviceSecureEraseStart(deviceUUID);
    }

    void DeviceSecureEraseFinish(
        const TString& deviceUUID,
        const NProto::TError& error) override
    {
        Handler->DeviceSecureEraseFinish(deviceUUID, error);
    }
};

}   // namespace

IRdmaTargetPtr CreateRdmaTarget(
    NProto::TRdmaEndpoint config,
    TRdmaTargetConfig rdmaTargetConfig,
    ILoggingServicePtr logging,
    NRdma::IServerPtr server,
    TDeviceClientPtr deviceClient,
    THashMap<TString, TStorageAdapterPtr> devices)
{
    auto threadPool = CreateThreadPool("RDMA", rdmaTargetConfig.PoolSize);
    threadPool->Start();

    return std::make_shared<TRdmaTarget>(
        std::move(config),
        std::move(logging),
        std::move(server),
        std::move(deviceClient),
        std::move(devices),
        std::move(threadPool),
        std::move(rdmaTargetConfig));
}

}   // namespace NCloud::NBlockStore::NStorage
