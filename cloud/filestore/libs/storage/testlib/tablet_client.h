#pragma once

#include "helpers.h"
#include "test_env.h"

#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/core/config.h>
#include <cloud/filestore/libs/storage/model/channel_data_kind.h>
#include <cloud/filestore/libs/storage/tablet/tablet_private.h>

#include <contrib/ydb/core/testlib/actors/test_runtime.h>
#include <contrib/ydb/core/testlib/test_client.h>
#include <contrib/ydb/core/filestore/core/filestore.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace NImpl {

template <typename T>
concept HasRecord = requires (T v)
{
    {v.Record};
};

template <typename T>
TString DebugDump(const T& v)
{
    if constexpr (HasRecord<T>) {
        return v.Record.ShortDebugString();
    }

    return typeid(T).name();
}

using TEvRemoteHttpInfoRequest = NActors::NMon::TEvRemoteHttpInfo;
using TEvRemoteHttpInfoResponse = NActors::NMon::TEvRemoteHttpInfoRes;

}   // namespace NImpl

////////////////////////////////////////////////////////////////////////////////

struct TPerformanceProfile
{
    bool ThrottlingEnabled = false;

    ui32 MaxReadIops = 0;
    ui32 MaxWriteIops = 0;
    ui64 MaxReadBandwidth = 0;
    ui64 MaxWriteBandwidth = 0;

    ui32 BoostTime = 0;
    ui32 BoostRefillTime = 0;
    ui32 BoostPercentage = 0;

    ui64 MaxPostponedWeight = 0;
    ui32 MaxWriteCostMultiplier = 0;
    ui32 MaxPostponedTime = 0;
    ui32 MaxPostponedCount = 0;

    ui32 BurstPercentage = 0;
    ui64 DefaultPostponedRequestWeight = 0;
};

struct TFileSystemConfig
{
    TString FileSystemId = "test";
    TString CloudId = "test_cloud";
    TString FolderId = "test_folder";
    ui32 BlockSize = DefaultBlockSize;
    ui64 BlockCount = DefaultBlockCount;
    ui64 NodeCount = MaxNodes;
    ui32 ChannelCount = DefaultChannelCount;
    ui32 StorageMediaKind = 0;
    TPerformanceProfile PerformanceProfile;
};

////////////////////////////////////////////////////////////////////////////////

const ui32 RangeIdHasherType = 1;

inline ui32 GetMixedRangeIndex(ui64 nodeId, ui32 blockIndex)
{
    static auto hasher = CreateRangeIdHasher(RangeIdHasherType);
    return hasher->Calc(nodeId, blockIndex);
}

////////////////////////////////////////////////////////////////////////////////

constexpr i32 DefaultPid = 123;

class TIndexTabletClient
{
private:
    NKikimr::TTestActorRuntime& Runtime;
    ui32 NodeIdx;
    ui64 TabletId;

    NActors::TActorId Sender;
    NActors::TActorId PipeClient;

    THeaders Headers;

public:
    TIndexTabletClient(
            NKikimr::TTestActorRuntime& runtime,
            ui32 nodeIdx,
            ui64 tabletId,
            const TFileSystemConfig& config = {},
            bool updateConfig = true)
        : Runtime(runtime)
        , NodeIdx(nodeIdx)
        , TabletId(tabletId)
    {
        Sender = Runtime.AllocateEdgeActor(NodeIdx);

        ReconnectPipe();

        WaitReady();
        if (updateConfig) {
            UpdateConfig(config);
        }
    }

    TIndexTabletClient& WithSessionSeqNo(ui64 seqNo)
    {
        Headers.SessionSeqNo = seqNo;
        return *this;
    }

    void RebootTablet()
    {
        TVector<ui64> tablets = { TabletId };
        auto guard = NKikimr::CreateTabletScheduledEventsGuard(
            tablets,
            Runtime,
            Sender);

        NKikimr::RebootTablet(Runtime, TabletId, Sender, NodeIdx);

        ReconnectPipe();
    }

    template <typename TRequest>
    void SendRequest(std::unique_ptr<TRequest> request, ui64 cookie = 0)
    {
        Runtime.SendToPipe(
            PipeClient,
            Sender,
            request.release(),
            NodeIdx,
            cookie);
    }

    template <typename TResponse>
    auto RecvResponse()
    {
        TAutoPtr<NActors::IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle);
        return std::unique_ptr<TResponse>(handle->Release<TResponse>().Release());
    }

    void RecoverSession()
    {
        CreateSession(Headers.ClientId, Headers.SessionId, TString(), 0, false, true);
    }

    auto InitSession(
        const TString& clientId,
        const TString& sessionId,
        const TString& checkpointId = {},
        ui64 sessionSeqNo = 0,
        bool readOnly = false,
        bool restoreClientSession = false)
    {
        auto response = CreateSession(
            clientId,
            sessionId,
            checkpointId,
            sessionSeqNo,
            readOnly,
            restoreClientSession);
        SetHeaders(clientId, sessionId, sessionSeqNo);

        return response;
    }

    void SetHeaders(const TString& clientId, const TString& sessionId, ui64 sessionSeqNo)
    {
        Headers = { "test", clientId, sessionId, sessionSeqNo};
    }

    template <typename T>
    auto CreateSessionRequest()
    {
        auto request = std::make_unique<T>();
        Headers.Fill(request->Record);
        return request;
    }

    //
    // TEvIndexTablet
    //

    auto CreateWaitReadyRequest()
    {
        return std::make_unique<TEvIndexTablet::TEvWaitReadyRequest>();
    }

    auto CreateGetStorageStatsRequest(
        ui32 compactionRangeCount = 0,
        ui32 compactionRangeCountByCleanupScore = 0)
    {
        auto request = std::make_unique<TEvIndexTablet::TEvGetStorageStatsRequest>();
        request->Record.SetCompactionRangeCountByCompactionScore(
            compactionRangeCount);
        request->Record.SetCompactionRangeCountByCleanupScore(
            compactionRangeCountByCleanupScore);
        return request;
    }

    auto CreateGetFileSystemConfigRequest()
    {
        return std::make_unique<TEvIndexTablet::TEvGetFileSystemConfigRequest>();
    }

    auto CreateCreateSessionRequest(
        const TString& clientId,
        const TString& sessionId,
        const TString& checkpointId = {},
        ui64 mountSeqNo = 0,
        bool readOnly = false,
        bool restoreClientSession = false)
    {
        auto request = std::make_unique<TEvIndexTablet::TEvCreateSessionRequest>();
        request->Record.SetCheckpointId(checkpointId);
        request->Record.SetMountSeqNumber(mountSeqNo);
        request->Record.SetReadOnly(readOnly);
        request->Record.SetRestoreClientSession(restoreClientSession);

        auto* headers = request->Record.MutableHeaders();
        headers->SetClientId(clientId);
        headers->SetSessionId(sessionId);
        return request;
    }

    auto CreateResetSessionRequest(
        const TString& clientId,
        const TString& sessionId,
        const ui64 sessionSeqNo,
        const TString& state)
    {
        auto request = std::make_unique<TEvService::TEvResetSessionRequest>();
        request->Record.SetSessionState(state);

        auto* headers = request->Record.MutableHeaders();
        headers->SetClientId(clientId);
        headers->SetSessionId(sessionId);
        headers->SetSessionSeqNo(sessionSeqNo);
        return request;
    }

    auto CreateResetSessionRequest(const TString& state)
    {
        auto request = std::make_unique<TEvService::TEvResetSessionRequest>();
        request->Record.SetSessionState(state);

        Headers.Fill(request->Record);
        return request;
    }

    auto CreateGetStorageConfigRequest()
    {
        return std::make_unique<TEvIndexTablet::TEvGetStorageConfigRequest>();
    }

    auto CreateChangeStorageConfigRequest(NProto::TStorageConfig patch)
    {
        auto request =
            std::make_unique<TEvIndexTablet::TEvChangeStorageConfigRequest>();
        *request->Record.MutableStorageConfig() = std::move(patch);
        return request;
    }

    auto CreateDestroySessionRequest(ui64 seqNo = 0)
    {
        auto request = std::make_unique<TEvIndexTablet::TEvDestroySessionRequest>();
        Headers.Fill(request->Record);
        if (seqNo) {
            request->Record.MutableHeaders()->SetSessionSeqNo(seqNo);
        }
        return request;
    }

    auto CreateForcedOperationRequest(
        NProtoPrivate::TForcedOperationRequest::EForcedOperationType type)
    {
        NProtoPrivate::TForcedOperationRequest request;
        request.SetOpType(type);
        auto requestToTablet =
            std::make_unique<TEvIndexTablet::TEvForcedOperationRequest>();

        requestToTablet->Record = std::move(request);
        return requestToTablet;
    }

    auto CreateWriteCompactionMapRequest(
        const TVector<NProtoPrivate::TCompactionRangeStats>& ranges)
    {
        NProtoPrivate::TWriteCompactionMapRequest request;
        for (auto range: ranges)
        {
            *request.AddRanges() = range;
        }

        auto requestToTablet =
            std::make_unique<TEvIndexTablet::TEvWriteCompactionMapRequest>();

        requestToTablet->Record = std::move(request);
        return requestToTablet;
    }

    auto CreateConfigureAsShardRequest(ui32 shardNo)
    {
        auto request =
            std::make_unique<TEvIndexTablet::TEvConfigureAsShardRequest>();
        request->Record.SetShardNo(shardNo);

        return request;
    }

    //
    // TEvIndexTabletPrivate
    //

    auto CreateFlushRequest()
    {
        return std::make_unique<TEvIndexTabletPrivate::TEvFlushRequest>();
    }

    auto CreateFlushBytesRequest()
    {
        return std::make_unique<TEvIndexTabletPrivate::TEvFlushBytesRequest>();
    }

    auto CreateCleanupRequest(ui32 rangeId)
    {
        return std::make_unique<TEvIndexTabletPrivate::TEvCleanupRequest>(rangeId);
    }

    auto CreateCompactionRequest(ui32 rangeId, bool filter = false)
    {
        return std::make_unique<TEvIndexTabletPrivate::TEvCompactionRequest>(rangeId, filter);
    }

    auto CreateForcedRangeOperationRequest(
        TVector<ui32> ranges,
        TEvIndexTabletPrivate::EForcedRangeOperationMode mode)
    {
        return std::make_unique<
            TEvIndexTabletPrivate::TEvForcedRangeOperationRequest>(
            std::move(ranges),
            mode,
            CreateGuidAsString());
    }

    auto CreateCollectGarbageRequest()
    {
        return std::make_unique<TEvIndexTabletPrivate::TEvCollectGarbageRequest>();
    }

    auto CreateTruncateRequest(ui64 node, ui64 length)
    {
        return std::make_unique<TEvIndexTabletPrivate::TEvTruncateRequest>(
            node,
            TByteRange(0, length, DefaultBlockSize));
    }

    auto CreateTruncateRangeRequest(ui64 node, ui64 offset, ui64 length)
    {
        return std::make_unique<TEvIndexTabletPrivate::TEvTruncateRangeRequest>(
            node,
            TByteRange(offset, length, DefaultBlockSize));
    }

    auto CreateZeroRangeRequest(ui64 node, ui64 offset, ui64 length)
    {
        return std::make_unique<TEvIndexTabletPrivate::TEvZeroRangeRequest>(
            node,
            TByteRange(offset, length, DefaultBlockSize));
    }

    auto CreateDeleteGarbageRequest(
        ui64 collectGarbageId,
        TVector<TPartialBlobId> newBlobs,
        TVector<TPartialBlobId> garbageBlobs)
    {
        return std::make_unique<TEvIndexTabletPrivate::TEvDeleteGarbageRequest>(
            collectGarbageId,
            std::move(newBlobs),
            std::move(garbageBlobs));
    }

    auto CreateFilterAliveNodesRequest(TStackVec<ui64, 16> nodes)
    {
        return std::make_unique<TEvIndexTabletPrivate::TEvFilterAliveNodesRequest>(
            std::move(nodes));
    }

    auto CreateGenerateCommitIdRequest()
    {
        return std::make_unique<TEvIndexTabletPrivate::TEvGenerateCommitIdRequest>();
    }

    auto CreateDumpCompactionRangeRequest(ui32 rangeId)
    {
        return std::make_unique<TEvIndexTabletPrivate::TEvDumpCompactionRangeRequest>(
            rangeId);
    }

    auto CreateCleanupSessionsRequest()
    {
        return std::make_unique<TEvIndexTabletPrivate::TEvCleanupSessionsRequest>();
    }

    auto CreateUpdateCounters()
    {
        return std::make_unique<TEvIndexTabletPrivate::TEvUpdateCounters>();
    }

    //
    // TEvService
    //

    auto CreateDescribeSessionsRequest()
    {
        return std::make_unique<TEvIndexTablet::TEvDescribeSessionsRequest>();
    }

    auto CreateCreateNodeRequest(
        const TCreateNodeArgs& args,
        ui64 requestId = 0)
    {
        auto request = CreateSessionRequest<TEvService::TEvCreateNodeRequest>();
        Headers.Fill(request->Record);
        args.Fill(request->Record);
        request->Record.MutableHeaders()->SetRequestId(requestId);
        return request;
    }

    auto CreateUnlinkNodeRequest(
        ui64 parent,
        const TString& name,
        bool unlinkDirectory,
        ui64 requestId = 0)
    {
        auto request = CreateSessionRequest<TEvService::TEvUnlinkNodeRequest>();
        request->Record.SetNodeId(parent);
        request->Record.SetName(name);
        request->Record.SetUnlinkDirectory(unlinkDirectory);
        request->Record.MutableHeaders()->SetRequestId(requestId);
        return request;
    }

    auto CreateRenameNodeRequest(
        ui64 parent,
        const TString& name,
        ui64 newparent,
        const TString& newname,
        ui32 flags = 0)
    {
        auto request = CreateSessionRequest<TEvService::TEvRenameNodeRequest>();
        request->Record.SetNodeId(parent);
        request->Record.SetName(name);
        request->Record.SetNewParentId(newparent);
        request->Record.SetNewName(newname);
        request->Record.SetFlags(flags);
        return request;
    }

    auto CreateAccessNodeRequest(ui64 node)
    {
        auto request = CreateSessionRequest<TEvService::TEvAccessNodeRequest>();
        request->Record.SetNodeId(node);
        return request;
    }

    auto CreateReadLinkRequest(ui64 node)
    {
        auto request = CreateSessionRequest<TEvService::TEvReadLinkRequest>();
        request->Record.SetNodeId(node);
        return request;
    }

    auto CreateListNodesRequest(ui64 node)
    {
        auto request = CreateSessionRequest<TEvService::TEvListNodesRequest>();
        request->Record.SetNodeId(node);
        return request;
    }

    auto CreateListNodesRequest(ui64 node, ui32 bytes, TString cookie)
    {
        auto request = CreateSessionRequest<TEvService::TEvListNodesRequest>();
        request->Record.SetNodeId(node);
        request->Record.SetMaxBytes(bytes);
        request->Record.SetCookie(std::move(cookie));
        return request;
    }

    auto CreateReadNodeRefsRequest(ui64 node, TString cookie, ui32 limit){
        auto request = std::make_unique<TEvIndexTablet::TEvReadNodeRefsRequest>();
        request->Record.SetNodeId(node);
        request->Record.SetCookie(std::move(cookie));
        request->Record.SetLimit(limit);
        return request;
    }

    auto CreateGetNodeAttrRequest(ui64 node, const TString& name = "")
    {
        auto request = CreateSessionRequest<TEvService::TEvGetNodeAttrRequest>();
        request->Record.SetNodeId(node);
        request->Record.SetName(name);
        return request;
    }

    auto CreateGetNodeAttrBatchRequest(ui64 node, const TVector<TString>& names)
    {
        auto request =
            CreateSessionRequest<TEvIndexTablet::TEvGetNodeAttrBatchRequest>();
        request->Record.SetNodeId(node);
        for (const auto& name: names) {
            *request->Record.AddNames() = name;
        }
        return request;
    }

    auto CreateSetNodeAttrRequest(const TSetNodeAttrArgs& args)
    {
        auto request = CreateSessionRequest<TEvService::TEvSetNodeAttrRequest>();
        args.Fill(request->Record);
        return request;
    }

    auto CreateListNodeXAttrRequest(ui64 node)
    {
        auto request = CreateSessionRequest<TEvService::TEvListNodeXAttrRequest>();
        request->Record.SetNodeId(node);
        return request;
    }

    auto CreateGetNodeXAttrRequest(ui64 node, const TString& name)
    {
        auto request = CreateSessionRequest<TEvService::TEvGetNodeXAttrRequest>();
        request->Record.SetNodeId(node);
        request->Record.SetName(name);
        return request;
    }

    auto CreateSetNodeXAttrRequest(ui64 node, const TString& name, const TString& value, ui64 flags = 0)
    {
        auto request = CreateSessionRequest<TEvService::TEvSetNodeXAttrRequest>();
        request->Record.SetNodeId(node);
        request->Record.SetName(name);
        request->Record.SetValue(value);
        request->Record.SetFlags(flags);
        return request;
    }

    auto CreateRemoveNodeXAttrRequest(ui64 node, const TString& name)
    {
        auto request = CreateSessionRequest<TEvService::TEvRemoveNodeXAttrRequest>();
        request->Record.SetNodeId(node);
        request->Record.SetName(name);
        return request;
    }

    auto CreateCreateHandleRequest(ui64 node, ui32 flags)
    {
        auto request = CreateSessionRequest<TEvService::TEvCreateHandleRequest>();
        request->Record.SetNodeId(node);
        request->Record.SetFlags(flags);
        return request;
    }

    auto CreateCreateHandleRequest(ui64 node, const TString& name, ui32 flags)
    {
        auto request = CreateSessionRequest<TEvService::TEvCreateHandleRequest>();
        request->Record.SetNodeId(node);
        request->Record.SetFlags(flags);
        request->Record.SetName(name);
        return request;
    }

    auto CreateDestroyHandleRequest(ui64 handle)
    {
        auto request = CreateSessionRequest<TEvService::TEvDestroyHandleRequest>();
        request->Record.SetHandle(handle);
        return request;
    }

    auto CreateAllocateDataRequest(ui64 handle, ui64 offset, ui64 length, ui32 flags)
    {
        auto request = CreateSessionRequest<TEvService::TEvAllocateDataRequest>();
        request->Record.SetHandle(handle);
        request->Record.SetOffset(offset);
        request->Record.SetLength(length);
        request->Record.SetFlags(flags);

        return request;
    }

    auto CreateWriteDataRequest(
        ui64 handle,
        ui64 offset,
        ui64 len,
        char fill,
        ui64 node = InvalidNodeId)
    {
        auto request = CreateSessionRequest<TEvService::TEvWriteDataRequest>();
        request->Record.SetHandle(handle);
        request->Record.SetOffset(offset);
        request->Record.SetBuffer(CreateBuffer(len, fill));
        if (node != InvalidNodeId) {
            request->Record.SetNodeId(node);
        }
        return request;
    }

    auto CreateWriteDataRequest(
        ui64 handle,
        ui64 offset,
        ui64 len,
        const char* data,
        ui64 node = InvalidNodeId)
    {
        auto request = CreateSessionRequest<TEvService::TEvWriteDataRequest>();
        request->Record.SetHandle(handle);
        request->Record.SetOffset(offset);
        request->Record.MutableBuffer()->assign(data, len);
        if (node != InvalidNodeId) {
            request->Record.SetNodeId(node);
        }
        return request;
    }

    auto CreateReadDataRequest(
        ui64 handle,
        ui64 offset,
        ui64 len,
        ui64 node = InvalidNodeId)
    {
        auto request = CreateSessionRequest<TEvService::TEvReadDataRequest>();
        request->Record.SetHandle(handle);
        request->Record.SetOffset(offset);
        request->Record.SetLength(len);
        if (node != InvalidNodeId) {
            request->Record.SetNodeId(node);
        }
        return request;
    }

    auto CreateDescribeDataRequest(ui64 handle, ui64 offset, ui64 len)
    {
        auto request =
            CreateSessionRequest<TEvIndexTablet::TEvDescribeDataRequest>();
        request->Record.SetHandle(handle);
        request->Record.SetOffset(offset);
        request->Record.SetLength(len);
        return request;
    }

    auto CreateGenerateBlobIdsRequest(
        ui64 nodeId,
        ui64 handle,
        ui64 offset,
        ui64 length)
    {
        auto request =
            CreateSessionRequest<TEvIndexTablet::TEvGenerateBlobIdsRequest>();
        request->Record.SetNodeId(nodeId);
        request->Record.SetHandle(handle);
        request->Record.SetOffset(offset);
        request->Record.SetLength(length);
        return request;
    }

    auto CreateAddDataRequest(
        ui64 nodeId,
        ui64 handle,
        ui64 offset,
        ui64 length,
        const TVector<NKikimr::TLogoBlobID>& blobIds,
        ui64 commitId,
        TVector<NProtoPrivate::TFreshDataRange> unalignedDataParts = {})
    {
        auto request = CreateSessionRequest<
            TEvIndexTablet::TEvAddDataRequest>();
        request->Record.SetNodeId(nodeId);
        request->Record.SetHandle(handle);
        request->Record.SetOffset(offset);
        request->Record.SetLength(length);
        for (const auto& blobId: blobIds) {
            NKikimr::LogoBlobIDFromLogoBlobID(
                blobId,
                request->Record.MutableBlobIds()->Add());
        }
        request->Record.SetCommitId(commitId);
        for (auto& part: unalignedDataParts) {
            *request->Record.AddUnalignedDataRanges() = std::move(part);
        }
        return request;
    }

    auto CreateAcquireLockRequest(
        ui64 handle,
        ui64 owner,
        ui64 offset,
        ui64 len,
        i32 pid = DefaultPid,
        NProto::ELockType type = NProto::E_EXCLUSIVE,
        NProto::ELockOrigin origin = NProto::E_FCNTL)
    {
        auto request = CreateSessionRequest<TEvService::TEvAcquireLockRequest>();
        auto& record = request->Record;
        record.SetHandle(handle);
        record.SetOwner(owner);
        record.SetOffset(offset);
        record.SetLength(len);
        record.SetPid(pid);
        record.SetLockType(type);
        record.SetLockOrigin(origin);
        return request;
    }

    auto CreateReleaseLockRequest(
        ui64 handle,
        ui64 owner,
        ui64 offset,
        ui64 len,
        i32 pid = DefaultPid,
        NProto::ELockOrigin origin = NProto::E_FCNTL)
    {
        auto request = CreateSessionRequest<TEvService::TEvReleaseLockRequest>();
        auto& record = request->Record;
        record.SetHandle(handle);
        record.SetOwner(owner);
        record.SetOffset(offset);
        record.SetLength(len);
        record.SetPid(pid);
        record.SetLockOrigin(origin);
        return request;
    }

    auto CreateTestLockRequest(
        ui64 handle,
        ui64 owner,
        ui64 offset,
        ui64 len,
        i32 pid = DefaultPid,
        NProto::ELockType type = NProto::E_EXCLUSIVE,
        NProto::ELockOrigin origin = NProto::E_FCNTL)
    {
        auto request = CreateSessionRequest<TEvService::TEvTestLockRequest>();
        auto& record = request->Record;
        record.SetHandle(handle);
        record.SetOwner(owner);
        record.SetOffset(offset);
        record.SetLength(len);
        record.SetPid(pid);
        record.SetLockType(type);
        record.SetLockOrigin(origin);
        return request;
    }

    auto CreateCreateCheckpointRequest(const TString& checkpointId, ui64 nodeId = RootNodeId)
    {
        auto request = CreateSessionRequest<TEvService::TEvCreateCheckpointRequest>();
        request->Record.SetCheckpointId(checkpointId);
        request->Record.SetNodeId(nodeId);
        return request;
    }

    auto CreateDestroyCheckpointRequest(const TString& checkpointId)
    {
        auto request = CreateSessionRequest<TEvService::TEvDestroyCheckpointRequest>();
        request->Record.SetCheckpointId(checkpointId);
        return request;
    }

    auto CreateSubscribeSessionRequest()
    {
        auto request = CreateSessionRequest<TEvService::TEvSubscribeSessionRequest>();
        return request;
    }

    //
    // Monitoring Http Info
    //

    void SendRemoteHttpInfoRequest(const TString& query = {})
    {
        auto request = std::make_unique<NImpl::TEvRemoteHttpInfoRequest>(
            "/app?" + query);
        SendRequest(std::move(request));
    }

    std::unique_ptr<NImpl::TEvRemoteHttpInfoResponse> RecvRemoteHttpInfoResponse()
    {
        return RecvResponse<NImpl::TEvRemoteHttpInfoResponse>();
    }

    std::unique_ptr<NImpl::TEvRemoteHttpInfoResponse> GetRemoteHttpInfo(TString query = {})
    {
        SendRemoteHttpInfoRequest(std::move(query));
        return RecvRemoteHttpInfoResponse();
    }

    //
    // Time based functions
    //

    void AdvanceTime(TDuration duration)
    {
        Runtime.AdvanceCurrentTime(duration);
    }

    void DispatchEvents()
    {
        Runtime.DispatchEvents({}, TDuration::Zero());
    }

#define FILESTORE_DECLARE_METHOD(name, ns)                                     \
    template <typename... Args>                                                \
    void Send##name##Request(Args&&... args)                                   \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        SendRequest(std::move(request));                                       \
    }                                                                          \
                                                                               \
    std::unique_ptr<ns::TEv##name##Response> Recv##name##Response()            \
    {                                                                          \
        return RecvResponse<ns::TEv##name##Response>();                        \
    }                                                                          \
                                                                               \
    template <typename... Args>                                                \
    std::unique_ptr<ns::TEv##name##Response> SendAndRecv##name(Args&&... args) \
    {                                                                          \
        Send##name##Request(std::forward<Args>(args)...);                      \
        return Recv##name##Response();                                         \
    }                                                                          \
                                                                               \
    template <typename... Args>                                                \
    std::unique_ptr<ns::TEv##name##Response> name(Args&&... args)              \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        SendRequest(std::move(request));                                       \
                                                                               \
        auto response = Recv##name##Response();                                \
        UNIT_ASSERT_C(                                                         \
            SUCCEEDED(response->GetStatus()),                                  \
            response->GetErrorReason());                                       \
        return response;                                                       \
    }                                                                          \
                                                                               \
    template <typename... Args>                                                \
    std::unique_ptr<ns::TEv##name##Response> Assert##name##Failed(             \
        Args&&... args)                                                        \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        auto dbg = NImpl::DebugDump(*request);                                 \
        SendRequest(std::move(request));                                       \
                                                                               \
        auto response = Recv##name##Response();                                \
        UNIT_ASSERT_C(                                                         \
            FAILED(response->GetStatus()),                                     \
            #name " has not failed as expected " + dbg);                       \
        return response;                                                       \
    }                                                                          \
                                                                               \
    std::unique_ptr<ns::TEv##name##Response> Assert##name##QuickResponse(      \
        ui32 status)                                                           \
    {                                                                          \
        DispatchEvents();                                                      \
        auto evList = Runtime.CaptureEvents();                                 \
                                                                               \
        std::unique_ptr<NActors::IEventHandle> handle;                         \
        for (auto& ev : evList) {                                              \
            if (ev->GetTypeRewrite() == ns::Ev##name##Response) {              \
                handle.reset(ev.Release());                                    \
                break;                                                         \
            }                                                                  \
        }                                                                      \
        UNIT_ASSERT(handle);                                                   \
        auto response = handle->Release<ns::TEv##name##Response>();            \
        UNIT_ASSERT_VALUES_EQUAL(status, response->GetStatus());               \
        Runtime.PushEventsFront(evList);                                       \
                                                                               \
        return std::unique_ptr<ns::TEv##name##Response>(response.Release());   \
    }                                                                          \
                                                                               \
    std::unique_ptr<ns::TEv##name##Response> Assert##name##Response(           \
        ui32 status)                                                           \
    {                                                                          \
        auto response = Recv##name##Response();                                \
        UNIT_ASSERT(response);                                                 \
        UNIT_ASSERT_VALUES_EQUAL(status, response->GetStatus());               \
        return response;                                                       \
    }                                                                          \
                                                                               \
    void Assert##name##NoResponse()                                            \
    {                                                                          \
        DispatchEvents();                                                      \
        auto evList = Runtime.CaptureEvents();                                 \
                                                                               \
        for (auto& ev: evList) {                                               \
            UNIT_ASSERT(                                                       \
                ev->GetTypeRewrite() != ns::TEv##name##Response::EventType     \
            );                                                                 \
        }                                                                      \
                                                                               \
        Runtime.PushEventsFront(evList);                                       \
    }                                                                          \
// FILESTORE_DECLARE_METHOD

    FILESTORE_SERVICE_REQUESTS(FILESTORE_DECLARE_METHOD, TEvService)

    FILESTORE_TABLET_REQUESTS(FILESTORE_DECLARE_METHOD, TEvIndexTablet)
    FILESTORE_TABLET_REQUESTS_PRIVATE(FILESTORE_DECLARE_METHOD, TEvIndexTabletPrivate)

#undef FILESTORE_DECLARE_METHOD

    auto CreateUpdateConfigRequest(const TFileSystemConfig& config)
    {
        auto request = std::make_unique<NKikimr::TEvFileStore::TEvUpdateConfig>();
        auto* c = request->Record.MutableConfig();
        c->SetFileSystemId(config.FileSystemId);
        c->SetCloudId(config.CloudId);
        c->SetFolderId(config.FolderId);
        c->SetBlockSize(config.BlockSize);
        c->SetBlocksCount(config.BlockCount);
        c->SetNodesCount(config.NodeCount);
        c->SetRangeIdHasherType(RangeIdHasherType);
        c->SetStorageMediaKind(config.StorageMediaKind);

        c->SetPerformanceProfileThrottlingEnabled(
            config.PerformanceProfile.ThrottlingEnabled);
        c->SetPerformanceProfileMaxReadIops(
            config.PerformanceProfile.MaxReadIops);
        c->SetPerformanceProfileMaxWriteIops(
            config.PerformanceProfile.MaxWriteIops);
        c->SetPerformanceProfileMaxReadBandwidth(
            config.PerformanceProfile.MaxReadBandwidth);
        c->SetPerformanceProfileMaxWriteBandwidth(
            config.PerformanceProfile.MaxWriteBandwidth);
        c->SetPerformanceProfileBoostTime(
            config.PerformanceProfile.BoostTime);
        c->SetPerformanceProfileBoostRefillTime(
            config.PerformanceProfile.BoostRefillTime);
        c->SetPerformanceProfileBoostPercentage(
            config.PerformanceProfile.BoostPercentage);
        c->SetPerformanceProfileMaxPostponedWeight(
            config.PerformanceProfile.MaxPostponedWeight);
        c->SetPerformanceProfileMaxWriteCostMultiplier(
            config.PerformanceProfile.MaxWriteCostMultiplier);
        c->SetPerformanceProfileMaxPostponedTime(
            config.PerformanceProfile.MaxPostponedTime);
        c->SetPerformanceProfileMaxPostponedCount(
            config.PerformanceProfile.MaxPostponedCount);
        c->SetPerformanceProfileBurstPercentage(
            config.PerformanceProfile.BurstPercentage);
        c->SetPerformanceProfileDefaultPostponedRequestWeight(
            config.PerformanceProfile.DefaultPostponedRequestWeight);

        Y_ABORT_UNLESS(config.ChannelCount >= 4);

        // system [0]
        {
            auto* ecp = c->MutableExplicitChannelProfiles()->Add();
            ecp->SetPoolKind("pool-kind-1");
            ecp->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
        }

        // index [1]
        {
            auto* ecp = c->MutableExplicitChannelProfiles()->Add();
            ecp->SetPoolKind("pool-kind-1");
            ecp->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));
        }

        // fresh [2]
        {
            auto* ecp = c->MutableExplicitChannelProfiles()->Add();
            ecp->SetPoolKind("pool-kind-1");
            ecp->SetDataKind(static_cast<ui32>(EChannelDataKind::Fresh));

        }

        // mixed [3+]
        for (ui32 channel = 3; channel < config.ChannelCount; ++channel) {
            auto* ecp = c->MutableExplicitChannelProfiles()->Add();
            ecp->SetPoolKind("pool-kind-1");
            ecp->SetDataKind(static_cast<ui32>(EChannelDataKind::Mixed));
        }

        return request;
    }

    std::unique_ptr<NKikimr::TEvFileStore::TEvUpdateConfigResponse> UpdateConfig(
        const TFileSystemConfig& config)
    {
        auto request = CreateUpdateConfigRequest(config);
        SendRequest(std::move(request));

        auto response = RecvResponse<NKikimr::TEvFileStore::TEvUpdateConfigResponse>();
        UNIT_ASSERT(response->Record.GetStatus() == NKikimrFileStore::OK);

        return response;
    }

    void ReconnectPipe()
    {
        PipeClient = Runtime.ConnectToPipe(
            TabletId,
            Sender,
            NodeIdx,
            NKikimr::GetPipeConfigWithRetries());
    }
};

////////////////////////////////////////////////////////////////////////////////

inline ui64 CreateNode(TIndexTabletClient& tablet, const TCreateNodeArgs& args)
{
    auto response = tablet.CreateNode(args);
    return response->Record.GetNode().GetId();
}

inline ui64 CreateHandle(
    TIndexTabletClient& tablet,
    ui64 node,
    const TString& name = {},
    ui32 flags = TCreateHandleArgs::RDWR)
{
    auto response = tablet.CreateHandle(node, name, flags);
    return response->Record.GetHandle();
}

inline NProto::TNodeAttr GetNodeAttrs(TIndexTabletClient& tablet, ui64 node)
{
    return tablet.GetNodeAttr(node)->Record.GetNode();
}

inline NProtoPrivate::TStorageStats GetStorageStats(TIndexTabletClient& tablet)
{
    return tablet.GetStorageStats()->Record.GetStats();
}

inline NCloud::NProto::EStorageMediaKind GetStorageMediaKind(TIndexTabletClient& tablet)
{
    return tablet.GetStorageStats()->Record.GetMediaKind();
}

inline NProtoPrivate::TFileSystemConfig GetFileSystemConfig(TIndexTabletClient& tablet)
{
    return tablet.GetFileSystemConfig()->Record.GetConfig();
}

inline TString ReadData(TIndexTabletClient& tablet, ui64 handle, ui64 size, ui64 offset = 0)
{
    auto response = tablet.ReadData(handle, offset, size);
    return response->Record.GetBuffer();
}

}   // namespace NCloud::NFileStore::NStorage
