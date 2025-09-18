#pragma once

#include "helpers.h"
#include "test_env.h"

#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/storage/core/protos/media.pb.h>

#include <contrib/ydb/core/testlib/actors/test_runtime.h>
#include <contrib/ydb/core/testlib/test_client.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TServiceClient
{
private:
    NKikimr::TTestActorRuntime& Runtime;
    ui32 NodeIdx;
    NActors::TActorId Sender;

    static constexpr i32 DefaultPid = 123;

public:
    TServiceClient(NKikimr::TTestActorRuntime& runtime, ui32 nodeIdx)
        : Runtime(runtime)
        , NodeIdx(nodeIdx)
        , Sender(runtime.AllocateEdgeActor(nodeIdx))
    {}

    const NActors::TActorId& GetSender() const
    {
        return Sender;
    }

    template <typename TRequest>
    void SendRequest(
        const NActors::TActorId& recipient,
        std::unique_ptr<TRequest> request,
        ui64 cookie = 0)
    {
        auto* ev = new NActors::IEventHandle(
            recipient,
            Sender,
            request.release(),
            0,          // flags
            cookie,     // cookie
            // forwardOnNondelivery
            nullptr);

        Runtime.UpdateCurrentTime(TInstant::Now());
        Runtime.EnableScheduleForActor(MakeStorageServiceId());
        Runtime.Send(ev, NodeIdx);
    }

    template <typename TResponse>
    auto RecvResponse()
    {
        TAutoPtr<NActors::IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle);
        return std::unique_ptr<TResponse>(handle->Release<TResponse>().Release());
    }

    auto& AccessRuntime()
    {
        return Runtime;
    }

    void RegisterLocalFileStore(
        const TString& fileSystemId,
        ui64 tabletId,
        ui64 generation,
        bool isShard,
        NProtoPrivate::TFileSystemConfig config)
    {
        auto request =
            std::make_unique<TEvService::TEvRegisterLocalFileStoreRequest>(
                fileSystemId,
                tabletId,
                generation,
                isShard,
                std::move(config));
        SendRequest(MakeStorageServiceId(), std::move(request));
        Runtime.DispatchEvents({}, TDuration::Seconds(1));
    }

    void UnregisterLocalFileStore(
        const TString& fileSystemId,
        ui64 generation)
    {
        auto request =
            std::make_unique<TEvService::TEvUnregisterLocalFileStoreRequest>(
                fileSystemId,
                generation);
        SendRequest(MakeStorageServiceId(), std::move(request));
        Runtime.DispatchEvents({}, TDuration::Seconds(1));
    }

    THeaders InitSession(
        const TString& fileSystemId,
        const TString& clientId,
        const TString& checkpointId = {},
        bool restoreClientSession = false,
        ui64 sessionSeqNo = 0,
        bool readOnly = false)
    {
        THeaders headers = {fileSystemId, clientId, "", sessionSeqNo};

        auto response = CreateSession(headers, checkpointId, restoreClientSession, sessionSeqNo, readOnly);
        headers.SessionId = response->Record.GetSession().GetSessionId();
        headers.SessionSeqNo = sessionSeqNo;
        return headers;
    }

    std::unique_ptr<TEvService::TEvCreateSessionResponse> InitSession(
        THeaders& headers,
        const TString& fileSystemId,
        const TString& clientId,
        const TString& checkpointId = {},
        bool restoreClientSession = false,
        ui64 sessionSeqNo = 0,
        bool readOnly = false)
    {
        headers = {fileSystemId, clientId, "", sessionSeqNo};

        auto response = CreateSession(headers, checkpointId, restoreClientSession, sessionSeqNo, readOnly);
        headers.SessionId = response->Record.GetSession().GetSessionId();
        headers.SessionSeqNo = sessionSeqNo;
        return response;
    }

    //
    // TEvService
    //

    auto CreateCreateFileStoreRequest(
        const TString& fileSystemId,
        ui64 blocksCount,
        ui32 blockSize = DefaultBlockSize,
        NProto::EStorageMediaKind mediaKind = NProto::STORAGE_MEDIA_DEFAULT)
    {
        auto request = std::make_unique<TEvService::TEvCreateFileStoreRequest>();
        request->Record.SetFileSystemId(fileSystemId);
        request->Record.SetCloudId("test_cloud");
        request->Record.SetFolderId("test_folder");
        request->Record.SetBlockSize(blockSize);
        request->Record.SetBlocksCount(blocksCount);
        request->Record.SetStorageMediaKind(mediaKind);
        return request;
    }

    auto CreateAlterFileStoreRequest(
        const TString& fileSystemId,
        const TString& cloud,
        const TString& folder)
    {
        auto request = std::make_unique<TEvService::TEvAlterFileStoreRequest>();
        request->Record.SetFileSystemId(fileSystemId);
        request->Record.SetCloudId(cloud);
        request->Record.SetFolderId(folder);
        return request;
    }

    auto CreateResizeFileStoreRequest(
        const TString& fileSystemId,
        ui64 blocksCount,
        bool force = false,
        ui32 shardCount = 0,
        bool turnOnStrictSizeMode = false)
    {
        auto request = std::make_unique<TEvService::TEvResizeFileStoreRequest>();
        request->Record.SetFileSystemId(fileSystemId);
        request->Record.SetBlocksCount(blocksCount);
        request->Record.SetForce(force);
        request->Record.SetShardCount(shardCount);
        request->Record.SetTurnOnStrictFileSystemSizeEnforcement(
            turnOnStrictSizeMode);
        return request;
    }

    auto CreateDestroyFileStoreRequest(
        const TString& fileSystemId,
        bool forceDestroy = false)
    {
        auto request =
            std::make_unique<TEvService::TEvDestroyFileStoreRequest>();
        request->Record.SetFileSystemId(fileSystemId);
        request->Record.SetForceDestroy(forceDestroy);
        return request;
    }

    auto CreateGetFileStoreInfoRequest(const TString& fileSystemId)
    {
        auto request = std::make_unique<TEvService::TEvGetFileStoreInfoRequest>();
        request->Record.SetFileSystemId(fileSystemId);
        return request;
    }

    auto CreateListFileStoresRequest()
    {
        auto request = std::make_unique<TEvService::TEvListFileStoresRequest>();
        return request;
    }

    auto CreateDescribeFileStoreModelRequest(
        ui64 blocksCount,
        ui32 blockSize = DefaultBlockSize)
    {
        auto request = std::make_unique<TEvService::TEvDescribeFileStoreModelRequest>();
        request->Record.SetBlocksCount(blocksCount);
        request->Record.SetBlockSize(blockSize);
        return request;
    }

    auto CreateCreateSessionRequest(
        const THeaders& headers,
        const TString& checkpointId = {},
        bool restoreClientSession = false,
        ui64 sessionSeqNo = 0,
        bool readOnly = false)
    {
        auto request = std::make_unique<TEvService::TEvCreateSessionRequest>();
        request->Record.SetFileSystemId(headers.FileSystemId);
        request->Record.SetCheckpointId(checkpointId);
        request->Record.SetRestoreClientSession(restoreClientSession);
        request->Record.SetMountSeqNumber(sessionSeqNo);
        request->Record.SetReadOnly(readOnly);
        headers.Fill(request->Record);
        return request;
    }

    auto CreatePingSessionRequest(const THeaders& headers)
    {
        auto request = std::make_unique<TEvService::TEvPingSessionRequest>();
        request->Record.SetFileSystemId(headers.FileSystemId);
        headers.Fill(request->Record);
        return request;
    }

    auto CreateDestroySessionRequest(const THeaders& headers)
    {
        auto request = std::make_unique<TEvService::TEvDestroySessionRequest>();
        request->Record.SetFileSystemId(headers.FileSystemId);
        headers.Fill(request->Record);
        return request;
    }

    auto CreateSubscribeSessionRequest(const THeaders& headers)
    {
        auto request = std::make_unique<TEvService::TEvSubscribeSessionRequest>();
        request->Record.SetFileSystemId(headers.FileSystemId);
        headers.Fill(request->Record);
        return request;
    }

    auto CreateGetSessionEventsRequest(const THeaders& headers, ui64 seqNo = 0)
    {
        auto request = std::make_unique<TEvService::TEvGetSessionEventsRequest>();
        request->Record.SetFileSystemId(headers.FileSystemId);
        request->Record.SetSeqNo(seqNo);
        headers.Fill(request->Record);
        return request;
    }

    auto CreateCreateNodeRequest(
        const THeaders& headers,
        const TCreateNodeArgs& args,
        const ui64 requestId = 0)
    {
        auto request = std::make_unique<TEvService::TEvCreateNodeRequest>();
        request->Record.SetFileSystemId(headers.FileSystemId);
        headers.Fill(request->Record);
        request->Record.MutableHeaders()->SetRequestId(requestId);
        args.Fill(request->Record);
        return request;
    }

    auto CreateUnlinkNodeRequest(
        const THeaders& headers,
        const ui64 parent,
        const TString& name,
        bool unlinkDirectory = false,
        const ui64 requestId = 0)
    {
        auto request = std::make_unique<TEvService::TEvUnlinkNodeRequest>();
        request->Record.SetFileSystemId(headers.FileSystemId);
        headers.Fill(request->Record);
        request->Record.MutableHeaders()->SetRequestId(requestId);
        request->Record.SetNodeId(parent);
        request->Record.SetName(name);
        request->Record.SetUnlinkDirectory(unlinkDirectory);
        return request;
    }

    auto CreateRenameNodeRequest(
        const THeaders& headers,
        const ui64 parent,
        const TString& name,
        const ui64 newParent,
        const TString& newName,
        ui32 flags)
    {
        auto request = std::make_unique<TEvService::TEvRenameNodeRequest>();
        request->Record.SetFileSystemId(headers.FileSystemId);
        headers.Fill(request->Record);
        request->Record.SetNodeId(parent);
        request->Record.SetName(name);
        request->Record.SetNewParentId(newParent);
        request->Record.SetNewName(newName);
        request->Record.SetFlags(flags);
        return request;
    }

    static auto CreateWriteDataRequest(
        const THeaders& headers,
        const TString& fileSystemId,
        ui64 nodeId,
        ui64 handle,
        ui64 offset,
        const TString& buffer)
    {
        auto request = std::make_unique<TEvService::TEvWriteDataRequest>();
        headers.Fill(request->Record);
        request->Record.SetFileSystemId(fileSystemId);
        request->Record.SetNodeId(nodeId);
        request->Record.SetHandle(handle);
        request->Record.SetOffset(offset);
        request->Record.SetBuffer(buffer);
        return request;
    }

    static auto CreateCreateHandleRequest(
        const THeaders& headers,
        const TString& fileSystemId,
        ui64 nodeId,
        const TString& name,
        ui32 flags,
        const TString& shardId = "",
        const ui64 requestId = 0)
    {
        auto request = std::make_unique<TEvService::TEvCreateHandleRequest>();
        headers.Fill(request->Record);
        request->Record.MutableHeaders()->SetRequestId(requestId);
        request->Record.SetFileSystemId(fileSystemId);
        request->Record.SetNodeId(nodeId);
        request->Record.SetName(name);
        request->Record.SetFlags(flags);
        request->Record.SetShardFileSystemId(shardId);
        return request;
    }

    static auto CreateReadDataRequest(
        const THeaders& headers,
        const TString& fileSystemId,
        ui64 nodeId,
        ui64 handle,
        ui64 offset,
        ui64 length)
    {
        auto request = std::make_unique<TEvService::TEvReadDataRequest>();
        headers.Fill(request->Record);
        request->Record.SetFileSystemId(fileSystemId);
        request->Record.SetNodeId(nodeId);
        request->Record.SetHandle(handle);
        request->Record.SetOffset(offset);
        request->Record.SetLength(length);
        return request;
    }

    auto CreateListNodesRequest(const THeaders& headers, ui64 nodeId)
    {
        auto request = std::make_unique<TEvService::TEvListNodesRequest>();
        request->Record.SetFileSystemId(headers.FileSystemId);
        headers.Fill(request->Record);
        request->Record.SetNodeId(nodeId);
        return request;
    }

    auto CreateResetSessionRequest(const THeaders& headers, TString state)
    {
        auto request = std::make_unique<TEvService::TEvResetSessionRequest>();
        request->Record.SetFileSystemId(headers.FileSystemId);
        request->Record.SetSessionState(std::move(state));
        headers.Fill(request->Record);
        return request;
    }

    std::unique_ptr<TEvService::TEvExecuteActionRequest> CreateExecuteActionRequest(
        const TString& action,
        const TString& input)
    {
        auto request = std::make_unique<TEvService::TEvExecuteActionRequest>();
        request->Record.SetAction(action);
        request->Record.SetInput(input);
        return request;
    }

    std::unique_ptr<TEvService::TEvGetNodeAttrRequest> CreateGetNodeAttrRequest(
        const THeaders& headers,
        const TString& fileSystemId,
        const ui64 nodeId,
        const TString& nodeName)
    {
        auto request = std::make_unique<TEvService::TEvGetNodeAttrRequest>();
        headers.Fill(request->Record);
        request->Record.SetFileSystemId(fileSystemId);
        request->Record.SetNodeId(nodeId);
        request->Record.SetName(nodeName);
        return request;
    }

    std::unique_ptr<TEvService::TEvListNodesRequest> CreateListNodesRequest(
        const THeaders& headers,
        const TString& fileSystemId,
        const ui64 nodeId)
    {
        auto request = std::make_unique<TEvService::TEvListNodesRequest>();
        headers.Fill(request->Record);
        request->Record.SetFileSystemId(fileSystemId);
        request->Record.SetNodeId(nodeId);
        return request;
    }

    std::unique_ptr<TEvService::TEvAccessNodeRequest> CreateAccessNodeRequest(
        const THeaders& headers,
        const TString& fileSystemId,
        const ui64 nodeId)
    {
        auto request = std::make_unique<TEvService::TEvAccessNodeRequest>();
        headers.Fill(request->Record);
        request->Record.SetFileSystemId(fileSystemId);
        request->Record.SetNodeId(nodeId);
        return request;
    }

    std::unique_ptr<TEvService::TEvSetNodeAttrRequest> CreateSetNodeAttrRequest(
        const THeaders& headers,
        const TString& fileSystemId,
        const ui64 nodeId,
        const ui64 size)
    {
        auto request = std::make_unique<TEvService::TEvSetNodeAttrRequest>();
        headers.Fill(request->Record);
        request->Record.SetFileSystemId(fileSystemId);
        request->Record.SetNodeId(nodeId);
        request->Record.MutableUpdate()->SetSize(size);
        request->Record.SetFlags(
            ProtoFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE));
        return request;
    }

    std::unique_ptr<TEvService::TEvSetNodeAttrRequest> CreateSetNodeAttrRequest(
        const THeaders& headers,
        const TString& fileSystemId,
        const TSetNodeAttrArgs& args)
    {
        auto request = std::make_unique<TEvService::TEvSetNodeAttrRequest>();
        headers.Fill(request->Record);
        request->Record.SetFileSystemId(fileSystemId);

        args.Fill(request->Record);
        return request;
    }

    std::unique_ptr<TEvService::TEvDestroyHandleRequest> CreateDestroyHandleRequest(
        const THeaders& headers,
        const TString& fileSystemId,
        const ui64 nodeId,
        const ui64 handle)
    {
        auto request = std::make_unique<TEvService::TEvDestroyHandleRequest>();
        headers.Fill(request->Record);
        request->Record.SetFileSystemId(fileSystemId);
        request->Record.SetNodeId(nodeId);
        request->Record.SetHandle(handle);
        return request;
    }

    std::unique_ptr<TEvService::TEvAllocateDataRequest> CreateAllocateDataRequest(
        const THeaders& headers,
        const TString& fileSystemId,
        const ui64 nodeId,
        const ui64 handle,
        const ui64 offset,
        const ui64 len)
    {
        auto request = std::make_unique<TEvService::TEvAllocateDataRequest>();
        headers.Fill(request->Record);
        request->Record.SetFileSystemId(fileSystemId);
        request->Record.SetNodeId(nodeId);
        request->Record.SetHandle(handle);
        request->Record.SetOffset(offset);
        request->Record.SetLength(len);
        return request;
    }

    std::unique_ptr<TEvService::TEvSetNodeXAttrRequest> CreateSetNodeXAttrRequest(
        const THeaders& headers,
        const TString& fileSystemId,
        const ui64 nodeId,
        const TString& attrName,
        const TString& attrValue)
    {
        auto request = std::make_unique<TEvService::TEvSetNodeXAttrRequest>();
        headers.Fill(request->Record);
        request->Record.SetFileSystemId(fileSystemId);
        request->Record.SetNodeId(nodeId);
        request->Record.SetName(attrName);
        request->Record.SetValue(attrValue);
        return request;
    }

    std::unique_ptr<TEvService::TEvGetNodeXAttrRequest> CreateGetNodeXAttrRequest(
        const THeaders& headers,
        const TString& fileSystemId,
        const ui64 nodeId,
        const TString& attrName)
    {
        auto request = std::make_unique<TEvService::TEvGetNodeXAttrRequest>();
        headers.Fill(request->Record);
        request->Record.SetFileSystemId(fileSystemId);
        request->Record.SetNodeId(nodeId);
        request->Record.SetName(attrName);
        return request;
    }

    std::unique_ptr<TEvService::TEvRemoveNodeXAttrRequest> CreateRemoveNodeXAttrRequest(
        const THeaders& headers,
        const TString& fileSystemId,
        const ui64 nodeId,
        const TString& attrName)
    {
        auto request = std::make_unique<TEvService::TEvRemoveNodeXAttrRequest>();
        headers.Fill(request->Record);
        request->Record.SetFileSystemId(fileSystemId);
        request->Record.SetNodeId(nodeId);
        request->Record.SetName(attrName);
        return request;
    }

    std::unique_ptr<TEvService::TEvListNodeXAttrRequest> CreateListNodeXAttrRequest(
        const THeaders& headers,
        const TString& fileSystemId,
        const ui64 nodeId)
    {
        auto request = std::make_unique<TEvService::TEvListNodeXAttrRequest>();
        headers.Fill(request->Record);
        request->Record.SetFileSystemId(fileSystemId);
        request->Record.SetNodeId(nodeId);
        return request;
    }

    std::unique_ptr<TEvService::TEvStatFileStoreRequest> CreateStatFileStoreRequest(
        const THeaders& headers,
        const TString& fileSystemId)
    {
        auto request = std::make_unique<TEvService::TEvStatFileStoreRequest>();
        headers.Fill(request->Record);
        request->Record.SetFileSystemId(fileSystemId);
        return request;
    }

    std::unique_ptr<TEvService::TEvAcquireLockRequest> CreateAcquireLockRequest(
        const THeaders& headers,
        const TString& fileSystemId,
        ui64 handle,
        ui64 owner,
        ui64 offset,
        ui64 len,
        i32 pid = DefaultPid,
        NProto::ELockType type = NProto::E_EXCLUSIVE,
        NProto::ELockOrigin origin = NProto::E_FCNTL)
    {
        auto request = std::make_unique<TEvService::TEvAcquireLockRequest>();
        headers.Fill(request->Record);
        auto& record = request->Record;
        record.SetFileSystemId(fileSystemId);
        record.SetHandle(handle);
        record.SetOwner(owner);
        record.SetOffset(offset);
        record.SetLength(len);
        record.SetPid(pid);
        record.SetLockType(type);
        record.SetLockOrigin(origin);
        return request;
    }

    std::unique_ptr<TEvService::TEvReleaseLockRequest> CreateReleaseLockRequest(
        const THeaders& headers,
        const TString& fileSystemId,
        ui64 handle,
        ui64 owner,
        ui64 offset,
        ui64 len,
        i32 pid = DefaultPid,
        NProto::ELockOrigin origin = NProto::E_FCNTL)
    {
        auto request = std::make_unique<TEvService::TEvReleaseLockRequest>();
        headers.Fill(request->Record);
        auto& record = request->Record;
        record.SetFileSystemId(fileSystemId);
        record.SetHandle(handle);
        record.SetOwner(owner);
        record.SetOffset(offset);
        record.SetLength(len);
        record.SetPid(pid);
        record.SetLockOrigin(origin);
        return request;
    }

    std::unique_ptr<TEvService::TEvTestLockRequest> CreateTestLockRequest(
        const THeaders& headers,
        const TString& fileSystemId,
        ui64 handle,
        ui64 owner,
        ui64 offset,
        ui64 len,
        i32 pid = DefaultPid,
        NProto::ELockType type = NProto::E_EXCLUSIVE,
        NProto::ELockOrigin origin = NProto::E_FCNTL)
    {
        auto request = std::make_unique<TEvService::TEvTestLockRequest>();
        headers.Fill(request->Record);
        auto& record = request->Record;
        record.SetFileSystemId(fileSystemId);
        record.SetHandle(handle);
        record.SetOwner(owner);
        record.SetOffset(offset);
        record.SetLength(len);
        record.SetPid(pid);
        record.SetLockType(type);
        record.SetLockOrigin(origin);
        return request;
    }

#define FILESTORE_DECLARE_METHOD(name, ns)                                     \
    template <typename... Args>                                                \
    void Send##name##Request(Args&&... args)                                   \
    {                                                                          \
        auto request = Create##name##Request(std::forward<Args>(args)...);     \
        request->CallContext->RequestType = EFileStoreRequest::name;           \
        SendRequest(MakeStorageServiceId(), std::move(request));               \
    }                                                                          \
                                                                               \
    void Send##name##Request(std::unique_ptr<ns::TEv##name##Request> request)  \
    {                                                                          \
        request->CallContext->RequestType = EFileStoreRequest::name;           \
        SendRequest(MakeStorageServiceId(), std::move(request));               \
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
        request->CallContext->RequestType = EFileStoreRequest::name;           \
        SendRequest(MakeStorageServiceId(), std::move(request));               \
                                                                               \
        auto response = RecvResponse<ns::TEv##name##Response>();               \
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
        request->CallContext->RequestType = EFileStoreRequest::name;           \
        auto dbg = request->Record.ShortDebugString();                         \
        SendRequest(MakeStorageServiceId(), std::move(request));               \
                                                                               \
        auto response = RecvResponse<ns::TEv##name##Response>();               \
        UNIT_ASSERT_C(                                                         \
            FAILED(response->GetStatus()),                                     \
            #name " has not failed as expected " + dbg);                       \
        return response;                                                       \
    }                                                                          \
// FILESTORE_DECLARE_METHOD

    FILESTORE_REMOTE_SERVICE(FILESTORE_DECLARE_METHOD, TEvService)

#undef FILESTORE_DECLARE_METHOD

    template <typename... Args>
    std::unique_ptr<TEvService::TEvGetSessionEventsResponse> GetSessionEventsStream(Args&&... args)
    {
        auto request = CreateGetSessionEventsRequest(std::forward<Args>(args)...);
        SendRequest(MakeStorageServiceId(), std::move(request), TEvService::StreamCookie);

        auto response = RecvResponse<TEvService::TEvGetSessionEventsResponse>();
        UNIT_ASSERT_C(
            SUCCEEDED(response->GetStatus()),
            response->GetErrorReason());
        return response;
    }
};

}   // namespace NCloud::NFileStore::NStorage
