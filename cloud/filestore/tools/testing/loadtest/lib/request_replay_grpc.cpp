/*

TODO:
create file/dir modes
create handle modes (now rw)
compare log and actual result ( S_OK E_FS_NOENT ...)

*/

#include "request.h"

#include "request_replay.h"

#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/public/api/protos/data.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>
#include <cloud/filestore/tools/analytics/libs/event-log/dump.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/guid.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/size_literals.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/system/fstat.h>
#include <util/system/mutex.h>

#include <atomic>
#include <utility>

namespace NCloud::NFileStore::NLoadTest {

using namespace NThreading;
using namespace NCloud::NFileStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReplayRequestGeneratorGRPC final
    : public IReplayRequestGenerator
    , public std::enable_shared_from_this<TReplayRequestGeneratorGRPC>
{
private:
    TMutex StateLock;

    using THandleLog = ui64;
    using THandleLocal = ui64;
    using TNodeLog = ui64;
    using TNodeLocal = ui64;

    ui64 InitialFileSize = 0;

    std::atomic<ui64> LastRequestId = 0;

    static constexpr ui32 LockLength = 4096;

    const ui64 OwnerId = RandomNumber(100500u);

    struct THandle
    {
        TString Path;
    };

    THashMap<ui64, THandle> Handles;
    THashSet<ui64> Locks;
    THashSet<ui64> StagedLocks;

    THashMap<TNodeLog, TNodeLocal> NodesLogToLocal{{RootNodeId, RootNodeId}};

    THashMap<THandleLog, THandleLocal> HandlesLogToActual;

public:
    TReplayRequestGeneratorGRPC(
            NProto::TReplaySpec spec,
            ILoggingServicePtr logging,
            ISessionPtr session,
            TString filesystemId,
            NProto::THeaders headers)
        : IReplayRequestGenerator(
              std::move(spec),
              std::move(logging),
              std::move(session),
              std::move(filesystemId),
              std::move(headers))
    {
        if (!Session) {
            ythrow yexception() << "Session not created. Missing FileSystemId?";
        }
    }

    TNodeLocal NodeIdMapped(const TNodeLog id)
    {
        TGuard<TMutex> guard(StateLock);

        if (const auto it = NodesLogToLocal.find(id);
            it != NodesLogToLocal.end())
        {
            return it->second;
        }

        STORAGE_DEBUG(
            "Node not found id=%lu map size=%zu",
            id,
            NodesLogToLocal.size());

        return InvalidNodeId;
    }

    THandleLocal HandleIdMapped(const THandleLog id)
    {
        TGuard<TMutex> guard(StateLock);

        if (const auto it = HandlesLogToActual.find(id);
            it != HandlesLogToActual.end())
        {
            return it->second;
        }

        STORAGE_DEBUG(
            "Handle not found id=%lu map size=%zu",
            id,
            HandlesLogToActual.size());

        return InvalidHandle;
    }

private:
    struct TRequestInfo
    {
        TInstant Started;
        ssize_t EventMessageNumber = 0;
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo LogRequest;
        TString Description;
    };

    TFuture<TCompletedRequest> DoAccessNode(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        // nfs     AccessNode      0.002297s       S_OK    {mask=4, node_id=36}

        if (Spec.GetSkipRead()) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_ACCESS_NODE,
                    Started,
                    MakeError(E_PRECONDITION_FAILED, "read disabled")});
        }

        const auto nodeId = NodeIdMapped(logRequest.GetNodeInfo().GetNodeId());
        if (nodeId == InvalidNodeId) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_ACCESS_NODE,
                    Started,
                    MakeError(E_NOT_FOUND, "node not found")});
        }

        auto request = CreateRequest<NProto::TAccessNodeRequest>();
        request->SetNodeId(nodeId);
        // TODO(proller): where is mask in logRequest?
        // request->SetMask(logRequest.GetNodeInfo().);
        auto self = weak_from_this();
        const auto future =
            Session->AccessNode(CreateCallContext(), std::move(request))
                .Apply(
                    [self,
                     info =
                         TRequestInfo{
                             Started,
                             EventMessageNumber,
                             logRequest,
                             ToString(nodeId)}

        ](const TFuture<NProto::TAccessNodeResponse>& future)
                    {
                        if (auto ptr = self.lock()) {
                            return ptr->HandleAccessNode(future, info);
                        }

                        return TCompletedRequest{
                            NProto::ACTION_ACCESS_NODE,
                            info.Started,
                            MakeError(E_INVALID_STATE, "cancelled")};
                    });
        const auto& response = future.GetValueSync();
        return MakeFuture(
            TCompletedRequest{
                NProto::ACTION_ACCESS_NODE,
                Started,
                response.Error});
    }

    TCompletedRequest HandleAccessNode(
        const TFuture<NProto::TAccessNodeResponse>& future,
        const TRequestInfo& info)
    {
        try {
            const auto& response = future.GetValue();
            CompareResponse(info, response.GetError().GetCode());
            CheckResponse(response);
            return {
                NProto::ACTION_ACCESS_NODE,
                info.Started,
                response.GetError()};
        } catch (const TServiceError& e) {
            CompareResponse(info, e.GetCode());
            return {
                NProto::ACTION_ACCESS_NODE,
                info.Started,
                MakeError(e.GetCode(), TString{e.GetMessage()})};
        }
    }

    TFuture<TCompletedRequest> DoCreateHandle(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        // {"TimestampMcs":1726503808715698,"DurationMcs":2622,"RequestType":38,"ErrorCode":0,"NodeInfo":{"ParentNodeId":13882,"NodeName":"compile_commands.json.tmpdf020","Flags":38,"Mode":436,"NodeId":15553,"Handle":46923415058768564,"Size":0}}

        const auto request = CreateRequest<NProto::TCreateHandleRequest>();
        auto name = logRequest.GetNodeInfo().GetNodeName();

        const auto nodeId =
            NodeIdMapped(logRequest.GetNodeInfo().GetParentNodeId());
        if (nodeId == InvalidNodeId) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_CREATE_HANDLE,
                    Started,
                    MakeError(E_NOT_FOUND, "node not found")});
        }
        request->SetNodeId(nodeId);
        request->SetName(logRequest.GetNodeInfo().GetNodeName());
        request->SetFlags(logRequest.GetNodeInfo().GetFlags());
        request->SetMode(logRequest.GetNodeInfo().GetMode());

        STORAGE_DEBUG(
            "Open %s handle=%lu flags=%d (%s) mode=%d node=%lu <- %lu",
            name.c_str(),
            logRequest.GetNodeInfo().GetHandle(),
            logRequest.GetNodeInfo().GetFlags(),
            HandleFlagsToString(logRequest.GetNodeInfo().GetFlags()).c_str(),
            logRequest.GetNodeInfo().GetMode(),
            nodeId,
            logRequest.GetNodeInfo().GetNodeId());

        const auto self = weak_from_this();
        const auto future =
            Session->CreateHandle(CreateCallContext(), request)
                .Apply(
                    [self,
                     info =
                         TRequestInfo{
                             Started,
                             EventMessageNumber,
                             logRequest,
                             name}](
                        const TFuture<NProto::TCreateHandleResponse>& future)
                    {
                        if (auto ptr = self.lock()) {
                            return ptr->HandleCreateHandle(future, info);
                        }

                        return MakeFuture(
                            TCompletedRequest{
                                NProto::ACTION_CREATE_HANDLE,
                                info.Started,
                                MakeError(E_INVALID_STATE, "cancelled")});
                    });
        const auto& response = future.GetValueSync();
        return MakeFuture(
            TCompletedRequest{
                NProto::ACTION_CREATE_HANDLE,
                Started,
                response.Error});
    }

    TFuture<TCompletedRequest> HandleCreateHandle(
        const TFuture<NProto::TCreateHandleResponse>& future,
        const TRequestInfo& info)
    {
        try {
            const auto& response = future.GetValue();
            CompareResponse(info, response.GetError().GetCode());
            CheckResponse(response);

            const auto handle = response.GetHandle();
            with_lock (StateLock) {
                HandlesLogToActual[info.LogRequest.GetNodeInfo().GetHandle()] =
                    handle;

                NodesLogToLocal[info.LogRequest.GetNodeInfo().GetNodeId()] =
                    response.GetNodeAttr().GetId();
            }

            TFuture<NProto::TSetNodeAttrResponse> setAttr;
            if (InitialFileSize) {
                static const int Flags =
                    ProtoFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);

                auto request = CreateRequest<NProto::TSetNodeAttrRequest>();
                request->SetHandle(handle);
                request->SetNodeId(response.GetNodeAttr().GetId());
                request->SetFlags(Flags);
                request->MutableUpdate()->SetSize(InitialFileSize);

                setAttr = Session->SetNodeAttr(
                    CreateCallContext(),
                    std::move(request));
            } else {
                setAttr =
                    NThreading::MakeFuture(NProto::TSetNodeAttrResponse());
            }

            return setAttr.Apply(
                [=]([[maybe_unused]] const TFuture<
                    NProto::TSetNodeAttrResponse>& f)
                {
                    // TODO(proller): return HandleResizeAfterCreateHandle(f,
                    // name, started);
                    return MakeFuture(
                        TCompletedRequest{
                            NProto::ACTION_CREATE_HANDLE,
                            info.Started,
                            f.GetValue().GetError()});
                });
        } catch (const TServiceError& e) {
            CompareResponse(info, e.GetCode());
            return NThreading::MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_CREATE_HANDLE,
                    info.Started,
                    MakeError(e.GetCode(), TString{e.GetMessage()})});
        }
    }

    TFuture<TCompletedRequest> DoReadData(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        if (Spec.GetSkipRead()) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_READ,
                    Started,
                    MakeError(E_PRECONDITION_FAILED, "read disabled")});
        }

        const auto handle =
            HandleIdMapped(logRequest.GetNodeInfo().GetHandle());
        if (handle == InvalidHandle) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_READ,
                    Started,
                    MakeError(E_NOT_FOUND, "handle not found")});
        }

        auto request = CreateRequest<NProto::TReadDataRequest>();
        request->SetHandle(handle);
        request->SetOffset(logRequest.GetRanges().cbegin()->GetOffset());
        request->SetLength(logRequest.GetRanges().cbegin()->GetBytes());

        auto self = weak_from_this();
        return Session->ReadData(CreateCallContext(), std::move(request))
            .Apply(
                [self,
                 info =
                     TRequestInfo{
                         Started,
                         EventMessageNumber,
                         logRequest,
                         ToString(handle)}](
                    const TFuture<NProto::TReadDataResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleRead(future, info);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_READ,
                        info.Started,
                        future.GetValue().GetError()};
                });
    }

    TCompletedRequest HandleRead(
        const TFuture<NProto::TReadDataResponse>& future,
        const TRequestInfo& info)
    {
        try {
            const auto& response = future.GetValue();
            CompareResponse(info, response.GetError().GetCode());
            CheckResponse(response);
            return {NProto::ACTION_READ, info.Started, response.GetError()};
        } catch (const TServiceError& e) {
            CompareResponse(info, e.GetCode());
            return {
                NProto::ACTION_READ,
                info.Started,
                MakeError(e.GetCode(), TString{e.GetMessage()})};
        }
    }

    static TString
    MakeBuffer(ui64 bytes, ui64 offset = 0, const TString& start = {})
    {
        TStringBuilder ret;
        ret << "[\n" << start;
        while (ret.size() < bytes) {
            ret << " . " << ret.size() << " : " << offset + ret.size();
        }
        return ret.substr(0, bytes);
    }

    TFuture<TCompletedRequest> DoWriteData(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        //{"TimestampMcs":1465489895000,"DurationMcs":2790,"RequestType":44,"Ranges":[{"NodeId":2,"Handle":20680158862113389,"Offset":13,"Bytes":12}]}

        if (Spec.GetSkipWrite()) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_WRITE,
                    Started,
                    MakeError(E_PRECONDITION_FAILED, "write disabled")});
        }

        const auto handleLog = logRequest.GetRanges(0).GetHandle();
        const auto handleLocal = HandleIdMapped(handleLog);

        if (handleLocal == InvalidHandle) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_WRITE,
                    Started,
                    MakeError(E_NOT_FOUND, "handle not found")});
        }
        auto request = CreateRequest<NProto::TWriteDataRequest>();
        request->SetHandle(handleLocal);

        const auto offset = logRequest.GetRanges(0).GetOffset();
        request->SetOffset(offset);
        const auto bytes = logRequest.GetRanges(0).GetBytes();

        TString buffer;

        if (Spec.GetWriteFill() == NProto::TReplaySpec_EWriteFill_Random) {
            buffer = NUnitTest::RandomString(bytes, handleLog);
        } else if (Spec.GetWriteFill() == NProto::TReplaySpec_EWriteFill_Empty)
        {
            buffer = TString{bytes, ' '};
        } else {
            buffer = MakeBuffer(
                bytes,
                offset,
                TStringBuilder{} << "handle=" << handleLog << " node="
                                 << logRequest.GetNodeInfo().GetNodeId()
                                 << " bytes=" << bytes << " offset=" << offset);
        }

        *request->MutableBuffer() = std::move(buffer);

        const auto self = weak_from_this();
        return Session->WriteData(CreateCallContext(), std::move(request))
            .Apply(
                [self,
                 info =
                     TRequestInfo{
                         Started,
                         EventMessageNumber,
                         logRequest,
                         ToString(handleLog)}](
                    const TFuture<NProto::TWriteDataResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleWrite(future, info);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_WRITE,
                        info.Started,
                        MakeError(E_INVALID_STATE, "cancelled")};
                });
    }

    TCompletedRequest HandleWrite(
        const TFuture<NProto::TWriteDataResponse>& future,
        const TRequestInfo& info)
    {
        try {
            const auto& response = future.GetValue();
            CompareResponse(info, response.GetError().GetCode());
            CheckResponse(response);
            return {NProto::ACTION_WRITE, info.Started, response.GetError()};
        } catch (const TServiceError& e) {
            CompareResponse(info, e.GetCode());
            return {
                NProto::ACTION_WRITE,
                info.Started,
                MakeError(e.GetCode(), TString{e.GetMessage()})};
        }
    }

    TFuture<TCompletedRequest> DoCreateNode(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        // {"TimestampMcs":1725895166478218,"DurationMcs":6328,"RequestType":26,"ErrorCode":0,"NodeInfo":{"NewParentNodeId":1,"NewNodeName":"home","Mode":509,"NodeId":12526,"Size":0}}
        // nfs     CreateNode      0.006404s       S_OK {new_parent_node_id=1,
        // new_node_name=home, mode=509, node_id=12526, size=0}

        if (Spec.GetSkipWrite()) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_CREATE_NODE,
                    Started,
                    MakeError(E_PRECONDITION_FAILED, "write disabled")});
        }

        const auto parentNode =
            NodeIdMapped(logRequest.GetNodeInfo().GetNewParentNodeId());
        if (parentNode == InvalidNodeId) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_CREATE_NODE,
                    Started,
                    MakeError(E_NOT_FOUND, "node not found")});
        }

        auto request = CreateRequest<NProto::TCreateNodeRequest>();
        request->SetNodeId(parentNode);
        const auto name = logRequest.GetNodeInfo().GetNewNodeName();
        request->SetName(name);

        // TODO(proller):
        // request->SetGid();
        // request->SetUid();

        switch (logRequest.GetNodeInfo().GetType()) {
            case NProto::E_REGULAR_NODE:
                request->MutableFile()->SetMode(
                    logRequest.GetNodeInfo().GetMode());
                break;
            case NProto::E_DIRECTORY_NODE:
                request->MutableDirectory()->SetMode(
                    logRequest.GetNodeInfo().GetMode());
                break;
            case NProto::E_LINK_NODE: {
                // type=26 name=CreateNode data="TimestampMcs: 1729271080782158
                // DurationMcs: 2496 RequestType: 26 ErrorCode: 0 NodeInfo {
                // NewParentNodeId: 33 NewNodeName:
                // \"pack-63f1.idx\" Mode:
                // 292 NodeId: 42 Size: 5804 Type: 3 }"
                const auto node =
                    NodeIdMapped(logRequest.GetNodeInfo().GetNodeId());

                if (node == InvalidNodeId) {
                    return MakeFuture(
                        TCompletedRequest{
                            NProto::ACTION_CREATE_NODE,
                            Started,
                            MakeError(E_NOT_FOUND, "node not found")});
                }

                request->MutableLink()->SetTargetNode(node);
                break;
            }
            case NProto::E_SYMLINK_NODE:
                // TODO(proller):
                //  request->MutableSymlink()->SetTargetPath();
                break;
            case NProto::E_SOCK_NODE:
                request->MutableSocket()->SetMode(
                    logRequest.GetNodeInfo().GetMode());
                break;
            case NProto::E_INVALID_NODE:
                return MakeFuture(
                    TCompletedRequest(NProto::ACTION_CREATE_NODE, Started, {}));
                // Do not create files with invalid
                // type - too hard to delete them
                break;
        }

        const auto self = weak_from_this();
        const auto future =
            Session->CreateNode(CreateCallContext(), std::move(request))
                .Apply(
                    [self,
                     info =
                         TRequestInfo{
                             Started,
                             EventMessageNumber,
                             logRequest,
                             name}](
                        const TFuture<NProto::TCreateNodeResponse>& future)
                    {
                        if (auto ptr = self.lock()) {
                            return ptr->HandleCreateNode(future, info);
                        }

                        return TCompletedRequest{
                            NProto::ACTION_CREATE_NODE,
                            info.Started,
                            MakeError(E_INVALID_STATE, "cancelled")};
                    });

        const auto& response = future.GetValueSync();
        return MakeFuture(
            TCompletedRequest{
                NProto::ACTION_CREATE_NODE,
                Started,
                response.Error});
    }

    TCompletedRequest HandleCreateNode(
        const TFuture<NProto::TCreateNodeResponse>& future,
        const TRequestInfo& info)
    {
        try {
            const auto& response = future.GetValue();
            CompareResponse(info, response.GetError().GetCode());
            CheckResponse(response);

            if (response.GetNode().GetId()) {
                with_lock (StateLock) {
                    NodesLogToLocal[info.LogRequest.GetNodeInfo().GetNodeId()] =
                        response.GetNode().GetId();
                }
            }

            return {
                NProto::ACTION_CREATE_NODE,
                info.Started,
                response.GetError()};
        } catch (const TServiceError& e) {
            CompareResponse(info, e.GetCode());
            return {
                NProto::ACTION_CREATE_NODE,
                info.Started,
                MakeError(e.GetCode(), TString{e.GetMessage()})};
        }
    }

    TFuture<TCompletedRequest> DoRenameNode(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        // {"TimestampMcs":895166000,"DurationMcs":2949,"RequestType":28,"NodeInfo":{"ParentNodeId":3,"NodeName":"HEAD.lock","NewParentNodeId":3,"NewNodeName":"HEAD"}}
        if (Spec.GetSkipRead()) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_RENAME_NODE,
                    Started,
                    MakeError(E_PRECONDITION_FAILED, "read disabled")});
        }

        const auto node =
            NodeIdMapped(logRequest.GetNodeInfo().GetParentNodeId());
        if (node == InvalidNodeId) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_RENAME_NODE,
                    Started,
                    MakeError(
                        E_NOT_FOUND,
                        "Log node %d not found in mapping",
                        logRequest.GetNodeInfo().GetParentNodeId())});
        }
        auto request = CreateRequest<NProto::TRenameNodeRequest>();
        request->SetNodeId(node);
        request->SetName(logRequest.GetNodeInfo().GetNodeName());
        request->SetNewParentId(
            NodeIdMapped(logRequest.GetNodeInfo().GetNewParentNodeId()));
        request->SetNewName(logRequest.GetNodeInfo().GetNewNodeName());
        request->SetFlags(logRequest.GetNodeInfo().GetFlags());

        const auto self = weak_from_this();
        const auto future =
            Session->RenameNode(CreateCallContext(), std::move(request))
                .Apply(
                    [self,
                     info =
                         TRequestInfo{
                             Started,
                             EventMessageNumber,
                             logRequest,
                             logRequest.GetNodeInfo().GetNodeName()}](
                        const TFuture<NProto::TRenameNodeResponse>& future)
                    {
                        if (auto ptr = self.lock()) {
                            return ptr->HandleRenameNode(future, info);
                        }

                        return TCompletedRequest{
                            NProto::ACTION_RENAME_NODE,
                            info.Started,
                            MakeError(E_INVALID_STATE, "invalid ptr")};
                    });
        const auto& response = future.GetValueSync();
        return MakeFuture(
            TCompletedRequest{
                NProto::ACTION_RENAME_NODE,
                Started,
                response.Error});
    }

    TCompletedRequest HandleRenameNode(
        const TFuture<NProto::TRenameNodeResponse>& future,
        const TRequestInfo& info)
    {
        try {
            const auto& response = future.GetValue();
            CompareResponse(info, response.GetError().GetCode());
            CheckResponse(response);
            return {
                NProto::ACTION_RENAME_NODE,
                info.Started,
                response.GetError()};
        } catch (const TServiceError& e) {
            CompareResponse(info, e.GetCode());
            return {
                NProto::ACTION_RENAME_NODE,
                info.Started,
                MakeError(e.GetCode(), TString{e.GetMessage()})};
        }
    }

    TFuture<TCompletedRequest> DoUnlinkNode(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        // UnlinkNode   0.5s  S_OK    {parent_node_id=3, node_name=tfrgYZ1}

        if (Spec.GetSkipWrite()) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_REMOVE_NODE,
                    Started,
                    MakeError(E_PRECONDITION_FAILED, "write disabled")});
        }

        const auto name = logRequest.GetNodeInfo().GetNodeName();
        auto request = CreateRequest<NProto::TUnlinkNodeRequest>();
        request->SetName(name);
        const auto node =
            NodeIdMapped(logRequest.GetNodeInfo().GetParentNodeId());
        if (node == InvalidNodeId) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_REMOVE_NODE,
                    Started,
                    MakeError(E_NOT_FOUND, "parent node not found")});
        }
        request->SetNodeId(node);
        const auto self = weak_from_this();
        return Session->UnlinkNode(CreateCallContext(), std::move(request))
            .Apply(
                [self,
                 info =
                     TRequestInfo{
                         Started,
                         EventMessageNumber,
                         logRequest,
                         name}](
                    const TFuture<NProto::TUnlinkNodeResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleUnlinkNode(future, info);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_REMOVE_NODE,
                        info.Started,
                        MakeError(E_INVALID_STATE, "cancelled")};
                });
    }

    TCompletedRequest HandleUnlinkNode(
        const TFuture<NProto::TUnlinkNodeResponse>& future,
        const TRequestInfo& info)
    {
        try {
            const auto& response = future.GetValue();
            CompareResponse(info, response.GetError().GetCode());
            CheckResponse(response);

            if (info.LogRequest.GetNodeInfo().GetNodeId()) {
                with_lock (StateLock) {
                    NodesLogToLocal.erase(
                        info.LogRequest.GetNodeInfo().GetNodeId());
                }
            }

            return {
                NProto::ACTION_REMOVE_NODE,
                info.Started,
                response.GetError()};
        } catch (const TServiceError& e) {
            CompareResponse(info, e.GetCode());
            return {
                NProto::ACTION_REMOVE_NODE,
                info.Started,
                MakeError(e.GetCode(), TString{e.GetMessage()})};
        }
    }

    TFuture<TCompletedRequest> DoDestroyHandle(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        //  DestroyHandle  0.05s S_OK  {node_id=10, handle=6142388172112}

        const auto handle =
            HandleIdMapped(logRequest.GetNodeInfo().GetHandle());
        if (handle == InvalidHandle) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_DESTROY_HANDLE,
                    Started,
                    MakeError(E_NOT_FOUND, "handle not found")});
        }

        with_lock (StateLock) {
            HandlesLogToActual.erase(handle);
        }

        auto request = CreateRequest<NProto::TDestroyHandleRequest>();
        request->SetHandle(handle);

        const auto self = weak_from_this();
        return Session->DestroyHandle(CreateCallContext(), std::move(request))
            .Apply(
                [self,
                 info =
                     TRequestInfo{
                         Started,
                         EventMessageNumber,
                         logRequest,
                         logRequest.GetNodeInfo().GetNodeName()}](
                    const TFuture<NProto::TDestroyHandleResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleDestroyHandle(future, info);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_DESTROY_HANDLE,
                        info.Started,
                        MakeError(E_INVALID_STATE, "cancelled")};
                });
    }

    TCompletedRequest HandleDestroyHandle(

        const TFuture<NProto::TDestroyHandleResponse>& future,
        const TRequestInfo& info)
    {
        try {
            const auto& response = future.GetValue();
            CompareResponse(info, response.GetError().GetCode());
            CheckResponse(response);

            // testme:
            if (info.LogRequest.GetNodeInfo().GetNodeId()) {
                with_lock (StateLock) {
                    HandlesLogToActual.erase(
                        info.LogRequest.GetNodeInfo().GetNodeId());
                }
            }

            return {NProto::ACTION_DESTROY_HANDLE, info.Started, {}};
        } catch (const TServiceError& e) {
            CompareResponse(info, e.GetCode());
            return {
                NProto::ACTION_DESTROY_HANDLE,
                info.Started,
                MakeError(e.GetCode(), TString{e.GetMessage()})};
        }
    }

    TFuture<TCompletedRequest> DoGetNodeAttr(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        if (Spec.GetSkipRead()) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_GET_NODE_ATTR,
                    Started,
                    MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        // TODO(proller): by parent + name
        // {"TimestampMcs":1726615533406265,"DurationMcs":192,"RequestType":33,"ErrorCode":2147942402,"NodeInfo":{"ParentNodeId":17033,"NodeName":"CPackSourceConfig.cmake","Flags":0,"Mode":0,"NodeId":0,"Handle":0,"Size":0}}
        // {"TimestampMcs":240399000,"DurationMcs":163,"RequestType":33,"NodeInfo":{"ParentNodeId":3,"NodeName":"branches","Flags":0,"Mode":0,"NodeId":0,"Handle":0,"Size":0}}
        // GetNodeAttr     0.006847s       S_OK    {parent_node_id=1,
        // node_name=test, flags=0, mode=509, node_id=2, handle=0, size=0}

        const auto node =
            NodeIdMapped(logRequest.GetNodeInfo().GetParentNodeId());
        if (node == InvalidNodeId) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_GET_NODE_ATTR,
                    Started,
                    MakeError(
                        E_NOT_FOUND,
                        Sprintf(
                            "Node %lu missing in mapping ",
                            logRequest.GetNodeInfo().GetParentNodeId()))});
        }

        auto request = CreateRequest<NProto::TGetNodeAttrRequest>();
        request->SetNodeId(node);
        const auto name = logRequest.GetNodeInfo().GetNodeName();
        request->SetName(name);
        request->SetFlags(logRequest.GetNodeInfo().GetFlags());
        const auto self = weak_from_this();
        return Session->GetNodeAttr(CreateCallContext(), std::move(request))
            .Apply(
                [self,
                 info =
                     TRequestInfo{
                         Started,
                         EventMessageNumber,
                         logRequest,
                         name}](
                    const TFuture<NProto::TGetNodeAttrResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleGetNodeAttr(future, info);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_GET_NODE_ATTR,
                        info.Started,
                        MakeError(E_INVALID_STATE, "cancelled")};
                });
    }

    TCompletedRequest HandleGetNodeAttr(
        const TFuture<NProto::TGetNodeAttrResponse>& future,
        const TRequestInfo& info

    )
    {
        try {
            const auto& response = future.GetValue();
            CompareResponse(info, response.GetError().GetCode());
            CheckResponse(response);

            return {NProto::ACTION_GET_NODE_ATTR, info.Started, {}};
        } catch (const TServiceError& e) {
            CompareResponse(info, e.GetCode());

            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            PrintError(info, error);

            return {NProto::ACTION_GET_NODE_ATTR, info.Started, error};
        }
    }

    TFuture<TCompletedRequest> DoAcquireLock(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        // TODO(proller):
        return MakeFuture(
            TCompletedRequest{
                NProto::ACTION_ACQUIRE_LOCK,
                Started,
                MakeError(E_NOT_IMPLEMENTED, "not implemented")});

        TGuard<TMutex> guard(StateLock);
        if (Handles.empty()) {
            // return DoCreateHandle();
        }

        auto it = Handles.begin();
        while (it != Handles.end() &&
               (Locks.contains(it->first) || StagedLocks.contains(it->first)))
        {
            ++it;
        }

        if (it == Handles.end()) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_ACQUIRE_LOCK,
                    Started,
                    MakeError(E_CANCELLED, "cancelled")});
        }

        auto handle = it->first;
        Y_ABORT_UNLESS(StagedLocks.insert(handle).second);

        auto request = CreateRequest<NProto::TAcquireLockRequest>();
        request->SetHandle(handle);
        request->SetOwner(OwnerId);
        request->SetLength(LockLength);

        auto self = weak_from_this();
        return Session->AcquireLock(CreateCallContext(), std::move(request))
            .Apply(
                [self,
                 info =
                     TRequestInfo{
                         Started,
                         EventMessageNumber,
                         logRequest,
                         ToString(handle)},
                 handle](const TFuture<NProto::TAcquireLockResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleAcquireLock(handle, future, info);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_ACQUIRE_LOCK,
                        info.Started,
                        MakeError(E_INVALID_STATE, "cancelled")};
                });
    }

    TCompletedRequest HandleAcquireLock(
        ui64 handle,
        const TFuture<NProto::TAcquireLockResponse>& future,
        const TRequestInfo& info)
    {
        TGuard<TMutex> guard(StateLock);

        auto it = StagedLocks.find(handle);
        if (it == StagedLocks.end()) {
            // nothing todo, file was removed
            Y_ABORT_UNLESS(!Locks.contains(handle));
            return {NProto::ACTION_ACQUIRE_LOCK, info.Started, {}};
        }

        StagedLocks.erase(it);

        try {
            const auto& response = future.GetValue();
            CompareResponse(info, response.GetError().GetCode());
            CheckResponse(response);

            Locks.insert(handle);
            return {NProto::ACTION_ACQUIRE_LOCK, info.Started, {}};
        } catch (const TServiceError& e) {
            CompareResponse(info, e.GetCode());
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "acquire lock on %lu has failed: %s",
                handle,
                FormatError(error).c_str());

            return {NProto::ACTION_ACQUIRE_LOCK, info.Started, error};
        }
    }

    TFuture<TCompletedRequest> DoReleaseLock(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        return MakeFuture(
            TCompletedRequest{
                NProto::ACTION_RELEASE_LOCK,
                Started,
                MakeError(E_NOT_IMPLEMENTED, "not implemented")});

        TGuard<TMutex> guard(StateLock);
        if (Locks.empty()) {
            return DoAcquireLock(logRequest);
        }

        const auto it = Locks.begin();
        const auto handle = *it;

        Y_ABORT_UNLESS(StagedLocks.insert(handle).second);
        Locks.erase(it);

        auto request = CreateRequest<NProto::TReleaseLockRequest>();
        request->SetHandle(handle);
        request->SetOwner(OwnerId);
        request->SetLength(LockLength);

        auto self = weak_from_this();
        return Session->ReleaseLock(CreateCallContext(), std::move(request))
            .Apply(
                [self,
                 info =
                     TRequestInfo{
                         Started,
                         EventMessageNumber,
                         logRequest,
                         ToString(handle)},
                 handle](const TFuture<NProto::TReleaseLockResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleReleaseLock(handle, future, info);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_RELEASE_LOCK,
                        info.Started,
                        MakeError(E_INVALID_STATE, "cancelled")};
                });
    }

    TCompletedRequest HandleReleaseLock(
        ui64 handle,
        const TFuture<NProto::TReleaseLockResponse>& future,
        const TRequestInfo& info)
    {
        TGuard<TMutex> guard(StateLock);

        auto it = StagedLocks.find(handle);
        if (it != StagedLocks.end()) {
            StagedLocks.erase(it);
        }

        try {
            CheckResponse(future.GetValue());
            return {NProto::ACTION_RELEASE_LOCK, info.Started, {}};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "release lock on %lu has failed: %s",
                handle,
                FormatError(error).c_str());

            return {NProto::ACTION_RELEASE_LOCK, info.Started, error};
        }
    }

    TFuture<TCompletedRequest> DoListNodes(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        // name=ListNodes data="TimestampMcs: 1729271080089294 DurationMcs:
        // 131 RequestType: 30 ErrorCode: 0 NodeInfo { NodeId: 33 Size: 0 }"
        if (Spec.GetSkipRead()) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_LIST_NODES,
                    Started,
                    MakeError(E_PRECONDITION_FAILED, "read disabled")});
        }

        const auto nodeId = NodeIdMapped(logRequest.GetNodeInfo().GetNodeId());
        if (nodeId == InvalidNodeId) {
            return MakeFuture(
                TCompletedRequest{
                    NProto::ACTION_LIST_NODES,
                    Started,
                    MakeError(
                        E_NOT_FOUND,
                        "Log node %d not found in mapping",
                        logRequest.GetNodeInfo().GetParentNodeId())});
        }
        auto request = CreateRequest<NProto::TListNodesRequest>();
        request->SetNodeId(nodeId);
        const auto self = weak_from_this();
        return Session->ListNodes(CreateCallContext(), std::move(request))
            .Apply(
                [self,
                 info =
                     TRequestInfo{
                         Started,
                         EventMessageNumber,
                         logRequest,
                         ToString(nodeId)}](
                    const TFuture<NProto::TListNodesResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleListNodes(future, info);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_LIST_NODES,
                        info.Started,
                        MakeError(E_INVALID_STATE, "cancelled")};
                });
    }

    TCompletedRequest HandleListNodes(
        const TFuture<NProto::TListNodesResponse>& future,
        const TRequestInfo& info)
    {
        try {
            const auto& response = future.GetValue();
            CompareResponse(info, response.GetError().GetCode());
            CheckResponse(response);
            if (response.NamesSize() != info.LogRequest.GetNodeInfo().GetSize())
            {
                STORAGE_DEBUG(Sprintf(
                    "List nodes %lu size mismatch log=%lu received=%zu",
                    info.LogRequest.GetNodeInfo().GetNodeId(),
                    info.LogRequest.GetNodeInfo().GetSize(),
                    response.NamesSize()));
            }
            return {
                NProto::ACTION_LIST_NODES,
                info.Started,
                response.GetError()};
        } catch (const TServiceError& e) {
            CompareResponse(info, e.GetCode());
            const auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_DEBUG(
                "Access node %lu has failed: %s",
                info.LogRequest.GetNodeInfo().GetNodeId(),
                FormatError(error).c_str());

            return {NProto::ACTION_LIST_NODES, info.Started, error};
        }
    }

    TFuture<TCompletedRequest> DoFlush(
        [[maybe_unused]] const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& logRequest) override
    {
        constexpr auto CurrentAction = NProto::ACTION_FLUSH;

        if (Spec.GetSkipWrite()) {
            return MakeFuture(
                TCompletedRequest{
                    CurrentAction,
                    Started,
                    MakeError(
                        E_PRECONDITION_FAILED,
                        "write disabled by SkipWrite")});
        }

        const auto handle =
            HandleIdMapped(logRequest.GetNodeInfo().GetHandle());
        if (handle == InvalidHandle) {
            return MakeFuture(
                TCompletedRequest{
                    CurrentAction,
                    Started,
                    MakeError(E_NOT_FOUND, "handle not found")});
        }

        auto request = CreateRequest<NProto::TFsyncRequest>();
        request->SetHandle(handle);
        const auto self = weak_from_this();
        return Session->Fsync(CreateCallContext(), std::move(request))
            .Apply(
                [self,
                 info =
                     TRequestInfo{
                         Started,
                         EventMessageNumber,
                         logRequest,
                         ToString(handle)}](
                    const TFuture<NProto::TFsyncResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleFlush(future, info);
                    }

                    return TCompletedRequest{
                        CurrentAction,
                        info.Started,
                        MakeError(E_INVALID_STATE, "cancelled")};
                });
    }

    TCompletedRequest HandleFlush(
        const TFuture<NProto::TFsyncResponse>& future,
        const TRequestInfo& info)
    {
        try {
            const auto& response = future.GetValue();
            CompareResponse(info, response.GetError().GetCode());
            CheckResponse(response);
            return {NProto::ACTION_FLUSH, info.Started, response.GetError()};
        } catch (const TServiceError& e) {
            CompareResponse(info, e.GetCode());
            const auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "Flush %lu has failed: %s",
                info.LogRequest.GetNodeInfo().GetHandle(),
                FormatError(error).c_str());

            return {NProto::ACTION_FLUSH, info.Started, error};
        }
    }

    template <typename T>
    std::shared_ptr<T> CreateRequest()
    {
        auto request = std::make_shared<T>();
        request->SetFileSystemId(TargetFilesystemId);
        request->MutableHeaders()->CopyFrom(Headers);

        return request;
    }

    template <typename T>
    void CheckResponse(const T& response)
    {
        if (HasError(response)) {
            throw TServiceError(response.GetError());
        }
    }

    void CompareResponse(const TRequestInfo& info, const ui32 code)
    {
        if (info.LogRequest.GetErrorCode() == code) {
            return;
        }

        STORAGE_TRACE(
            "Msg=%zd: result code mismatch: type=%s received=%d log=%d : "
            "%s",
            info.EventMessageNumber,
            RequestName(info.LogRequest.GetRequestType()).c_str(),
            code,
            info.LogRequest.GetErrorCode(),
            info.Description.c_str());
    }

    void PrintError(const TRequestInfo& info, const NProto::TError& error)
    {
        STORAGE_DEBUG(
            "Msg=%zd: request fail: %s (%d) fail: %d %s [%s]",
            info.EventMessageNumber,
            RequestName(info.LogRequest.GetRequestType()).c_str(),
            info.LogRequest.GetRequestType(),
            error.GetCode(),
            error.GetMessage().c_str(),
            info.Description.c_str());
    }

    TIntrusivePtr<TCallContext> CreateCallContext()
    {
        return MakeIntrusive<TCallContext>(
            LastRequestId.fetch_add(1, std::memory_order_relaxed));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRequestGeneratorPtr CreateReplayRequestGeneratorGRPC(
    NProto::TReplaySpec spec,
    ILoggingServicePtr logging,
    ISessionPtr session,
    TString filesystemId,
    NProto::THeaders headers)
{
    return std::make_shared<TReplayRequestGeneratorGRPC>(
        std::move(spec),
        std::move(logging),
        std::move(session),
        std::move(filesystemId),
        std::move(headers));
}

}   // namespace NCloud::NFileStore::NLoadTest
