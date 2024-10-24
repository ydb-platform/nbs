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
    {}

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
    TFuture<TCompletedRequest> DoAccessNode(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        // nfs     AccessNode      0.002297s       S_OK    {mask=4, node_id=36}

        if (Spec.GetSkipRead()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_ACCESS_NODE,
                Started,
                MakeError(E_PRECONDITION_FAILED, "read disabled")});
        }

        const auto request = CreateRequest<NProto::TAccessNodeRequest>();

        const auto nodeId = NodeIdMapped(logRequest.GetNodeInfo().GetNodeId());
        if (nodeId == InvalidNodeId) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_ACCESS_NODE,
                Started,
                MakeError(E_NOT_FOUND, "node not found")});
        }
        request->SetNodeId(nodeId);
        // TODO(proller): where is mask in logRequest?
        // request->SetMask(logRequest.GetNodeInfo().);
        auto self = weak_from_this();
        const auto future =
            Session->AccessNode(CreateCallContext(), std::move(request))
                .Apply(
                    [=, started = Started](
                        const TFuture<NProto::TAccessNodeResponse>& future)
                    {
                        if (auto ptr = self.lock()) {
                            return ptr->HandleAccessNode(
                                future,
                                started,
                                logRequest);
                        }

                        return TCompletedRequest{
                            NProto::ACTION_ACCESS_NODE,
                            started,
                            MakeError(E_INVALID_STATE, "cancelled")};
                    });
        const auto& response = future.GetValueSync();
        return MakeFuture(TCompletedRequest{
            NProto::ACTION_ACCESS_NODE,
            Started,
            response.Error});
    }

    TCompletedRequest HandleAccessNode(
        const TFuture<NProto::TAccessNodeResponse>& future,
        TInstant started,
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);
            return {NProto::ACTION_ACCESS_NODE, started, response.GetError()};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "Access node %lu has failed: %s",
                logRequest.GetNodeInfo().GetNodeId(),
                FormatError(error).c_str());

            return {NProto::ACTION_ACCESS_NODE, started, error};
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
            return MakeFuture(TCompletedRequest{
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
                    [=, started = Started, name = std::move(name)](
                        const TFuture<NProto::TCreateHandleResponse>& future)
                    {
                        if (auto ptr = self.lock()) {
                            return ptr->HandleCreateHandle(
                                future,
                                name,
                                started,
                                logRequest);
                        }

                        return MakeFuture(TCompletedRequest{
                            NProto::ACTION_CREATE_HANDLE,
                            started,
                            MakeError(E_INVALID_STATE, "cancelled")});
                    });
        const auto& response = future.GetValueSync();
        return MakeFuture(TCompletedRequest{
            NProto::ACTION_CREATE_HANDLE,
            Started,
            response.Error});
    }

    TFuture<TCompletedRequest> HandleCreateHandle(
        const TFuture<NProto::TCreateHandleResponse>& future,
        const TString& name,
        TInstant started,
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);

            auto handle = response.GetHandle();
            with_lock (StateLock) {
                HandlesLogToActual[logRequest.GetNodeInfo().GetHandle()] =
                    handle;
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
                    return MakeFuture(TCompletedRequest{});
                });
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "create handle for %s has failed: %s",
                name.Quote().c_str(),
                FormatError(error).c_str());

            return NThreading::MakeFuture(TCompletedRequest{
                NProto::ACTION_CREATE_HANDLE,
                started,
                error});
        }
    }

    TFuture<TCompletedRequest> DoReadData(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        if (Spec.GetSkipRead()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_READ,
                Started,
                MakeError(E_PRECONDITION_FAILED, "read disabled")});
        }

        auto request = CreateRequest<NProto::TReadDataRequest>();
        const auto handle =
            HandleIdMapped(logRequest.GetNodeInfo().GetHandle());
        if (handle == InvalidHandle) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_READ,
                Started,
                MakeError(E_NOT_FOUND, "handle not found")});
        }
        request->SetHandle(handle);
        request->SetOffset(logRequest.GetRanges().cbegin()->GetOffset());
        request->SetLength(logRequest.GetRanges().cbegin()->GetBytes());

        auto self = weak_from_this();
        return Session->ReadData(CreateCallContext(), std::move(request))
            .Apply(
                [=, started = Started](
                    const TFuture<NProto::TReadDataResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleRead(future, started);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_READ,
                        started,
                        future.GetValue().GetError()};
                });
    }

    TCompletedRequest HandleRead(
        const TFuture<NProto::TReadDataResponse>& future,
        TInstant started)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);
            return {NProto::ACTION_READ, started, response.GetError()};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "read for %s has failed: ",
                FormatError(error).c_str());

            return {NProto::ACTION_READ, started, error};
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

    TFuture<TCompletedRequest> DoWrite(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        //{"TimestampMcs":1465489895000,"DurationMcs":2790,"RequestType":44,"Ranges":[{"NodeId":2,"Handle":20680158862113389,"Offset":13,"Bytes":12}]}

        if (Spec.GetSkipWrite()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_WRITE,
                Started,
                MakeError(E_PRECONDITION_FAILED, "write disabled")});
        }

        auto request = CreateRequest<NProto::TWriteDataRequest>();
        const auto handleLog = logRequest.GetRanges(0).GetHandle();
        const auto handleLocal = HandleIdMapped(handleLog);

        if (handleLocal == InvalidHandle) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_WRITE,
                Started,
                MakeError(E_NOT_FOUND, "handle not found")});
        }
        request->SetHandle(handleLocal);

        const auto offset = logRequest.GetRanges(0).GetOffset();
        request->SetOffset(offset);
        auto bytes = logRequest.GetRanges(0).GetBytes();

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
        return Session->WriteData(CreateCallContext(), request)
            .Apply(
                [=, started = Started](
                    const TFuture<NProto::TWriteDataResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleWrite(future, started);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_WRITE,
                        started,
                        MakeError(E_INVALID_STATE, "cancelled")};
                });
    }

    TCompletedRequest HandleWrite(
        const TFuture<NProto::TWriteDataResponse>& future,
        TInstant started)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);
            return {NProto::ACTION_WRITE, started, response.GetError()};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            return {NProto::ACTION_WRITE, started, error};
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
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_CREATE_NODE,
                Started,
                MakeError(E_PRECONDITION_FAILED, "write disabled")});
        }

        auto request = CreateRequest<NProto::TCreateNodeRequest>();

        const auto parentNode =
            NodeIdMapped(logRequest.GetNodeInfo().GetNewParentNodeId());
        if (parentNode == InvalidNodeId) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_CREATE_NODE,
                Started,
                MakeError(E_NOT_FOUND, "node not found")});
        }
        request->SetNodeId(parentNode);
        const auto name = logRequest.GetNodeInfo().GetNewNodeName();
        request->SetName(logRequest.GetNodeInfo().GetNewNodeName());

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
            case NProto::E_LINK_NODE:
                // TODO(proller):
                //  request->MutableLink()->SetTargetNode(r.GetNodeInfo().Get...);
                //  request->MutableLink()->SetFollowerNodeName(r.GetNodeInfo().Get...);
                return MakeFuture(TCompletedRequest{});
                break;
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
                    [=, started = Started](
                        const TFuture<NProto::TCreateNodeResponse>& future)
                    {
                        if (auto ptr = self.lock()) {
                            return ptr->HandleCreateNode(
                                future,
                                started,
                                logRequest);
                        }

                        return TCompletedRequest{
                            NProto::ACTION_CREATE_NODE,
                            started,
                            MakeError(E_INVALID_STATE, "cancelled")};
                    });
        const auto& response = future.GetValueSync();
        return MakeFuture(TCompletedRequest{
            NProto::ACTION_CREATE_NODE,
            Started,
            response.Error});
    }

    TCompletedRequest HandleCreateNode(
        const TFuture<NProto::TCreateNodeResponse>& future,
        TInstant started,
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);
            if (response.GetNode().GetId()) {
                with_lock (StateLock) {
                    NodesLogToLocal[logRequest.GetNodeInfo().GetNodeId()] =
                        response.GetNode().GetId();
                }
            }

            return {NProto::ACTION_CREATE_NODE, started, response.GetError()};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            return {NProto::ACTION_CREATE_NODE, started, error};
        }
    }

    TFuture<TCompletedRequest> DoRenameNode(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        // {"TimestampMcs":895166000,"DurationMcs":2949,"RequestType":28,"NodeInfo":{"ParentNodeId":3,"NodeName":"HEAD.lock","NewParentNodeId":3,"NewNodeName":"HEAD"}}
        if (Spec.GetSkipRead()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_RENAME_NODE,
                Started,
                MakeError(E_PRECONDITION_FAILED, "read disabled")});
        }

        auto request = CreateRequest<NProto::TRenameNodeRequest>();
        const auto node =
            NodeIdMapped(logRequest.GetNodeInfo().GetParentNodeId());
        if (node == InvalidNodeId) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_RENAME_NODE,
                Started,
                MakeError(
                    E_NOT_FOUND,
                    "Log node %d not found in mapping",
                    logRequest.GetNodeInfo().GetParentNodeId())});
        }
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
                    [=, started = Started](
                        const TFuture<NProto::TRenameNodeResponse>& future)
                    {
                        if (auto ptr = self.lock()) {
                            return ptr->HandleRenameNode(future, started);
                        }

                        return TCompletedRequest{
                            NProto::ACTION_RENAME_NODE,
                            started,
                            MakeError(E_INVALID_STATE, "invalid ptr")};
                    });
        const auto& response = future.GetValueSync();
        return MakeFuture(TCompletedRequest{
            NProto::ACTION_RENAME_NODE,
            Started,
            response.Error});
    }

    TCompletedRequest HandleRenameNode(
        const TFuture<NProto::TRenameNodeResponse>& future,
        TInstant started)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);
            return {NProto::ACTION_RENAME_NODE, started, response.GetError()};
        } catch (const TServiceError& e) {
            const auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            return {NProto::ACTION_RENAME_NODE, started, error};
        }
    }

    TFuture<TCompletedRequest> DoUnlinkNode(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        // UnlinkNode   0.5s  S_OK    {parent_node_id=3, node_name=tfrgYZ1}

        if (Spec.GetSkipWrite()) {
            return MakeFuture(TCompletedRequest{
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
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_REMOVE_NODE,
                Started,
                MakeError(E_NOT_FOUND, "parent node not found")});
        }
        request->SetNodeId(node);
        const auto self = weak_from_this();
        return Session->UnlinkNode(CreateCallContext(), std::move(request))
            .Apply(
                [=, started = Started](
                    const TFuture<NProto::TUnlinkNodeResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleUnlinkNode(future, started);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_REMOVE_NODE,
                        started,
                        MakeError(E_INVALID_STATE, "cancelled")};
                });
    }

    TCompletedRequest HandleUnlinkNode(
        const TFuture<NProto::TUnlinkNodeResponse>& future,
        TInstant started)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);
            return {NProto::ACTION_REMOVE_NODE, started, response.GetError()};
        } catch (const TServiceError& e) {
            const auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            return {NProto::ACTION_REMOVE_NODE, started, error};
        }
    }
    TFuture<TCompletedRequest> DoDestroyHandle(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        //  DestroyHandle  0.05s S_OK  {node_id=10, handle=6142388172112}

        const auto name = logRequest.GetNodeInfo().GetNodeName();

        auto request = CreateRequest<NProto::TDestroyHandleRequest>();

        const auto handle =
            HandleIdMapped(logRequest.GetNodeInfo().GetHandle());
        if (handle == InvalidHandle) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_DESTROY_HANDLE,
                Started,
                MakeError(E_NOT_FOUND, "handle not found")});
        }

        with_lock (StateLock) {
            HandlesLogToActual.erase(handle);
        }

        request->SetHandle(handle);

        const auto self = weak_from_this();
        return Session->DestroyHandle(CreateCallContext(), std::move(request))
            .Apply(
                [=, started = Started, name = std::move(name)](
                    const TFuture<NProto::TDestroyHandleResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleDestroyHandle(future, started);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_DESTROY_HANDLE,
                        started,
                        MakeError(E_INVALID_STATE, "cancelled")};
                });
    }

    TCompletedRequest HandleDestroyHandle(
        const TFuture<NProto::TDestroyHandleResponse>& future,
        TInstant started)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);
            return {NProto::ACTION_DESTROY_HANDLE, started, {}};
        } catch (const TServiceError& e) {
            return {
                NProto::ACTION_DESTROY_HANDLE,
                started,
                MakeError(e.GetCode(), TString{e.GetMessage()})};
        }
    }

    TFuture<TCompletedRequest> DoGetNodeAttr(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        if (Spec.GetSkipRead()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_GET_NODE_ATTR,
                Started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        // TODO(proller): by parent + name
        // {"TimestampMcs":1726615533406265,"DurationMcs":192,"RequestType":33,"ErrorCode":2147942402,"NodeInfo":{"ParentNodeId":17033,"NodeName":"CPackSourceConfig.cmake","Flags":0,"Mode":0,"NodeId":0,"Handle":0,"Size":0}}
        // {"TimestampMcs":240399000,"DurationMcs":163,"RequestType":33,"NodeInfo":{"ParentNodeId":3,"NodeName":"branches","Flags":0,"Mode":0,"NodeId":0,"Handle":0,"Size":0}}
        // GetNodeAttr     0.006847s       S_OK    {parent_node_id=1,
        // node_name=test, flags=0, mode=509, node_id=2, handle=0, size=0}

        auto request = CreateRequest<NProto::TGetNodeAttrRequest>();
        const auto node =
            NodeIdMapped(logRequest.GetNodeInfo().GetParentNodeId());
        if (node == InvalidNodeId) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_GET_NODE_ATTR,
                Started,
                MakeError(
                    E_NOT_FOUND,
                    Sprintf(
                        "Node %lu missing in mapping ",
                        logRequest.GetNodeInfo().GetParentNodeId()))});
        }
        request->SetNodeId(node);
        const auto name = logRequest.GetNodeInfo().GetNodeName();
        request->SetName(logRequest.GetNodeInfo().GetNodeName());
        request->SetFlags(logRequest.GetNodeInfo().GetFlags());
        const auto self = weak_from_this();
        STORAGE_DEBUG(
            "GetNodeAttr client started name=%s node=%lu  <- %lu",
            name.c_str(),
            node,
            logRequest.GetNodeInfo().GetParentNodeId());
        return Session->GetNodeAttr(CreateCallContext(), std::move(request))
            .Apply(
                [=, name = std::move(name), started = Started](
                    const TFuture<NProto::TGetNodeAttrResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleGetNodeAttr(name, future, started);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_GET_NODE_ATTR,
                        started,
                        MakeError(E_INVALID_STATE, "cancelled")};
                });
    }

    TCompletedRequest HandleGetNodeAttr(
        const TString& name,
        const TFuture<NProto::TGetNodeAttrResponse>& future,
        TInstant started)
    {
        try {
            const auto& response = future.GetValue();
            STORAGE_DEBUG("GetNodeAttr client completed");
            CheckResponse(response);
            return {NProto::ACTION_GET_NODE_ATTR, started, {}};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "get node attr %s has failed: %s",
                name.c_str(),
                FormatError(error).c_str());

            return {NProto::ACTION_GET_NODE_ATTR, started, error};
        }
    }

    TFuture<TCompletedRequest> DoAcquireLock(
        const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& /*logRequest*/) override
    {
        // TODO(proller):
        return MakeFuture(TCompletedRequest{
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
            return MakeFuture(TCompletedRequest{
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
                [=, started = Started](
                    const TFuture<NProto::TAcquireLockResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleAcquireLock(handle, future, started);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_ACQUIRE_LOCK,
                        started,
                        MakeError(E_INVALID_STATE, "cancelled")};
                });
    }

    TCompletedRequest HandleAcquireLock(
        ui64 handle,
        const TFuture<NProto::TAcquireLockResponse>& future,
        TInstant started)
    {
        TGuard<TMutex> guard(StateLock);

        auto it = StagedLocks.find(handle);
        if (it == StagedLocks.end()) {
            // nothing todo, file was removed
            Y_ABORT_UNLESS(!Locks.contains(handle));
            return {NProto::ACTION_ACQUIRE_LOCK, started, {}};
        }

        StagedLocks.erase(it);

        try {
            const auto& response = future.GetValue();
            CheckResponse(response);

            Locks.insert(handle);
            return {NProto::ACTION_ACQUIRE_LOCK, started, {}};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "acquire lock on %lu has failed: %s",
                handle,
                FormatError(error).c_str());

            return {NProto::ACTION_ACQUIRE_LOCK, started, error};
        }
    }

    TFuture<TCompletedRequest> DoReleaseLock(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        return MakeFuture(TCompletedRequest{
            NProto::ACTION_RELEASE_LOCK,
            Started,
            MakeError(E_NOT_IMPLEMENTED, "not implemented")});

        TGuard<TMutex> guard(StateLock);
        if (Locks.empty()) {
            return DoAcquireLock(logRequest);
        }

        auto it = Locks.begin();
        auto handle = *it;

        Y_ABORT_UNLESS(StagedLocks.insert(handle).second);
        Locks.erase(it);

        auto request = CreateRequest<NProto::TReleaseLockRequest>();
        request->SetHandle(handle);
        request->SetOwner(OwnerId);
        request->SetLength(LockLength);

        auto self = weak_from_this();
        return Session->ReleaseLock(CreateCallContext(), std::move(request))
            .Apply(
                [=, started = Started](
                    const TFuture<NProto::TReleaseLockResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleReleaseLock(handle, future, started);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_RELEASE_LOCK,
                        started,
                        MakeError(E_INVALID_STATE, "cancelled")};
                });
    }

    TCompletedRequest HandleReleaseLock(
        ui64 handle,
        const TFuture<NProto::TReleaseLockResponse>& future,
        TInstant started)
    {
        TGuard<TMutex> guard(StateLock);

        auto it = StagedLocks.find(handle);
        if (it != StagedLocks.end()) {
            StagedLocks.erase(it);
        }

        try {
            CheckResponse(future.GetValue());
            return {NProto::ACTION_RELEASE_LOCK, started, {}};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "release lock on %lu has failed: %s",
                handle,
                FormatError(error).c_str());

            return {NProto::ACTION_RELEASE_LOCK, started, error};
        }
    }

    TFuture<TCompletedRequest> DoListNodes(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        // name=ListNodes data="TimestampMcs: 1729271080089294 DurationMcs: 131
        // RequestType: 30 ErrorCode: 0 NodeInfo { NodeId: 33 Size: 0 }"
        if (Spec.GetSkipRead()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_LIST_NODES,
                Started,
                MakeError(E_PRECONDITION_FAILED, "read disabled")});
        }

        auto request = CreateRequest<NProto::TListNodesRequest>();

        const auto nodeId = NodeIdMapped(logRequest.GetNodeInfo().GetNodeId());
        if (nodeId == InvalidNodeId) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_LIST_NODES,
                Started,
                MakeError(
                    E_NOT_FOUND,
                    "Log node %d not found in mapping",
                    logRequest.GetNodeInfo().GetParentNodeId())});
        }
        request->SetNodeId(nodeId);
        const auto self = weak_from_this();
        return Session->ListNodes(CreateCallContext(), std::move(request))
            .Apply(
                [=, started = Started](
                    const TFuture<NProto::TListNodesResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleListNodes(
                            future,
                            started,
                            logRequest);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_LIST_NODES,
                        started,
                        MakeError(E_INVALID_STATE, "cancelled")};
                });
    }

    TCompletedRequest HandleListNodes(
        const TFuture<NProto::TListNodesResponse>& future,
        TInstant started,
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);
            if (response.NamesSize() != logRequest.GetNodeInfo().GetSize()) {
                STORAGE_DEBUG(Sprintf(
                    "List nodes %lu size mismatch log=%lu received=%zu",
                    logRequest.GetNodeInfo().GetNodeId(),
                    logRequest.GetNodeInfo().GetSize(),
                    response.NamesSize()));
            }
            return {NProto::ACTION_LIST_NODES, started, response.GetError()};
        } catch (const TServiceError& e) {
            const auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "Access node %lu has failed: %s",
                logRequest.GetNodeInfo().GetNodeId(),
                FormatError(error).c_str());

            return {NProto::ACTION_LIST_NODES, started, error};
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
