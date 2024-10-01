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

    struct TNode
    {
        TString Name;
    };

    struct THandle
    {
        TString Path;
    };

    THashMap<ui64, THandle> Handles;
    THashSet<ui64> Locks;
    THashSet<ui64> StagedLocks;

    THashMap<TNodeLog, TNodeLocal> NodesLogToLocal{{RootNodeId, RootNodeId}};
    THashMap<TNodeLocal, TString> NodePath{{RootNodeId, "/"}};

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
            "node not found " << id << " map size=" << NodesLogToLocal.size());
        return 0;
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
            "handle not found " << id
                                << " map size=" << HandlesLogToActual.size());

        return 0;
    }

private:
    TFuture<TCompletedRequest> DoAccessNode(
        const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& /*logRequest*/) override
    {
        // nfs     AccessNode      0.002297s       S_OK    {mask=4, node_id=36}

        if (Spec.GetNoRead()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_ACCESS_NODE,
                Started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        return MakeFuture(TCompletedRequest{
            NProto::ACTION_ACCESS_NODE,
            Started,
            MakeError(E_NOT_IMPLEMENTED, "not implemented")});
    }

    TFuture<TCompletedRequest> DoCreateHandle(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        // json={"TimestampMcs":1726503808715698,"DurationMcs":2622,"RequestType":38,"ErrorCode":0,"NodeInfo":{"ParentNodeId":13882,"NodeName":"compile_commands.json.tmpdf020","Flags":38,"Mode":436,"NodeId":15553,"Handle":46923415058768564,"Size":0}}
        // {"TimestampMcs":1725895168384258,"DurationMcs":2561,"RequestType":38,"ErrorCode":0,"NodeInfo":{"ParentNodeId":12527,"NodeName":"index.lock","Flags":15,"Mode":436,"NodeId":12584,"Handle":65382484937735195,"Size":0}}
        // nfs     CreateHandle    0.004161s       S_OK    {parent_node_id=65,
        // node_name=ini, flags=14, mode=436, node_id=66,
        // handle=11024287581389312, size=0}

        auto request = CreateRequest<NProto::TCreateHandleRequest>();
        auto name = logRequest.GetNodeInfo().GetNodeName();

        const auto node =
            NodeIdMapped(logRequest.GetNodeInfo().GetParentNodeId());
        if (!node) {
            return MakeFuture(TCompletedRequest{});
        }
        request->SetNodeId(node);
        request->SetName(logRequest.GetNodeInfo().GetNodeName());
        request->SetFlags(logRequest.GetNodeInfo().GetFlags());
        request->SetMode(logRequest.GetNodeInfo().GetMode());

        STORAGE_DEBUG(
            "open " << " handle=" << logRequest.GetNodeInfo().GetHandle()
                    << " flags=" << logRequest.GetNodeInfo().GetFlags() << " "
                    << HandleFlagsToString(logRequest.GetNodeInfo().GetFlags())
                    << " mode=" << logRequest.GetNodeInfo().GetMode()
                    << " node=" << node << " <- "
                    << logRequest.GetNodeInfo().GetNodeId());

        auto self = weak_from_this();
        const auto future =
            Session->CreateHandle(CreateCallContext(), std::move(request))
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
                            MakeError(E_FAIL, "cancelled")});
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
                    return MakeFuture(TCompletedRequest{});
                    // return HandleResizeAfterCreateHandle(f, name, started);
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
        const auto started = TInstant::Now();
        if (Spec.GetNoRead()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_READ,
                started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        auto request = CreateRequest<NProto::TReadDataRequest>();
        const auto handle =
            HandleIdMapped(logRequest.GetNodeInfo().GetHandle());
        if (!handle) {
            return MakeFuture(TCompletedRequest{});
        }
        request->SetHandle(handle);
        request->SetOffset(logRequest.GetRanges().cbegin()->GetOffset());
        request->SetLength(logRequest.GetRanges().cbegin()->GetBytes());

        auto self = weak_from_this();
        return Session->ReadData(CreateCallContext(), std::move(request))
            .Apply(
                [=](const TFuture<NProto::TReadDataResponse>& future)
                {
                    return TCompletedRequest{
                        NProto::ACTION_READ,
                        started,
                        future.GetValue().GetError()};
                    /*
                                        if (auto ptr = self.lock()) {
                                            return ptr->HandleRead(
                                                future,
                                                handleInfo,
                                                started,
                                                byteOffset);
                                        }
                    */
                    return TCompletedRequest{
                        NProto::ACTION_READ,
                        started,
                        MakeError(E_FAIL, "cancelled")};
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
                // handleInfo.Name.Quote().c_str(),
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

        const auto started = TInstant::Now();
        if (Spec.GetNoWrite()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_WRITE,
                started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        auto request = CreateRequest<NProto::TWriteDataRequest>();
        const auto logHandle = logRequest.GetRanges(0).GetHandle();
        const auto handle = HandleIdMapped(logHandle);

        if (!handle) {
            return MakeFuture(TCompletedRequest{});
        }
        request->SetHandle(handle);

        const auto offset = logRequest.GetRanges(0).GetOffset();
        request->SetOffset(offset);
        auto bytes = logRequest.GetRanges(0).GetBytes();

        TString buffer;

        if (Spec.GetWriteRandom()) {
            buffer = NUnitTest::RandomString(bytes, logHandle);
        } else if (Spec.GetWriteEmpty()) {
            buffer = TString{bytes, ' '};
        } else {
            buffer = MakeBuffer(
                bytes,
                offset,
                TStringBuilder{} << "handle=" << logHandle << " node="
                                 << logRequest.GetNodeInfo().GetNodeId()
                                 << " bytes=" << bytes << " offset=" << offset);
        }

        *request->MutableBuffer() = std::move(buffer);

        auto self = weak_from_this();
        return Session->WriteData(CreateCallContext(), std::move(request))
            .Apply(
                [=](const TFuture<NProto::TWriteDataResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleWrite(future, started);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_WRITE,
                        started,
                        MakeError(E_FAIL, "cancelled")};
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
            /*
                        STORAGE_ERROR(
                            "write on %s has failed: %s",
                            handleInfo.Name.Quote().c_str(),
                            FormatError(error).c_str());
            */
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

        if (Spec.GetNoWrite()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_CREATE_NODE,
                Started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        auto request = CreateRequest<NProto::TCreateNodeRequest>();

        const auto parentNode =
            NodeIdMapped(logRequest.GetNodeInfo().GetNewParentNodeId());
        if (!parentNode) {
            return MakeFuture(TCompletedRequest{});
        }
        request->SetNodeId(parentNode);
        auto name = logRequest.GetNodeInfo().GetNewNodeName();
        request->SetName(logRequest.GetNodeInfo().GetNewNodeName());

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
        Cerr << "createnoderec" << *request << "\n";
        auto self = weak_from_this();
        const auto future =
            Session->CreateNode(CreateCallContext(), std::move(request))
                .Apply(
                    [=, started = Started](
                        const TFuture<NProto::TCreateNodeResponse>& future)
                    {
                        if (auto ptr = self.lock()) {
                            return ptr->HandleCreateNode(
                                future,
                                name,
                                started,
                                logRequest);
                        }

                        return TCompletedRequest{
                            NProto::ACTION_CREATE_NODE,
                            started,
                            MakeError(E_CANCELLED, "cancelled")};
                    });
        const auto& response = future.GetValueSync();
        return MakeFuture(TCompletedRequest{
            NProto::ACTION_CREATE_NODE,
            Started,
            response.Error});
    }

    TCompletedRequest HandleCreateNode(
        const TFuture<NProto::TCreateNodeResponse>& future,
        const TString& name,
        TInstant started,
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);
            // Nodes[name] = TNode{name, response.GetNode()};
            if (response.GetNode().GetId()) {
                with_lock (StateLock) {
                    NodesLogToLocal[logRequest.GetNodeInfo().GetNodeId()] =
                        response.GetNode().GetId();
                }
            }

            return {NProto::ACTION_CREATE_NODE, started, response.GetError()};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "create node %s has failed: %s",
                name.c_str(),
                FormatError(error).c_str());

            return {NProto::ACTION_CREATE_NODE, started, error};
        }
    }

    TFuture<TCompletedRequest> DoRenameNode(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        // {"TimestampMcs":895166000,"DurationMcs":2949,"RequestType":28,"NodeInfo":{"ParentNodeId":3,"NodeName":"HEAD.lock","NewParentNodeId":3,"NewNodeName":"HEAD"}}
        // nfs     RenameNode      0.002569s       S_OK {parent_node_id=12527,
        // node_name=HEAD.lock, new_parent_node_id=12527, new_node_name=HEAD}
        // request->SetNodeId(NodesLogToActual[r.GetNodeInfo().GetNodeId()]);
        if (Spec.GetNoRead()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_RENAME_NODE,
                Started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        auto request = CreateRequest<NProto::TRenameNodeRequest>();
        const auto node =
            NodeIdMapped(logRequest.GetNodeInfo().GetParentNodeId());
        if (!node) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_RENAME_NODE,
                Started,
                MakeError(
                    E_FAIL,
                    TStringBuilder{}
                        << "Log node "
                        << logRequest.GetNodeInfo().GetParentNodeId()
                        << "not found in mappiong")});
        }
        request->SetNodeId(node);
        request->SetName(logRequest.GetNodeInfo().GetNodeName());
        request->SetNewParentId(
            NodeIdMapped(logRequest.GetNodeInfo().GetNewParentNodeId()));
        request->SetNewName(logRequest.GetNodeInfo().GetNewNodeName());
        request->SetFlags(logRequest.GetNodeInfo().GetFlags());

        auto self = weak_from_this();
        const auto future =
            Session->RenameNode(CreateCallContext(), std::move(request))
                .Apply(
                    [=, started = Started](
                        const TFuture<NProto::TRenameNodeResponse>& future)
                    {
                        if (auto ptr = self.lock()) {
                            return ptr->HandleRenameNode(
                                future,
                                started,
                                logRequest);
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
        TInstant started,
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);
            return {NProto::ACTION_RENAME_NODE, started, response.GetError()};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "rename node %s has failed: %s",
                logRequest.GetNodeInfo().GetNodeName().c_str(),
                FormatError(error).c_str());
            return {NProto::ACTION_RENAME_NODE, started, error};
        }
    }

    TFuture<TCompletedRequest> DoUnlinkNode(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        // UnlinkNode      0.002605s       S_OK    {parent_node_id=3,
        // node_name=tfrgYZ1}

        if (Spec.GetNoWrite()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_REMOVE_NODE,
                Started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        auto name = logRequest.GetNodeInfo().GetNodeName();
        auto request = CreateRequest<NProto::TUnlinkNodeRequest>();
        request->SetName(name);
        const auto node =
            NodeIdMapped(logRequest.GetNodeInfo().GetParentNodeId());
        if (!node) {
            return MakeFuture(TCompletedRequest{});
        }
        request->SetNodeId(node);
        auto self = weak_from_this();
        return Session->UnlinkNode(CreateCallContext(), std::move(request))
            .Apply(
                [=, started = Started, name = std::move(name)](
                    const TFuture<NProto::TUnlinkNodeResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleUnlinkNode(future, name, started);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_REMOVE_NODE,
                        started,
                        MakeError(E_CANCELLED, "cancelled")};
                });
    }

    TCompletedRequest HandleUnlinkNode(
        const TFuture<NProto::TUnlinkNodeResponse>& future,
        const TString& name,
        TInstant started)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);

            return {NProto::ACTION_REMOVE_NODE, started, response.GetError()};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "unlink for %s has failed: %s",
                name.c_str(),
                FormatError(error).c_str());

            return {NProto::ACTION_REMOVE_NODE, started, error};
        }
    }
    TFuture<TCompletedRequest> DoDestroyHandle(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        //  DestroyHandle   0.002475s       S_OK    {node_id=10,
        // handle=61465562388172112}

        auto name = logRequest.GetNodeInfo().GetNodeName();

        auto request = CreateRequest<NProto::TDestroyHandleRequest>();

        const auto handle =
            HandleIdMapped(logRequest.GetNodeInfo().GetHandle());
        if (!handle) {
            return MakeFuture(TCompletedRequest{});
        }
        with_lock (StateLock) {
            HandlesLogToActual.erase(handle);
        }

        request->SetHandle(handle);

        auto self = weak_from_this();
        return Session->DestroyHandle(CreateCallContext(), std::move(request))
            .Apply(
                [=, started = Started, name = std::move(name)](
                    const TFuture<NProto::TDestroyHandleResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleDestroyHandle(name, future, started);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_DESTROY_HANDLE,
                        started,
                        MakeError(E_CANCELLED, "cancelled")};
                });
    }

    TFuture<TCompletedRequest> DoGetNodeAttr(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        const auto started = TInstant::Now();
        if (Spec.GetNoRead()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_GET_NODE_ATTR,
                started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        // TODO(proller): by parent + name        //
        // {"TimestampMcs":1726615533406265,"DurationMcs":192,"RequestType":33,"ErrorCode":2147942402,"NodeInfo":{"ParentNodeId":17033,"NodeName":"CPackSourceConfig.cmake","Flags":0,"Mode":0,"NodeId":0,"Handle":0,"Size":0}}
        // {"TimestampMcs":240399000,"DurationMcs":163,"RequestType":33,"NodeInfo":{"ParentNodeId":3,"NodeName":"branches","Flags":0,"Mode":0,"NodeId":0,"Handle":0,"Size":0}}
        // nfs     GetNodeAttr     0.006847s       S_OK    {parent_node_id=1,
        // node_name=freeminer, flags=0, mode=509, node_id=2, handle=0, size=0}

        auto request = CreateRequest<NProto::TGetNodeAttrRequest>();
        const auto node =
            NodeIdMapped(logRequest.GetNodeInfo().GetParentNodeId());
        if (!node) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_GET_NODE_ATTR,
                started,
                MakeError(
                    E_NOT_FOUND,
                    TStringBuilder{}
                        << "Node missing in mapping "
                        << logRequest.GetNodeInfo().GetParentNodeId())});
        }
        request->SetNodeId(node);
        auto name = logRequest.GetNodeInfo().GetNodeName();
        request->SetName(logRequest.GetNodeInfo().GetNodeName());
        request->SetFlags(logRequest.GetNodeInfo().GetFlags());
        auto self = weak_from_this();
        STORAGE_DEBUG(
            "GetNodeAttr client started name="
            << name << " node=" << node << " <- "
            << logRequest.GetNodeInfo().GetParentNodeId()   // why only parent?
            << " request=" << *request);
        return Session->GetNodeAttr(CreateCallContext(), std::move(request))
            .Apply(
                [=, name = std::move(name)](
                    const TFuture<NProto::TGetNodeAttrResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleGetNodeAttr(name, future, started);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_GET_NODE_ATTR,
                        started,
                        MakeError(E_CANCELLED, "cancelled")};
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

    TCompletedRequest HandleDestroyHandle(
        const TString& name,
        const TFuture<NProto::TDestroyHandleResponse>& future,
        TInstant started)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);

            return {NProto::ACTION_DESTROY_HANDLE, started, {}};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "destroy handle %s has failed: %s",
                name.c_str(),
                FormatError(error).c_str());

            return {NProto::ACTION_DESTROY_HANDLE, started, error};
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

        auto started = TInstant::Now();
        auto it = Handles.begin();
        while (it != Handles.end() &&
               (Locks.contains(it->first) || StagedLocks.contains(it->first)))
        {
            ++it;
        }

        if (it == Handles.end()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_ACQUIRE_LOCK,
                started,
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
                [=](const TFuture<NProto::TAcquireLockResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleAcquireLock(handle, future, started);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_ACQUIRE_LOCK,
                        started,
                        MakeError(E_CANCELLED, "cancelled")};
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

        auto started = TInstant::Now();
        auto self = weak_from_this();
        return Session->ReleaseLock(CreateCallContext(), std::move(request))
            .Apply(
                [=](const TFuture<NProto::TReleaseLockResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleReleaseLock(handle, future, started);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_RELEASE_LOCK,
                        started,
                        MakeError(E_CANCELLED, "cancelled")};
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
        const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& /*logRequest*/) override
    {
        return MakeFuture(TCompletedRequest{
            NProto::ACTION_RELEASE_LOCK,
            Started,
            MakeError(E_NOT_IMPLEMENTED, "invalid not implemented")});
    }

    template <typename T>
    std::shared_ptr<T> CreateRequest()
    {
        auto request = std::make_shared<T>();
        request->SetFileSystemId(FileSystemId);
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
