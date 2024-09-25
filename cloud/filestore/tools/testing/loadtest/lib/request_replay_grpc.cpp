/*

TODO:
create file/dir modes
create handle modes (now rw)
compare log and actual result ( S_OK E_FS_NOENT ...)

*/

#include "request.h"

#include "cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h"
#include "cloud/filestore/tools/analytics/libs/event-log/dump.h"
#include "library/cpp/eventlog/iterator.h"
#include "library/cpp/testing/unittest/registar.h"
#include "util/system/fs.h"

#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/public/api/protos/data.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

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

namespace NCloud::NFileStore::NLoadTest {

using namespace NThreading;
using namespace NCloud::NFileStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReplayRequestGeneratorGRPC final
    : public IRequestGenerator
    , public std::enable_shared_from_this<TReplayRequestGeneratorGRPC>
{
private:
    const NProto::TReplaySpec Spec;
    TString FileSystemId;
    const NProto::THeaders Headers;

    TLog Log;

    ISessionPtr Session;

    TVector<std::pair<ui64, NProto::EAction>> Actions;

    TMutex StateLock;

    using THandleLog = ui64;
    using THandleLocal = ui64;
    using TNodeLog = ui64;
    using TNodeLocal = ui64;

    ui64 InitialFileSize = 0;

    std::atomic<ui64> LastRequestId = 0;

    THolder<NEventLog::IIterator> EventlogIterator;
    TConstEventPtr EventPtr;
    int EventMessageNumber = 0;
    NProto::TProfileLogRecord* EventLogMessagePtr{};

    static constexpr ui32 LockLength = 4096;

    const ui64 OwnerId = RandomNumber(100500u);

    struct TNode
    {
        TString Name;
        NProto::TNodeAttr Attrs;

        TNodeLog ParentLog = 0;
    };

    struct THandle
    {
        TString Path;
        ui64 Handle = 0;
    };

    THashMap<ui64, THandle> Handles;
    THashSet<ui64> Locks;
    THashSet<ui64> StagedLocks;

    THashMap<TNodeLog, TNodeLocal> NodesLogToLocal{{RootNodeId, RootNodeId}};
    THashMap<TNodeLocal, TString> NodePath{{RootNodeId, "/"}};
    THashMap<TNodeLog, TNode> KnownLogNodes;

    THashMap<THandleLog, THandleLocal> HandlesLogToActual;
    // THashMap<THandleLocal, TFile> OpenHandles;

    ui64 TimestampMcs{};
    TInstant Started;

public:
    TReplayRequestGeneratorGRPC(
        NProto::TReplaySpec spec,
        ILoggingServicePtr logging,
        ISessionPtr session,
        TString filesystemId,
        NProto::THeaders headers)
        : Spec(std::move(spec))
        , FileSystemId(std::move(filesystemId))
        , Headers(std::move(headers))
        , Session(std::move(session))
    {
        Log = logging->CreateLog(Headers.GetClientId());

        NEventLog::TOptions options;
        options.FileName = Spec.GetFileName();
        options.SetForceStrongOrdering(true);   // need this?
        EventlogIterator = CreateIterator(options);
    }

    ~TReplayRequestGeneratorGRPC()
    {}

    bool InstantProcessQueue() override
    {
        return true;
    }
    bool FailOnError() override
    {
        return false;
    }

    TNodeLocal NodeIdMapped(const TNodeLog id)
    {
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

    void Advance()
    {
        EventPtr = EventlogIterator->Next();
        if (!EventPtr) {
            return;
        }

        EventLogMessagePtr = const_cast<NProto::TProfileLogRecord*>(
            dynamic_cast<const NProto::TProfileLogRecord*>(
                EventPtr->GetProto()));
        if (!EventLogMessagePtr) {
            return;
        }

        if (FileSystemId.empty()) {
            FileSystemId = TString{EventLogMessagePtr->GetFileSystemId()};
        }

        EventMessageNumber = EventLogMessagePtr->GetRequests().size();
    }

    bool HasNextRequest() override
    {
        if (!EventPtr) {
            Advance();
        }
        return !!EventPtr;
    }

    TInstant NextRequestAt() override
    {
        return TInstant::Max();
    }

    NThreading::TFuture<TCompletedRequest> ExecuteNextRequest() override
    {
        if (!HasNextRequest()) {
            return MakeFuture(TCompletedRequest(true));
        }

        for (; EventPtr; Advance()) {
            if (!EventLogMessagePtr) {
                continue;
            }

            for (; EventMessageNumber > 0;) {
                auto request =
                    EventLogMessagePtr->GetRequests()[--EventMessageNumber];

                {
                    auto timediff = (request.GetTimestampMcs() - TimestampMcs) *
                                    Spec.GetTimeScale();
                    TimestampMcs = request.GetTimestampMcs();
                    if (timediff > 1000000) {
                        timediff = 0;
                    }
                    const auto current = TInstant::Now();
                    auto diff = current - Started;

                    if (timediff > diff.MicroSeconds()) {
                        auto slp = TDuration::MicroSeconds(
                            timediff - diff.MicroSeconds());
                        STORAGE_DEBUG(
                            "sleep=" << slp << " timediff=" << timediff
                                     << " diff=" << diff);

                        Sleep(slp);
                    }
                    Started = current;
                }

                STORAGE_DEBUG(
                    "Processing message "
                    << EventMessageNumber
                    << " typename=" << request.GetTypeName()
                    << " type=" << request.GetRequestType() << " "
                    << " name=" << RequestName(request.GetRequestType())
                    << " json=" << request.AsJSON());

                {
                    const auto& action = request.GetRequestType();
                    switch (static_cast<EFileStoreRequest>(action)) {
                        case EFileStoreRequest::ReadData:
                            return DoReadData(request);
                        case EFileStoreRequest::WriteData:
                            return DoWrite(request);
                        case EFileStoreRequest::CreateNode:
                            return DoCreateNode(request);
                        case EFileStoreRequest::RenameNode:
                            return DoRenameNode(request);
                        case EFileStoreRequest::UnlinkNode:
                            return DoUnlinkNode(request);
                        case EFileStoreRequest::CreateHandle:
                            return DoCreateHandle(request);
                        case EFileStoreRequest::DestroyHandle:
                            return DoDestroyHandle(request);
                        case EFileStoreRequest::GetNodeAttr:
                            return DoGetNodeAttr(request);
                        case EFileStoreRequest::AcquireLock:
                            return DoAcquireLock();
                        case EFileStoreRequest::ReleaseLock:
                            return DoReleaseLock();

                        case EFileStoreRequest::AccessNode:
                            return DoAccessNode(request);

                        case EFileStoreRequest::ReadBlob:
                        case EFileStoreRequest::WriteBlob:
                        case EFileStoreRequest::GenerateBlobIds:
                        case EFileStoreRequest::PingSession:
                        case EFileStoreRequest::Ping:
                            continue;
                        default:
                            STORAGE_INFO(
                                "Uninmplemented action="
                                << action << " "
                                << RequestName(request.GetRequestType()));
                            continue;
                    }
                }
            }
        }
        STORAGE_INFO(
            "Log finished n=" << EventMessageNumber << " ptr=" << !!EventPtr);

        return MakeFuture(TCompletedRequest(true));
    }

private:
    TFuture<TCompletedRequest> DoAccessNode(
        [[maybe_unused]] const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& logRequest)
    {
        // nfs     AccessNode      0.002297s       S_OK    {mask=4, node_id=36}

        if (Spec.GetNoRead()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_ACCESS_NODE,
                Started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        TGuard<TMutex> guard(StateLock);

        return MakeFuture(TCompletedRequest{
            NProto::ACTION_ACCESS_NODE,
            Started,
            MakeError(E_NOT_IMPLEMENTED, "not implemented")});
    }

    TFuture<TCompletedRequest> DoCreateHandle(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
    {
        // json={"TimestampMcs":1726503808715698,"DurationMcs":2622,"RequestType":38,"ErrorCode":0,"NodeInfo":{"ParentNodeId":13882,"NodeName":"compile_commands.json.tmpdf020","Flags":38,"Mode":436,"NodeId":15553,"Handle":46923415058768564,"Size":0}}
        // {"TimestampMcs":1725895168384258,"DurationMcs":2561,"RequestType":38,"ErrorCode":0,"NodeInfo":{"ParentNodeId":12527,"NodeName":"index.lock","Flags":15,"Mode":436,"NodeId":12584,"Handle":65382484937735195,"Size":0}}
        // nfs     CreateHandle    0.004161s       S_OK    {parent_node_id=65,
        // node_name=ini, flags=14, mode=436, node_id=66,
        // handle=11024287581389312, size=0}

        TGuard<TMutex> guard(StateLock);
        auto started = TInstant::Now();

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

        auto self = weak_from_this();
        return Session->CreateHandle(CreateCallContext(), std::move(request))
            .Apply(
                [=, name = std::move(name)](
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
    }

    TFuture<TCompletedRequest> HandleCreateHandle(
        const TFuture<NProto::TCreateHandleResponse>& future,
        const TString& name,
        TInstant started,
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
    {
        try {
            auto response = future.GetValue();
            CheckResponse(response);

            auto handle = response.GetHandle();
            with_lock (StateLock) {
                HandlesLogToActual[logRequest.GetNodeInfo().GetHandle()] =
                    handle;
            }

            NThreading::TFuture<NProto::TSetNodeAttrResponse> setAttr;
            if (InitialFileSize) {
                static const int flags =
                    ProtoFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);

                auto request = CreateRequest<NProto::TSetNodeAttrRequest>();
                request->SetHandle(handle);
                request->SetNodeId(response.GetNodeAttr().GetId());
                request->SetFlags(flags);
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

    static constexpr ui32 BlockSize = 4_KB;
    static std::shared_ptr<char> Acalloc(ui64 dataSize)
    {
        std::shared_ptr<char> buffer = {
            static_cast<char*>(aligned_alloc(BlockSize, dataSize)),
            [](auto* p)
            {
                free(p);
            }};
        memset(buffer.get(), 0, dataSize);

        return buffer;
    }
    TFuture<TCompletedRequest> DoReadData(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
    {
        const auto started = TInstant::Now();
        if (Spec.GetNoRead()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_READ,
                started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        TGuard<TMutex> guard(StateLock);

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
            auto response = future.GetValue();
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

    TString MakeBuffer(ui64 bytes, ui64 offset = 0, const TString& start = {})
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
    {
        //{"TimestampMcs":1465489895000,"DurationMcs":2790,"RequestType":44,"Ranges":[{"NodeId":2,"Handle":20680158862113389,"Offset":13,"Bytes":12}]}

        const auto started = TInstant::Now();
        if (Spec.GetNoWrite()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_WRITE,
                started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }
        TGuard<TMutex> guard(StateLock);

        auto request = CreateRequest<NProto::TWriteDataRequest>();
        const auto handle = HandleIdMapped(logRequest.GetRanges(0).GetHandle());

        if (!handle) {
            return MakeFuture(TCompletedRequest{});
        }
        request->SetHandle(handle);

        request->SetOffset(logRequest.GetRanges(0).GetOffset());
        auto bytes = logRequest.GetRanges(0).GetBytes();
        auto buffer = NUnitTest::RandomString(bytes);

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
            auto response = future.GetValue();
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

    TString PathByNode(TNodeLocal nodeid)
    {
        if (const auto& it = NodePath.find(nodeid); it != NodePath.end()) {
            return it->second;
        }
        return {};
    }

    static TNodeLocal MakeDirectoryRecursive(const TString& name)
    {
        NFs::MakeDirectoryRecursive(name);
        const auto inode = TFileStat{name}.INode;
        return inode;
    }

    TFuture<TCompletedRequest> DoCreateNode(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
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

        TGuard<TMutex> guard(StateLock);

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
                // TODO:
                //  request->MutableLink()->SetTargetNode(r.GetNodeInfo().Get...);
                //  request->MutableLink()->SetFollowerNodeName(r.GetNodeInfo().Get...);
                return MakeFuture(TCompletedRequest{});
                break;
            case NProto::E_SYMLINK_NODE:
                // TODO:
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
        return Session->CreateNode(CreateCallContext(), std::move(request))
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
    }

    TCompletedRequest HandleCreateNode(
        const TFuture<NProto::TCreateNodeResponse>& future,
        const TString& name,
        TInstant started,
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
    {
        TGuard<TMutex> guard(StateLock);

        try {
            auto response = future.GetValue();
            CheckResponse(response);
            // Nodes[name] = TNode{name, response.GetNode()};
            if (response.GetNode().GetId()) {
                NodesLogToLocal[logRequest.GetNodeInfo().GetNodeId()] =
                    response.GetNode().GetId();
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

        TGuard<TMutex> guard(StateLock);

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
        return Session->RenameNode(CreateCallContext(), std::move(request))
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
                        MakeError(E_CANCELLED, "cancelled")};
                });
    }

    TCompletedRequest HandleRenameNode(
        const TFuture<NProto::TRenameNodeResponse>& future,
        TInstant started,
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
    {
        TGuard<TMutex> guard(StateLock);
        try {
            auto response = future.GetValue();
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
    {
        // UnlinkNode      0.002605s       S_OK    {parent_node_id=3,
        // node_name=tfrgYZ1}

        if (Spec.GetNoWrite()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_REMOVE_NODE,
                Started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        TGuard<TMutex> guard(StateLock);

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
            auto response = future.GetValue();
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
    {
        //  DestroyHandle   0.002475s       S_OK    {node_id=10,
        // handle=61465562388172112}
        TGuard<TMutex> guard(StateLock);

        auto name = logRequest.GetNodeInfo().GetNodeName();

        auto request = CreateRequest<NProto::TDestroyHandleRequest>();

        const auto handle =
            HandleIdMapped(logRequest.GetNodeInfo().GetHandle());
        if (!handle) {
            return MakeFuture(TCompletedRequest{});
        }

        HandlesLogToActual.erase(handle);

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
    {
        const auto started = TInstant::Now();
        if (Spec.GetNoRead()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_GET_NODE_ATTR,
                started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        TGuard<TMutex> guard(StateLock);

        // TODO: by parent + name        //
        // {"TimestampMcs":1726503153650998,"DurationMcs":7163,"RequestType":35,"ErrorCode":2147942422,"NodeInfo":{"NodeName":"security.capability","NewNodeName":"","NodeId":5,"Size":0}}
        // {"TimestampMcs":1726615533406265,"DurationMcs":192,"RequestType":33,"ErrorCode":2147942402,"NodeInfo":{"ParentNodeId":17033,"NodeName":"CPackSourceConfig.cmake","Flags":0,"Mode":0,"NodeId":0,"Handle":0,"Size":0}}
        // {"TimestampMcs":240399000,"DurationMcs":163,"RequestType":33,"NodeInfo":{"ParentNodeId":3,"NodeName":"branches","Flags":0,"Mode":0,"NodeId":0,"Handle":0,"Size":0}}
        // nfs     GetNodeAttr     0.006847s       S_OK    {parent_node_id=1,
        // node_name=freeminer, flags=0, mode=509, node_id=2, handle=0, size=0}

        auto request = CreateRequest<NProto::TGetNodeAttrRequest>();
        const auto node =
            NodeIdMapped(logRequest.GetNodeInfo().GetParentNodeId());
        if (!node) {
            return MakeFuture(TCompletedRequest{});
        }
        request->SetNodeId(node);
        auto name = logRequest.GetNodeInfo().GetNodeName();
        request->SetName(logRequest.GetNodeInfo().GetNodeName());
        request->SetFlags(logRequest.GetNodeInfo().GetFlags());
        auto self = weak_from_this();
        STORAGE_DEBUG("GetNodeAttr client started");
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
            auto response = future.GetValue();
            STORAGE_DEBUG("GetNodeAttr client completed");
            CheckResponse(response);
            TGuard<TMutex> guard(StateLock);
            return {NProto::ACTION_GET_NODE_ATTR, started, {}};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "get node attr %s has failed: %s",
                name.c_str(),
                FormatError(error).c_str());

            return {NProto::ACTION_GET_NODE_ATTR, started, {}};
        }
    }

    TCompletedRequest HandleDestroyHandle(
        const TString& name,
        const TFuture<NProto::TDestroyHandleResponse>& future,
        TInstant started)
    {
        try {
            auto response = future.GetValue();
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

    TFuture<TCompletedRequest> DoAcquireLock()
    {
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

    TFuture<TCompletedRequest> DoReleaseLock()
    {
        TGuard<TMutex> guard(StateLock);
        if (Locks.empty()) {
            return DoAcquireLock();
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
