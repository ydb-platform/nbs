/*
TODO:
create file/dir modes
create handle modes (now rw)
compare log and actual result ( S_OK E_FS_NOENT ...)
read/write with multiranges (now only first processed)
*/

#include "request.h"

#include "cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h"
#include "cloud/filestore/tools/analytics/libs/event-log/dump.h"
#include "library/cpp/aio/aio.h"
#include "library/cpp/eventlog/iterator.h"
#include "library/cpp/testing/unittest/registar.h"
#include "util/folder/dirut.h"
#include "util/folder/path.h"
#include "util/system/fs.h"

#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service_local/lowlevel.h>
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

class TReplayRequestGeneratorFs final
    : public IRequestGenerator
    , public std::enable_shared_from_this<TReplayRequestGeneratorFs>
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
    THashMap<THandleLocal, TFile> OpenHandles;

    NAsyncIO::TAsyncIOService AsyncIO;
    ui64 TimestampMcs{};
    TInstant Started;

public:
    TReplayRequestGeneratorFs(
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

        if (Spec.GetReplayRoot().empty()) {
            ythrow yexception() << "ReplayRoot is not defined";
        }

        AsyncIO.Start();

        NEventLog::TOptions options;
        options.FileName = Spec.GetFileName();
        options.SetForceStrongOrdering(true);   // need this?
        EventlogIterator = CreateIterator(options);
        TFsPath(Spec.GetReplayRoot()).MkDirs();
    }

    ~TReplayRequestGeneratorFs()
    {
        AsyncIO.Stop();
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
                        case EFileStoreRequest::AccessNode:
                            return DoAccessNode(request);
                        case EFileStoreRequest::ListNodes:
                            return DoListNodes(request);

                            // TODO:
                        case EFileStoreRequest::AcquireLock:
                            return DoAcquireLock();
                        case EFileStoreRequest::ReleaseLock:
                            return DoReleaseLock();

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
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
    {
        // nfs     AccessNode      0.002297s       S_OK    {mask=4, node_id=36}

        if (Spec.GetNoRead()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_ACCESS_NODE,
                Started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        TGuard<TMutex> guard(StateLock);

        const auto node = NodeIdMapped(logRequest.GetNodeInfo().GetNodeId());

        if (!node) {
            STORAGE_ERROR(
                "access fail: " << " no node="
                                << logRequest.GetNodeInfo().GetNodeId());
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_ACCESS_NODE,
                Started,
                MakeError(E_FAIL, "cancelled")});
        }

        auto fname = Spec.GetReplayRoot() + "/" + PathByNode(node);
        int res = access(fname.c_str(), R_OK);
        STORAGE_DEBUG(
            "access " << node << " <- " << logRequest.GetNodeInfo().GetNodeId()
                      << " = " << res);
        return MakeFuture(
            TCompletedRequest{NProto::ACTION_ACCESS_NODE, Started, {}});
    }

    // Recursive, no infinity loop check
    TNodeLocal CreateDirIfMissingByNodeLog(TNodeLog nodeIdLog)
    {
        if (const auto& nodeIdLocal = NodeIdMapped(nodeIdLog)) {
            return nodeIdLocal;
        }

        const auto& it = KnownLogNodes.find(nodeIdLog);
        if (it == KnownLogNodes.end()) {
            return 0;
        }
        auto parent = NodeIdMapped(it->second.ParentLog);

        if (!parent && it->second.ParentLog &&
            nodeIdLog != it->second.ParentLog)
        {
            parent = CreateDirIfMissingByNodeLog(it->second.ParentLog);
        }

        {
            auto parentPath = PathByNode(NodeIdMapped(it->second.ParentLog));
            if (parentPath.empty() && parent) {
                parentPath = PathByNode(parent);
            }
            if (parentPath.empty()) {
                parentPath = "/__lost__/";
            }
            const auto name = parentPath +
                              (it->second.Name.empty() ? ToString(nodeIdLog)
                                                       : it->second.Name) +
                              "/";
            const auto nodeId =
                MakeDirectoryRecursive(Spec.GetReplayRoot() + name);
            NodePath[nodeId] = name;
            NodesLogToLocal[nodeIdLog] = nodeId;

            return nodeId;
        }
    }

    static EOpenModeFlag FileOpenFlags(ui32 flags, EOpenModeFlag init = {})
    {
        auto systemflags = HandleFlagsToSystem(flags);
        ui32 value{init};

        if (systemflags & O_RDWR) {
            value |= EOpenModeFlag::RdWr | EOpenModeFlag::OpenAlways;
        } else if (systemflags & O_WRONLY) {
            value |= EOpenModeFlag::WrOnly | EOpenModeFlag::OpenAlways;
        } else if (systemflags & O_RDONLY) {
            value |= EOpenModeFlag::RdOnly;
        }

        return static_cast<EOpenModeFlag>(value);
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

        TString relativePathName;
        if (logRequest.GetNodeInfo().GetNodeId()) {
            if (auto path = PathByNode(logRequest.GetNodeInfo().GetNodeId())) {
                relativePathName = path;
            }
        }

        if (relativePathName.empty()) {
            auto parentNode =
                NodeIdMapped(logRequest.GetNodeInfo().GetParentNodeId());
            if (!parentNode) {
                parentNode = NodeIdMapped(
                    KnownLogNodes[logRequest.GetNodeInfo().GetParentNodeId()]
                        .ParentLog);
            }

            if (!parentNode && logRequest.GetNodeInfo().GetParentNodeId() !=
                                   logRequest.GetNodeInfo().GetNodeId())
            {
                parentNode = CreateDirIfMissingByNodeLog(
                    logRequest.GetNodeInfo().GetParentNodeId());
            }

            if (!parentNode) {
                STORAGE_ERROR(
                    "create handle fail :"
                    << logRequest.GetNodeInfo().GetHandle() << " no parent="
                    << logRequest.GetNodeInfo().GetParentNodeId());
                return MakeFuture(TCompletedRequest{
                    NProto::ACTION_CREATE_HANDLE,
                    Started,
                    MakeError(E_FAIL, "cancelled")});
            }

            auto nodeName = logRequest.GetNodeInfo().GetNodeName();
            if (nodeName.empty() &&
                logRequest.GetNodeInfo().GetNodeId() !=
                    logRequest.GetNodeInfo().GetParentNodeId())
            {
                nodeName =
                    KnownLogNodes[logRequest.GetNodeInfo().GetNodeId()].Name;
            }
            const auto parentpath = PathByNode(parentNode);

            if (nodeName.empty() && IsDir(Spec.GetReplayRoot() + parentpath)) {
                nodeName =
                    KnownLogNodes[logRequest.GetNodeInfo().GetParentNodeId()]
                        .Name;
            }

            relativePathName = parentpath + nodeName;
        }
        STORAGE_DEBUG(
            "open " << relativePathName
                    << " handle=" << logRequest.GetNodeInfo().GetHandle()
                    << " flags=" << logRequest.GetNodeInfo().GetFlags() << " "
                    << HandleFlagsToString(logRequest.GetNodeInfo().GetFlags())
                    << " mode=" << logRequest.GetNodeInfo().GetMode()
                    << " node=" << logRequest.GetNodeInfo().GetNodeId());

        try {
            TFile fileHandle(
                Spec.GetReplayRoot() + relativePathName,
                // OpenAlways | (Spec.GetNoWrite() ? RdOnly : RdWr)
                FileOpenFlags(
                    logRequest.GetNodeInfo().GetFlags(),
                    Spec.GetNoWrite()        ? OpenExisting
                    : Spec.GetCreateOnRead() ? OpenAlways
                                             : OpenExisting));

            if (!fileHandle.IsOpen()) {
                return MakeFuture(TCompletedRequest{
                    NProto::ACTION_CREATE_HANDLE,
                    Started,
                    MakeError(E_FAIL, "fail")});
            }
            const auto fh = fileHandle.GetHandle();
            if (!fh) {
                return MakeFuture(TCompletedRequest{
                    NProto::ACTION_CREATE_HANDLE,
                    Started,
                    MakeError(E_FAIL, "no filehandle")});
            }

            OpenHandles[fh] = std::move(fileHandle);
            HandlesLogToActual[logRequest.GetNodeInfo().GetHandle()] = fh;
            const auto inode =
                TFileStat{Spec.GetReplayRoot() + relativePathName}.INode;
            if (logRequest.GetNodeInfo().GetNodeId()) {
                NodesLogToLocal[logRequest.GetNodeInfo().GetNodeId()] = inode;
                NodePath[inode] = relativePathName;
            }
            STORAGE_DEBUG(
                "open " << fh << "<-" << logRequest.GetNodeInfo().GetHandle()
                        << " inode=" << inode
                        << " known handles=" << HandlesLogToActual.size()
                        << " opened=" << OpenHandles.size());
        } catch (const TFileError& error) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_CREATE_HANDLE,
                Started,
                MakeError(E_FAIL, error.what())});
        }

        return MakeFuture(
            TCompletedRequest{NProto::ACTION_CREATE_HANDLE, Started, {}});
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
        NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
    {
        if (Spec.GetNoRead()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_READ,
                Started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        TGuard<TMutex> guard(StateLock);

        const auto handle = HandleIdMapped(logRequest.GetRanges(0).GetHandle());
        if (!handle) {
            STORAGE_WARN(
                "read: no handle "
                << logRequest.GetRanges(0).GetHandle()
                << " ranges size=" << logRequest.GetRanges().size()
                << " map size=" << HandlesLogToActual.size());
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_READ,
                Started,
                MakeError(E_FAIL, "cancelled")});
        }
        auto& fh = OpenHandles[handle];
        STORAGE_DEBUG(
            "Read from " << handle << " fh.len=" << fh.GetLength()
                         << " fh.pos=" << fh.GetPosition());
        auto buffer = Acalloc(logRequest.GetRanges().cbegin()->GetBytes());

        /*
                if (!fh.GetLength()) {
                    // incorrect aligned to read size, should use size from
           nodeattr fh.Reserve( logRequest.GetRanges().cbegin()->GetOffset() +
                        logRequest.GetRanges().cbegin()->GetBytes());
                }
        */

        TFileHandle FileHandle{fh.GetHandle()};

        const auto future = AsyncIO.Read(
            FileHandle,
            {},
            logRequest.GetRanges().cbegin()->GetBytes(),
            logRequest.GetRanges().cbegin()->GetOffset());
        FileHandle.Release();

        return future.Apply(
            [started = Started]([[maybe_unused]] const auto& future) mutable
            {
                if (future.GetValue()) {
                    return TCompletedRequest(NProto::ACTION_READ, started, {});
                }
                return TCompletedRequest(
                    NProto::ACTION_READ,
                    started,
                    MakeError(E_IO, TStringBuilder{} << "nothing read"));
            });
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
    {
        //{"TimestampMcs":1465489895000,"DurationMcs":2790,"RequestType":44,"Ranges":[{"NodeId":2,"Handle":20680158862113389,"Offset":13,"Bytes":12}]}

        if (Spec.GetNoWrite()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_WRITE,
                Started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }
        TGuard<TMutex> guard(StateLock);

        const auto logHandle = logRequest.GetRanges(0).GetHandle();
        const auto handle = HandleIdMapped(logHandle);
        if (!handle) {
            return MakeFuture(TCompletedRequest(
                NProto::ACTION_WRITE,
                Started,
                MakeError(
                    E_CANCELLED,
                    TStringBuilder{} << "write cancelled: no handle ="
                                     << logHandle)));   // todo
        }
        const auto bytes = logRequest.GetRanges(0).GetBytes();
        const auto offset = logRequest.GetRanges(0).GetOffset();

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

        auto& fh = OpenHandles[handle];

        STORAGE_DEBUG(
            "Write to " << handle << " fh.length=" << fh.GetLength()
                        << " fh.pos=" << fh.GetPosition());
        // TODO TEST USE AFTER FREE on buffer
        TFileHandle FileHandle{fh.GetHandle()};
        const auto writeFuture = AsyncIO.Write(
            // fh,
            FileHandle,
            buffer.data(),
            bytes,
            offset);
        FileHandle.Release();
        return writeFuture.Apply(
            [started = Started]([[maybe_unused]] const auto& future) mutable
            {
                if (future.GetValue()) {
                    return TCompletedRequest(NProto::ACTION_WRITE, started, {});
                }
                return TCompletedRequest(
                    NProto::ACTION_WRITE,
                    started,
                    MakeError(E_IO, TStringBuilder{} << "nothing writed"));
            });
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

        auto parentNode =
            NodeIdMapped(logRequest.GetNodeInfo().GetNewParentNodeId());

        parentNode = CreateDirIfMissingByNodeLog(
            logRequest.GetNodeInfo().GetNewParentNodeId());

        auto fullName = Spec.GetReplayRoot() + "/" + PathByNode(parentNode) +
                        logRequest.GetNodeInfo().GetNewNodeName();

        ui64 nodeid = 0;
        bool isDir = false;
        switch (logRequest.GetNodeInfo().GetType()) {
            case NProto::E_REGULAR_NODE: {
                // TODO: transform r.GetNodeInfo().GetMode() to correct open
                // mode
                TFileHandle fh(
                    fullName,
                    OpenAlways | (Spec.GetNoWrite() ? RdOnly : RdWr));
                if (fh) {
                    nodeid = TFileStat{fh}.INode;
                } else {
                    nodeid = TFileStat{fullName}.INode;
                }

                if (logRequest.GetNodeInfo().GetSize()) {
                    fh.Reserve(logRequest.GetNodeInfo().GetSize());
                }
            } break;
            case NProto::E_DIRECTORY_NODE: {
                isDir = true;
                nodeid = MakeDirectoryRecursive(fullName);
            } break;
            case NProto::E_LINK_NODE: {
                // {"TimestampMcs":1727703903595285,"DurationMcs":2432,"RequestType":26,"ErrorCode":0,"NodeInfo":{"NewParentNodeId":267,"NewNodeName":"pack-ebe666445578da0c6157f4172ad581cd731742ec.idx","Mode":292,"Type":3,"NodeId":274,"Size":245792}}

                const auto targetNode =
                    NodeIdMapped(logRequest.GetNodeInfo().GetNodeId());
                const auto targetFullName =
                    Spec.GetReplayRoot() + "/" + PathByNode(targetNode);
                NFs::HardLink(targetFullName, fullName);
            } break;
            case NProto::E_SYMLINK_NODE: {
                const auto targetNode =
                    NodeIdMapped(logRequest.GetNodeInfo().GetNodeId());
                const auto targetFullName =
                    Spec.GetReplayRoot() + "/" + PathByNode(targetNode);
                NFs::SymLink(targetFullName, fullName);
            } break;
            case NProto::E_SOCK_NODE:
                return MakeFuture(TCompletedRequest{
                    NProto::ACTION_CREATE_NODE,
                    Started,
                    MakeError(E_NOT_IMPLEMENTED, "sock not implemented")});

            case NProto::E_INVALID_NODE:
                return MakeFuture(TCompletedRequest{
                    NProto::ACTION_CREATE_NODE,
                    Started,
                    MakeError(E_NOT_IMPLEMENTED, "invalid not implemented")});
        }

        if (!nodeid) {
            nodeid = TFileStat{fullName}.INode;
        }

        // CreateIfMissing(PathByNode())
        if (nodeid) {
            NodesLogToLocal[logRequest.GetNodeInfo().GetNodeId()] = nodeid;
            NodePath[nodeid] = PathByNode(parentNode) +
                               logRequest.GetNodeInfo().GetNewNodeName() +
                               (isDir ? "/" : "");
        }

        return MakeFuture(
            TCompletedRequest(NProto::ACTION_CREATE_NODE, Started, {}));
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

        const auto parentnodeid =
            NodeIdMapped(logRequest.GetNodeInfo().GetParentNodeId());

        auto fullName = Spec.GetReplayRoot() + PathByNode(parentnodeid) +
                        logRequest.GetNodeInfo().GetNodeName();

        const auto newparentnodeid =
            NodeIdMapped(logRequest.GetNodeInfo().GetNewParentNodeId());

        auto newFullName = Spec.GetReplayRoot() + PathByNode(newparentnodeid) +
                           logRequest.GetNodeInfo().GetNewNodeName();

        const auto renameres = NFs::Rename(fullName, newFullName);

        STORAGE_DEBUG(
            "rename " << fullName << " => " << newFullName << " : " << renameres
                      << (renameres
                              ? (TStringBuilder{}
                                 << " errno=" << LastSystemError() << " err="
                                 << LastSystemErrorText(LastSystemError()))
                              : TStringBuilder{}));
        return MakeFuture(
            TCompletedRequest{NProto::ACTION_RENAME_NODE, Started, {}});
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

        const auto parentNodeId =
            NodeIdMapped(logRequest.GetNodeInfo().GetParentNodeId());
        if (!parentNodeId) {
            STORAGE_WARN(
                "unlink : no parent orig="
                << logRequest.GetNodeInfo().GetParentNodeId());
            return MakeFuture(TCompletedRequest(
                NProto::ACTION_REMOVE_NODE,
                Started,
                MakeError(E_CANCELLED, "cancelled")));
        }
        const auto fullName = Spec.GetReplayRoot() + "/" +
                              PathByNode(parentNodeId) +
                              logRequest.GetNodeInfo().GetNodeName();
        const auto unlinkres = NFs::Remove(fullName);
        STORAGE_DEBUG("unlink " << fullName << " = " << unlinkres);
        // TODO :
        // NodesLogToActual.erase(...)
        // NodePath.erase(...)
        return MakeFuture(
            TCompletedRequest(NProto::ACTION_REMOVE_NODE, Started, {}));
    }

    TFuture<TCompletedRequest> DoDestroyHandle(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
    {
        //  DestroyHandle   0.002475s       S_OK    {node_id=10,
        // handle=61465562388172112}
        TGuard<TMutex> guard(StateLock);

        const auto handleid =
            HandleIdMapped(logRequest.GetNodeInfo().GetHandle());

        const auto& it = OpenHandles.find(handleid);
        if (it == OpenHandles.end()) {
            return MakeFuture(TCompletedRequest(
                NProto::ACTION_DESTROY_HANDLE,
                Started,
                MakeError(
                    E_CANCELLED,
                    TStringBuilder{} << "close " << handleid << " <- "
                                     << logRequest.GetNodeInfo().GetHandle()
                                     << " fail: not found in "
                                     << OpenHandles.size())));
        }

        auto& fhandle = it->second;
        const auto len = fhandle.GetLength();
        const auto pos = fhandle.GetPosition();
        fhandle.Close();
        OpenHandles.erase(handleid);
        HandlesLogToActual.erase(logRequest.GetNodeInfo().GetHandle());
        STORAGE_DEBUG(
            "Close " << handleid << " <- "
                     << logRequest.GetNodeInfo().GetHandle() << " pos=" << pos
                     << " len=" << len
                     << " open map size=" << OpenHandles.size()
                     << " map size=" << HandlesLogToActual.size());
        return MakeFuture(
            TCompletedRequest(NProto::ACTION_DESTROY_HANDLE, Started, {}));
    }

    TFuture<TCompletedRequest> DoGetNodeAttr(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
    {
        if (Spec.GetNoRead()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_GET_NODE_ATTR,
                Started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        TGuard<TMutex> guard(StateLock);

        // TODO: by ParentNodeId + NodeName        //
        // {"TimestampMcs":1726503153650998,"DurationMcs":7163,"RequestType":35,"ErrorCode":2147942422,"NodeInfo":{"NodeName":"security.capability","NewNodeName":"","NodeId":5,"Size":0}}
        // {"TimestampMcs":1726615533406265,"DurationMcs":192,"RequestType":33,"ErrorCode":2147942402,"NodeInfo":{"ParentNodeId":17033,"NodeName":"CPackSourceConfig.cmake","Flags":0,"Mode":0,"NodeId":0,"Handle":0,"Size":0}}
        // {"TimestampMcs":240399000,"DurationMcs":163,"RequestType":33,"NodeInfo":{"ParentNodeId":3,"NodeName":"branches","Flags":0,"Mode":0,"NodeId":0,"Handle":0,"Size":0}}
        // {"TimestampMcs":1727464381415468,"DurationMcs":1982,"RequestType":33,"ErrorCode":2147942402,"NodeInfo":{"ParentNodeId":2,"NodeName":"libc.so.6","Flags":0,"Mode":0,"NodeId":0,"Handle":0,"Size":0}}
        // nfs     GetNodeAttr     0.006847s       S_OK    {parent_node_id=1,
        // node_name=freeminer, flags=0, mode=509, node_id=2, handle=0, size=0}

        if (logRequest.GetNodeInfo().GetNodeName()) {
            KnownLogNodes[logRequest.GetNodeInfo().GetNodeId()].Name =
                logRequest.GetNodeInfo().GetNodeName();
        }
        if (logRequest.GetNodeInfo().GetParentNodeId() &&
            logRequest.GetNodeInfo().GetParentNodeId() !=
                logRequest.GetNodeInfo().GetNodeId())
        {
            KnownLogNodes[logRequest.GetNodeInfo().GetNodeId()].ParentLog =
                logRequest.GetNodeInfo().GetParentNodeId();
        }

        // TODO: can create and truncate to size here missing files

        const auto nodeid = NodeIdMapped(logRequest.GetNodeInfo().GetNodeId());

        if (!nodeid) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_GET_NODE_ATTR,
                Started,
                MakeError(
                    E_NOT_FOUND,
                    TStringBuilder{} << "Node not found "
                                     << logRequest.GetNodeInfo().GetNodeId()
                                     << " in " << NodesLogToLocal.size())});
        }

        auto fname = Spec.GetReplayRoot() + "/" + PathByNode(nodeid);
        [[maybe_unused]] const auto stat = TFileStat{fname};
        return MakeFuture(
            TCompletedRequest(NProto::ACTION_GET_NODE_ATTR, Started, {}));
    }

    TFuture<TCompletedRequest> DoAcquireLock()
    {
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
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
    {
        // json={"TimestampMcs":1726615510721016,"DurationMcs":3329,"RequestType":30,"ErrorCode":0,"NodeInfo":{"NodeId":164,"Size":10}}

        TGuard<TMutex> guard(StateLock);

        const auto nodeid = NodeIdMapped(logRequest.GetNodeInfo().GetNodeId());
        if (!nodeid) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_LIST_NODES,
                Started,
                MakeError(
                    E_CANCELLED,
                    TStringBuilder{} << "Node not found in mapping"
                                     << nodeid)});
        }

        // if (!Spec.GetNoWrite()) {
        //    CreateDirIfMissingByNodeLog(r.GetNodeInfo().GetNodeId());
        // }

        auto path = Spec.GetReplayRoot() + "/" + PathByNode(nodeid);
        TFileHandle dir{path, RdOnly};
        if (!dir.IsOpen()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_LIST_NODES,
                Started,
                MakeError(E_CANCELLED, "cancelled")});
        }
        const auto dirs = NLowLevel::ListDirAt(dir, true);
        if (logRequest.GetNodeInfo().GetSize() != dirs.size()) {
            STORAGE_DEBUG(
                "Dir size differs "
                << path << " log=" << logRequest.GetNodeInfo().GetSize()
                << " local=" << dirs.size());
        }

        return MakeFuture(
            TCompletedRequest(NProto::ACTION_LIST_NODES, Started, {}));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRequestGeneratorPtr CreateReplayRequestGeneratorFs(
    NProto::TReplaySpec spec,
    ILoggingServicePtr logging,
    ISessionPtr session,
    TString filesystemId,
    NProto::THeaders headers)
{
    return std::make_shared<TReplayRequestGeneratorFs>(
        std::move(spec),
        std::move(logging),
        std::move(session),
        std::move(filesystemId),
        std::move(headers));
}

}   // namespace NCloud::NFileStore::NLoadTest
