/*
TODO:
create file/dir modes
create handle modes (now rw)
compare log and actual result ( S_OK E_FS_NOENT ...)
read/write with multiranges (now only first processed)
*/

#include "request_replay.h"

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/request_i.h>
#include <cloud/filestore/libs/service_local/lowlevel.h>
#include <cloud/filestore/public/api/protos/data.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>
#include <cloud/filestore/tools/testing/loadtest/protos/loadtest.pb.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/aio/aio.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/generic/hash_set.h>
#include <util/string/builder.h>
#include <util/system/fstat.h>

namespace NCloud::NFileStore::NLoadTest {

using namespace NThreading;
using namespace NCloud::NFileStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReplayRequestGeneratorFs final
    : public IReplayRequestGenerator
    , public std::enable_shared_from_this<TReplayRequestGeneratorFs>
{
private:
    NAsyncIO::TAsyncIOService AsyncIO;
    TMutex StateLock;

    using THandleLog = ui64;
    using THandleLocal = ui64;

    using TNodeLog = ui64;
    using TNodeLocal = ui64;

    // Map between log and actual local fs node id's
    THashMap<TNodeLog, TNodeLocal> NodesLogToLocal{{RootNodeId, RootNodeId}};

    // Relative to local root path of node. Dirs ends with /
    THashMap<TNodeLocal, TFsPath> NodePath;

    // Collected info for log node id's
    struct TNode
    {
        TString Name;
        TNodeLog ParentLog = 0;
    };
    THashMap<TNodeLog, TNode> KnownLogNodes;

    // Map between log and actual local open handle id's
    THashMap<THandleLog, THandleLocal> HandlesLogToActual;

    THashMap<THandleLocal, TFile> OpenHandles;

    const TString LostName = "__lost__";

public:
    TReplayRequestGeneratorFs(
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
        if (Spec.GetReplayRoot().empty()) {
            ythrow yexception() << "ReplayRoot is not defined";
        }
        NodePath.emplace(RootNodeId, Spec.GetReplayRoot());

        AsyncIO.Start();
    }

    ~TReplayRequestGeneratorFs()
    {
        AsyncIO.Stop();
    }

    TNodeLocal NodeIdMapped(const TNodeLog id)
    {
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
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        TGuard<TMutex> guard(StateLock);

        const auto node = NodeIdMapped(logRequest.GetNodeInfo().GetNodeId());

        if (node == InvalidNodeId) {
            STORAGE_ERROR(
                "Access fail: no node=%lu",
                logRequest.GetNodeInfo().GetNodeId());
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_ACCESS_NODE,
                Started,
                MakeError(E_FAIL, "cancelled")});
        }

        const auto fname = PathByNode(node);
        int res = access(fname.c_str(), R_OK);
        STORAGE_DEBUG(
            "Access %lu <- %lu = %d ",
            node,
            logRequest.GetNodeInfo().GetNodeId(),
            res);
        return MakeFuture(
            TCompletedRequest{NProto::ACTION_ACCESS_NODE, Started, {}});
    }

    // Recursive, no infinity loop check
    TNodeLocal CreateDirIfMissingByNodeLog(TNodeLog nodeIdLog)
    {
        if (const auto& nodeIdLocal = NodeIdMapped(nodeIdLog);
            nodeIdLocal != InvalidNodeId)
        {
            return nodeIdLocal;
        }

        const auto& it = KnownLogNodes.find(nodeIdLog);
        if (it == KnownLogNodes.end()) {
            return 0;
        }
        auto parent = NodeIdMapped(it->second.ParentLog);

        if (parent == InvalidNodeId && it->second.ParentLog &&
            nodeIdLog != it->second.ParentLog)
        {
            parent = CreateDirIfMissingByNodeLog(it->second.ParentLog);
        }

        {
            auto parentPath = PathByNode(NodeIdMapped(it->second.ParentLog));
            if (parentPath.IsDefined() && parent) {
                parentPath = PathByNode(parent);
            }
            if (parentPath.IsDefined()) {
                parentPath = PathByNode(RootNodeId) / LostName / "_nodeid_" +
                             ToString(it->second.ParentLog);
            }
            const auto name =
                parentPath / (it->second.Name.empty()
                                  ? "_nodeid_" + ToString(nodeIdLog)
                                  : it->second.Name);
            const auto nodeId = MakeDirectoryRecursive(name);
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
        override
    {
        // json={"TimestampMcs":1726503808715698,"DurationMcs":2622,"RequestType":38,"ErrorCode":0,"NodeInfo":{"ParentNodeId":13882,"NodeName":"compile_commands.json.tmpdf020","Flags":38,"Mode":436,"NodeId":15553,"Handle":46923415058768564,"Size":0}}
        // {"TimestampMcs":1725895168384258,"DurationMcs":2561,"RequestType":38,"ErrorCode":0,"NodeInfo":{"ParentNodeId":12527,"NodeName":"index.lock","Flags":15,"Mode":436,"NodeId":12584,"Handle":65382484937735195,"Size":0}}
        // nfs     CreateHandle    0.004161s       S_OK    {parent_node_id=65,
        // node_name=ini, flags=14, mode=436, node_id=66,
        // handle=11024287581389312, size=0}

        TGuard<TMutex> guard(StateLock);

        TFsPath relativePathName;
        if (logRequest.GetNodeInfo().GetNodeId()) {
            if (const auto path =
                    PathByNode(logRequest.GetNodeInfo().GetNodeId()))
            {
                relativePathName = path;
            }
        }

        if (relativePathName.IsDefined()) {
            auto parentNode =
                NodeIdMapped(logRequest.GetNodeInfo().GetParentNodeId());
            if (parentNode == InvalidNodeId) {
                parentNode = NodeIdMapped(
                    KnownLogNodes[logRequest.GetNodeInfo().GetParentNodeId()]
                        .ParentLog);
            }

            if (parentNode == InvalidNodeId &&
                logRequest.GetNodeInfo().GetParentNodeId() !=
                    logRequest.GetNodeInfo().GetNodeId())
            {
                parentNode = CreateDirIfMissingByNodeLog(
                    logRequest.GetNodeInfo().GetParentNodeId());
            }

            if (parentNode == InvalidNodeId) {
                STORAGE_ERROR(
                    "Create handle %lu fail: no parent=%lu",
                    logRequest.GetNodeInfo().GetHandle(),
                    logRequest.GetNodeInfo().GetParentNodeId());
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

            if (nodeName.empty() && parentpath.IsDirectory()) {
                nodeName =
                    KnownLogNodes[logRequest.GetNodeInfo().GetParentNodeId()]
                        .Name;
            }

            relativePathName = parentpath / nodeName;
        }
        STORAGE_DEBUG(
            "Open %s handle=%lu flags=%d (%s) mode=%d node=%lu",
            relativePathName.c_str(),
            logRequest.GetNodeInfo().GetHandle(),
            logRequest.GetNodeInfo().GetFlags(),
            HandleFlagsToString(logRequest.GetNodeInfo().GetFlags()).c_str(),
            logRequest.GetNodeInfo().GetMode(),
            logRequest.GetNodeInfo().GetNodeId());

        try {
            EOpenModeFlag mode{};
            if (Spec.GetCreateOnRead()) {
                mode = OpenAlways;
            } else {
                mode = OpenExisting;
            }
            TFile fileHandle(
                relativePathName,
                FileOpenFlags(logRequest.GetNodeInfo().GetFlags(), mode));

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

            OpenHandles[fh] = fileHandle;
            HandlesLogToActual[logRequest.GetNodeInfo().GetHandle()] = fh;
            const auto stat = TFileStat{relativePathName};
            const auto inode = stat.INode;
            if (logRequest.GetNodeInfo().GetNodeId()) {
                NodesLogToLocal[logRequest.GetNodeInfo().GetNodeId()] = inode;
                NodePath[inode] = relativePathName;
            }
            STORAGE_DEBUG(
                "Open %d <- %lu inode=%lu known handles=%zu opened=%zu "
                "size=%lu",
                fh,
                logRequest.GetNodeInfo().GetHandle(),
                inode,
                HandlesLogToActual.size(),
                OpenHandles.size(),
                stat.Size);
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
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        if (Spec.GetSkipRead()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_READ,
                Started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        TGuard<TMutex> guard(StateLock);

        const auto handle = HandleIdMapped(logRequest.GetRanges(0).GetHandle());
        if (handle == InvalidHandle) {
            STORAGE_WARN(
                "Read: no handle %lu ranges size=%d map size=%zu",
                logRequest.GetRanges(0).GetHandle(),
                logRequest.GetRanges().size(),
                HandlesLogToActual.size());
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_READ,
                Started,
                MakeError(E_FAIL, "cancelled")});
        }
        auto& fh = OpenHandles[handle];
        STORAGE_DEBUG(
            "Read from %lu fh.len=%ld fh.pos=%ld ",
            handle,
            fh.GetLength(),
            fh.GetPosition());
        auto buffer = Acalloc(logRequest.GetRanges().cbegin()->GetBytes());

        /*
                if (!fh.GetLength()) {
                    // incorrect aligned to read size, should use size from
           nodeattr fh.Reserve( logRequest.GetRanges().cbegin()->GetOffset() +
                        logRequest.GetRanges().cbegin()->GetBytes());
                }
        */

        TFileHandle fileHandle{fh.GetHandle()};

        const auto future = AsyncIO.Read(
            fileHandle,
            {},
            logRequest.GetRanges().cbegin()->GetBytes(),
            logRequest.GetRanges().cbegin()->GetOffset());
        fileHandle.Release();

        return future.Apply(
            [started = Started]([[maybe_unused]] const auto& future) mutable
            {
                if (future.GetValue()) {
                    return TCompletedRequest(NProto::ACTION_READ, started, {});
                }
                return TCompletedRequest(
                    NProto::ACTION_READ,
                    started,
                    MakeError(E_IO, "nothing read"));
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
        override
    {
        //{"TimestampMcs":1465489895000,"DurationMcs":2790,"RequestType":44,"Ranges":[{"NodeId":2,"Handle":20680158862113389,"Offset":13,"Bytes":12}]}

        if (Spec.GetSkipWrite()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_WRITE,
                Started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }
        TGuard<TMutex> guard(StateLock);

        const auto handleLog = logRequest.GetRanges(0).GetHandle();
        const auto handleLocal = HandleIdMapped(handleLog);
        if (handleLocal == InvalidHandle) {
            // TODO(proller): Suggest filename, place in __lost__ if unknown,
            // create and open file, continue to write
            return MakeFuture(TCompletedRequest(
                NProto::ACTION_WRITE,
                Started,
                MakeError(
                    E_CANCELLED,
                    TStringBuilder{} << "write cancelled: no handle ="
                                     << handleLog)));
        }
        const auto bytes = logRequest.GetRanges(0).GetBytes();
        const auto offset = logRequest.GetRanges(0).GetOffset();

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

        auto& fh = OpenHandles[handleLocal];

        STORAGE_DEBUG(
            "Write to %lu fh.length=%ld fh.pos=%ld",
            handleLocal,
            fh.GetLength(),
            fh.GetPosition());
        // TODO(proller): TEST USE AFTER FREE on buffer
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
                    MakeError(E_IO, TStringBuilder{} << "nothing written"));
            });
    }

    TFsPath PathByNode(TNodeLocal nodeid)
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
        override
    {
        // {"TimestampMcs":1725895166478218,"DurationMcs":6328,"RequestType":26,"ErrorCode":0,"NodeInfo":{"NewParentNodeId":1,"NewNodeName":"home","Mode":509,"NodeId":12526,"Size":0}}
        // nfs     CreateNode      0.006404s       S_OK {new_parent_node_id=1,
        // new_node_name=home, mode=509, node_id=12526, size=0}

        if (Spec.GetSkipWrite()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_CREATE_NODE,
                Started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        TGuard<TMutex> guard(StateLock);

        const auto parentNode = CreateDirIfMissingByNodeLog(
            logRequest.GetNodeInfo().GetNewParentNodeId());

        auto fullName =
            PathByNode(parentNode) / logRequest.GetNodeInfo().GetNewNodeName();
        ui64 nodeid = 0;
        switch (logRequest.GetNodeInfo().GetType()) {
            case NProto::E_REGULAR_NODE: {
                // TODO(proller): transform r.GetNodeInfo().GetMode() to correct
                // open mode
                TFileHandle fh(fullName, OpenAlways | RdWr);
                if (fh) {
                    nodeid = TFileStat{fh}.INode;
                } else {
                    nodeid = TFileStat{fullName}.INode;
                }

                if (logRequest.GetNodeInfo().GetSize()) {
                    fh.Reserve(logRequest.GetNodeInfo().GetSize());
                }
                break;
            }
            case NProto::E_DIRECTORY_NODE: {
                nodeid = MakeDirectoryRecursive(fullName);
                break;
            }
            case NProto::E_LINK_NODE: {
                // {"TimestampMcs":1727703903595285,"DurationMcs":2432,"RequestType":26,"ErrorCode":0,"NodeInfo":{"NewParentNodeId":267,"NewNodeName":"pack-ebe666445578da0c6157f4172ad581cd731742ec.idx","Mode":292,"Type":3,"NodeId":274,"Size":245792}}

                const auto targetNode =
                    NodeIdMapped(logRequest.GetNodeInfo().GetNodeId());
                const auto targetFullName = PathByNode(targetNode);
                NFs::HardLink(targetFullName, fullName);
                break;
            }
            case NProto::E_SYMLINK_NODE: {
                const auto targetNode =
                    NodeIdMapped(logRequest.GetNodeInfo().GetNodeId());
                const auto targetFullName = PathByNode(targetNode);
                NFs::SymLink(targetFullName, fullName);
                break;
            }
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
            NodePath[nodeid] = PathByNode(parentNode) /
                               logRequest.GetNodeInfo().GetNewNodeName();
        }

        return MakeFuture(
            TCompletedRequest(NProto::ACTION_CREATE_NODE, Started, {}));
    }
    TFuture<TCompletedRequest> DoRenameNode(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        // {"TimestampMcs":895166000,"DurationMcs":2949,"RequestType":28,"NodeInfo":{"ParentNodeId":3,"NodeName":"HEAD.lock","NewParentNodeId":3,"NewNodeName":"HEAD"}}
        // nfs     RenameNode      0.002569s       S_OK {parent_node_id=12527,
        // node_name=HEAD.lock, new_parent_node_id=12527, new_node_name=HEAD}
        // request->SetNodeId(NodesLogToActual[r.GetNodeInfo().GetNodeId()]);
        if (Spec.GetSkipWrite()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_RENAME_NODE,
                Started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        TGuard<TMutex> guard(StateLock);

        const auto parentNodeId =
            NodeIdMapped(logRequest.GetNodeInfo().GetParentNodeId());

        auto fullName =
            PathByNode(parentNodeId) / logRequest.GetNodeInfo().GetNodeName();

        const auto newParentNodeId =
            NodeIdMapped(logRequest.GetNodeInfo().GetNewParentNodeId());

        auto newFullName = PathByNode(newParentNodeId) /
                           logRequest.GetNodeInfo().GetNewNodeName();

        const auto renameres = NFs::Rename(fullName, newFullName);

        if (renameres) {
            STORAGE_DEBUG(
                "Rename fail %s => %s : %d errno=%d err=%s",
                fullName.c_str(),
                newFullName.c_str(),
                renameres,
                LastSystemError(),
                LastSystemErrorText(LastSystemError()));
        } else {
            STORAGE_DEBUG(
                "Renamed %s => %s ",
                fullName.c_str(),
                newFullName.c_str());
        }
        return MakeFuture(
            TCompletedRequest{NProto::ACTION_RENAME_NODE, Started, {}});
    }

    TFuture<TCompletedRequest> DoUnlinkNode(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        // UnlinkNode      0.002605s       S_OK    {parent_node_id=3,
        // node_name=tfrgYZ1}

        if (Spec.GetSkipWrite()) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_REMOVE_NODE,
                Started,
                MakeError(E_PRECONDITION_FAILED, "disabled")});
        }

        TGuard<TMutex> guard(StateLock);

        const auto parentNodeId =
            NodeIdMapped(logRequest.GetNodeInfo().GetParentNodeId());
        if (parentNodeId == InvalidNodeId) {
            STORAGE_WARN(
                "Unlink : no parent orig=%lu",
                logRequest.GetNodeInfo().GetParentNodeId());
            return MakeFuture(TCompletedRequest(
                NProto::ACTION_REMOVE_NODE,
                Started,
                MakeError(E_CANCELLED, "cancelled")));
        }
        const auto fullName =
            PathByNode(parentNodeId) / logRequest.GetNodeInfo().GetNodeName();
        const auto unlinkres = NFs::Remove(fullName);
        STORAGE_DEBUG("Unlink %s : %d ", fullName.c_str(), unlinkres);
        // TODO(proller):
        // NodesLogToActual.erase(...)
        // NodePath.erase(...)
        return MakeFuture(
            TCompletedRequest(NProto::ACTION_REMOVE_NODE, Started, {}));
    }

    TFuture<TCompletedRequest> DoDestroyHandle(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
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
            "Close %lu <- %lu pos=%lu len=%ld open map size=%ld map size=%zu",
            handleid,
            logRequest.GetNodeInfo().GetHandle(),
            pos,
            len,
            OpenHandles.size(),
            HandlesLogToActual.size());
        return MakeFuture(
            TCompletedRequest(NProto::ACTION_DESTROY_HANDLE, Started, {}));
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

        TGuard<TMutex> guard(StateLock);

        // TODO(proller): by ParentNodeId + NodeName
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

        // TODO(proller): can create and truncate to size here missing files

        const auto nodeid = NodeIdMapped(logRequest.GetNodeInfo().GetNodeId());

        if (nodeid == InvalidNodeId) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_GET_NODE_ATTR,
                Started,
                MakeError(
                    E_NOT_FOUND,
                    TStringBuilder{} << "Node not found "
                                     << logRequest.GetNodeInfo().GetNodeId()
                                     << " in " << NodesLogToLocal.size())});
        }

        auto fullname = PathByNode(nodeid);
        [[maybe_unused]] const auto stat = TFileStat{fullname};
        return MakeFuture(
            TCompletedRequest(NProto::ACTION_GET_NODE_ATTR, Started, {}));
    }

    TFuture<TCompletedRequest> DoAcquireLock(
        const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& /*logRequest*/) override
    {
        return MakeFuture(TCompletedRequest{
            NProto::ACTION_RELEASE_LOCK,
            Started,
            MakeError(E_NOT_IMPLEMENTED, "invalid not implemented")});
    }

    TFuture<TCompletedRequest> DoReleaseLock(
        const NCloud::NFileStore::NProto::
            TProfileLogRequestInfo& /*logRequest*/) override
    {
        return MakeFuture(TCompletedRequest{
            NProto::ACTION_RELEASE_LOCK,
            Started,
            MakeError(E_NOT_IMPLEMENTED, "invalid not implemented")});
    }

    TFuture<TCompletedRequest> DoListNodes(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& logRequest)
        override
    {
        // json={"TimestampMcs":1726615510721016,"DurationMcs":3329,"RequestType":30,"ErrorCode":0,"NodeInfo":{"NodeId":164,"Size":10}}

        TGuard<TMutex> guard(StateLock);

        const auto nodeid = NodeIdMapped(logRequest.GetNodeInfo().GetNodeId());
        if (nodeid == InvalidNodeId) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_LIST_NODES,
                Started,
                MakeError(
                    E_CANCELLED,
                    TStringBuilder{} << "Node not found in mapping"
                                     << nodeid)});
        }

        if (!Spec.GetSkipWrite()) {
            CreateDirIfMissingByNodeLog(logRequest.GetNodeInfo().GetNodeId());
        }

        const auto path = PathByNode(nodeid);
        if (NFs::Exists(path)) {
            return MakeFuture(TCompletedRequest{
                NProto::ACTION_LIST_NODES,
                Started,
                MakeError(
                    E_NOT_FOUND,
                    TStringBuilder{} << "Local dir not found " << path)});
        }
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
                "Dir size differs %s log=%lu local=%zu",
                path.c_str(),
                logRequest.GetNodeInfo().GetSize(),
                dirs.size());
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
