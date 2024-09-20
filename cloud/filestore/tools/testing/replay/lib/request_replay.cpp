/*

TODO:
create file/dir modes
create handle modes (now rw)
compare log and actual result ( S_OK E_FS_NOENT ...)





*/

#include "request.h"

#include "cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h"
#include "cloud/filestore/tools/analytics/libs/event-log/dump.h"
// #include "dump.h"
#include "library/cpp/aio/aio.h"
#include "library/cpp/eventlog/iterator.h"
#include "library/cpp/testing/unittest/registar.h"
#include "util/folder/dirut.h"
#include "util/folder/path.h"
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

namespace NCloud::NFileStore::NReplay {

using namespace NThreading;
using namespace NCloud::NFileStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

const ui64 SEGMENT_SIZE = 128;

struct TSegment
{
    ui64 Handle = 0;
    ui64 LastWriteRequestId = 0;
};

static_assert(sizeof(TSegment) <= SEGMENT_SIZE);
/*
bool Compare(const TSegment& expected, const TSegment& actual, TString* message)
{
    TStringBuilder sb;

    if (expected.Handle != actual.Handle) {
        sb << "expected.Handle != actual.Handle: " << expected.Handle
           << " != " << actual.Handle;
    }

    if (expected.LastWriteRequestId != actual.LastWriteRequestId) {
        if (sb.Size()) {
            sb << ", ";
        }

        sb << "expected.LastWriteRequestId != actual.LastWriteRequestId: "
           << expected.LastWriteRequestId
           << " != " << actual.LastWriteRequestId;
    }

    *message = sb;

    return sb.Empty();
}
*/
struct TSegments
    : TVector<TSegment>
    , TAtomicRefCount<TSegments>
{
};

using TSegmentsPtr = TIntrusivePtr<TSegments>;

////////////////////////////////////////////////////////////////////////////////

struct THandleInfo
{
    TString Name;
    ui64 Handle = 0;
    ui64 Size = 0;
    TSegmentsPtr Segments;
    ui64 LastSlot = 0;

    THandleInfo() = default;

    THandleInfo(
        TString name,
        ui64 handle,
        ui64 size,
        TSegmentsPtr segments) noexcept
        : Name(std::move(name))
        , Handle(handle)
        , Size(size)
        , Segments(std::move(segments))
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TReplayRequestGenerator final
    : public IRequestGenerator
    , public std::enable_shared_from_this<TReplayRequestGenerator>
{
private:
    // static constexpr ui32 DefaultBlockSize = 4_KB;

    const NProto::TReplaySpec Spec;
    // const
    TString FileSystemId;
    const NProto::THeaders Headers;

    TLog Log;

    ISessionPtr Session;

    TVector<std::pair<ui64, NProto::EAction>> Actions;
    // ui64 TotalRate = 0;

    TMutex StateLock;
    ui64 LastWriteRequestId = 0;

    using THandleLog = ui64;
    using THandleLocal = ui64;
    using TNodeLog = ui64;
    using TNodeLocal = ui64;

    TVector<THandleInfo> IncompleteHandleInfos;
    TVector<THandleInfo> HandleInfos;
    THashMap<ui64, THandleInfo> HandleInfosh;
    THashMap<THandleLog, THandleLocal> HandlesLogToActual;

    // ui64 ReadBytes = DefaultBlockSize;
    //  ui64 WriteBytes = DefaultBlockSize;
    //  double AppendProbability = 1;
    //   ui64 BlockSize = DefaultBlockSize;
    ui64 InitialFileSize = 0;

    std::atomic<ui64> LastRequestId = 0;

    // NEventLog::IIterator *
    THolder<NEventLog::IIterator> EventlogIterator;
    TConstEventPtr EventPtr;
    int EventMessageNumber = 0;
    NProto::TProfileLogRecord* messagep{};
    // TString FileSystemId;

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

    THashMap<TString, TNode> StagedNodes;
    THashMap<TString, TNode> Nodes;
    // THashMap<ui64, TNode> NodesLogToActual;
    THashMap<TNodeLog, TNodeLocal> NodesLogToLocal{{RootNodeId, RootNodeId}};
    THashMap<TNodeLocal, TString> NodePath{{RootNodeId, "/"}};

    //THashMap<ui64, TFileHandle> FileHandles;

    THashMap<ui64, THandle> Handles;

    THashSet<ui64> Locks;
    THashSet<ui64> StagedLocks;

    NAsyncIO::TAsyncIOService AsyncIO;
    // TString ReplayRoot;
    ui64 TimestampMcs{};
    // ui64 DurationMcs{};
    TInstant Started;
    // THashMap<TString, TFileHandle> OpenFiles;
    // THashMap<THandleLocal, TFileHandle> OpenHandles;
    THashMap<THandleLocal, TFile> OpenHandles;
    THashMap<TNodeLog, TNode> KnownLogNodes;

public:
    TReplayRequestGenerator(
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

        // DUMP(spec);

        // ReplayRoot = spec.GetReplayRoot();

        AsyncIO.Start();

        /*
                if (auto size = Spec.GetBlockSize()) {
                    BlockSize = size;
                    Y_ENSURE(BlockSize % DefaultBlockSize == 0);
                }
                if (auto bytes = Spec.GetReadBytes()) {
                    ReadBytes = bytes;
                }
                if (auto bytes = Spec.GetWriteBytes()) {
                    WriteBytes = bytes;
                }
                if (auto p = Spec.GetAppendPercentage()) {
                    AppendProbability = p / 100.;
                }

                InitialFileSize = Spec.GetInitialFileSize();

                if (Spec.GetValidationEnabled()) {
                    Y_ENSURE(
                        InitialFileSize % SEGMENT_SIZE == 0,
                        Sprintf(
                            "InitialFileSize (%lu) %% SEGMENT_SIZE (%lu) != 0",
                            InitialFileSize,
                            SEGMENT_SIZE
                        )
                    );

                    Y_ENSURE(
                        WriteBytes % SEGMENT_SIZE == 0,
                        Sprintf(
                            "WriteBytes (%lu) %% SEGMENT_SIZE (%lu) != 0",
                            WriteBytes,
                            SEGMENT_SIZE
                        )
                    );

                    Y_ENSURE(
                        ReadBytes % SEGMENT_SIZE == 0,
                        Sprintf(
                            "ReadBytes (%lu) %% SEGMENT_SIZE (%lu) != 0",
                            ReadBytes,
                            SEGMENT_SIZE
                        )
                    );
                }
        */

        /*
                for (const auto& action: Spec.GetActions()) {
                    Y_ENSURE(action.GetRate() > 0, "please specify positive
           action rate");

                    TotalRate += action.GetRate();
                    Actions.emplace_back(std::make_pair(TotalRate,
           action.GetAction()));
                }

                Y_ENSURE(!Actions.empty(), "please specify at least one action
           for the test spec");
        */

        NEventLog::TOptions options;
        // DUMP(Spec);
        // Cerr << "SPEC=" << Spec;
        options.FileName = Spec.GetFileName();
        options.SetForceStrongOrdering(true);
        // options.ForceStreamMode
        //  THolder<NEventLog::IIterator> it = CreateIterator(options, fac);
        //  DUMP(options.FileName);
        //  THolder<NEventLog::IIterator> it
        EventlogIterator = CreateIterator(options);
        // EventPtr = EventlogIterator->Next();
        //  DUMP("opened");
        //  NEventLog::IIterator* i
        //  EventlogIterator = it.Get();
        //  DUMP((long)i);
        if (!Spec.GetReplayRoot().empty()) {
            // auto fspath =
            TFsPath(Spec.GetReplayRoot()).MkDirs();
        }
    }

    ~TReplayRequestGenerator()
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

        STORAGE_INFO(
            "node not found " << id << " map size=" << NodesLogToLocal.size());
        // DUMP("but known=", KnownLogNodes.contains(id));
        return 0;
    }

    THandleLocal HandleIdMapped(const THandleLog id)
    {
        if (const auto it = HandlesLogToActual.find(id);
            it != HandlesLogToActual.end())
        {
            return it->second;
        }
        STORAGE_INFO(
            "handle not found " << id
                                << " map size=" << HandlesLogToActual.size());
        return 0;
    }

    bool UseFs()
    {
        return !Spec.GetReplayRoot().empty();
    }

    void Advance()
    {
        EventPtr = EventlogIterator->Next();
        // DUMP(!!EventPtr, (long)EventPtr.Get(), EventMessageNumber);
        if (!EventPtr) {
            return;
        }
        // if (EventPtr) {
        // DUMP(EventPtr->GetName(),EventPtr->Class,EventPtr->FrameId);
        //,EventPtr->ToString()

        // const NProto::TProfileLogRecord*
        messagep = const_cast<NProto::TProfileLogRecord*>(
            dynamic_cast<const NProto::TProfileLogRecord*>(
                EventPtr->GetProto()));
        if (!messagep) {
            // DUMP("nomesssss");
            return;
        }

        // DUMP(messagep->GetClassData(), messagep->GetFileSystemId());
        if (FileSystemId.empty()) {
            FileSystemId = TString{messagep->GetFileSystemId()};
        }

        EventMessageNumber = messagep->GetRequests().size();
    }

    bool HasNextRequest() override
    {
        // DUMP("hasnext?", !!EventPtr);
        if (!EventPtr) {
            // EventPtr = EventlogIterator->Next();
            Advance();
        }
        // DUMP("hasnext=", !!EventPtr);
        return !!EventPtr;
    }

    /*    TInstant NextRequestAt() override
        {
            return TInstant::Max();
        }
    */

    NThreading::TFuture<TCompletedRequest> ExecuteNextRequest() override
    {
        // DUMP("en");
        //  auto *ev = EventlogIterator.Get();
        //  EventPtr = EventlogIterator.Get();
#if 0
        if (0) {
            while (auto ev = EventlogIterator->Next()) {
                // auto ev = EventlogIterator->Next();

                DUMP(ev->GetName(), ev->ToString());

                // event->Class;

                const auto* message =
                    dynamic_cast<const NProto::TProfileLogRecord*>(
                        ev->GetProto());
                if (!message) {
                    DUMP("nomess");
                    continue;
                }
                // const auto *message = ev->GetProto();
                // DUMP(message->AsJSON());
                // DUMP(message->GetTypeName());
                for (const auto& r: message->GetRequests()) {
                    DUMP(r.GetTypeName(), r.GetBlobsInfo(), r.GetRanges());
                    DUMP(RequestName(r.GetRequestType()));
                    // TBasicString<char, std::char_traits<char> >&& () noexcept
                    // RequestName(r.GetRequestType()) = "GetNodeAttr";
                    // {"TimestampMcs":1725548818281695,"DurationMcs":128,"RequestType":33,"ErrorCode":0,"NodeInfo":{"ParentNodeId":2395,"NodeName":"pre-receive.sample","Flags":0,"Mode":509,"NodeId":2402,"Handle":0,"Size":544}}
                    // DUMP(r.AsJSON());
                    // Cerr << r.AsJSON() << "\n";
                    DUMP(r.GetMetadata());
                }
            }
        }
#endif
        {
            if (!HasNextRequest()) {
                // DUMP("noreq");
                return MakeFuture(TCompletedRequest{});
            };
            // if (!EventPtr) {EventPtr = EventlogIterator->Next();}

            //            NProto::TProfileLogRequestInfo request;

            // DUMP(!!EventPtr, (long)EventPtr.Get(), EventMessageNumber);
            //  do {
            for (; EventPtr;

                 // EventMessageNumber = 0,
                 // EventPtr = EventlogIterator->Next()
                 Advance())
            {
                /*
                                if (!EventPtr) {
                                    continue;
                                }
                */
                // DUMP(!!EventPtr, (long)EventPtr.Get(), EventMessageNumber);
                //  if (EventPtr) {
                /*
                                const NProto::TProfileLogRecord* messagep =
                                    dynamic_cast<const
                   NProto::TProfileLogRecord*>( EventPtr->GetProto());
                                        */
                if (!messagep) {
                    // DUMP("nomess");
                    continue;
                }

                /* const auto i = message->GetRequests().begin();
                            for (const auto& r: message->GetRequests()) {
                            r.GetRequestType();
                            }
                            */
                STORAGE_DEBUG("Processing %d", EventMessageNumber);
                // DUMP(EventMessageNumber, messagep->GetRequests().size());
                /*
                                if (EventMessageNumber >=
                   messagep->GetRequests().size()) { DUMP("nonextreq");
                                    continue;
                                }
                                for (; EventMessageNumber <
                   messagep->GetRequests().size();) { auto request =
                   messagep->GetRequests()[EventMessageNumber++];
                                */
                for (; EventMessageNumber > 0;) {
                    auto request =
                        messagep->GetRequests()[--EventMessageNumber];
                    // TODO check ranges
                    // DUMP(request.AsJSON());
                    // Cerr << request.AsJSON() << "\n";
                    STORAGE_DEBUG("message json=" << request.AsJSON());
                    // "TimestampMcs":1725981613498599,"DurationMcs":2101,
                    // TODO WTF
                    // 2024-09-10T14:52:07.145177Z     nfs     ListNodes
                    // 0.000385s       S_OK    {node_id=1, size=0}
                    // 2024-09-10T14:52:07.146009Z     nfs     ListNodes
                    // 0.000128s       S_OK    {node_id=1, size=0}
                    // 1970-01-01T00:14:52.767000Z     nfs     PingSession
                    // 0.000328s       S_OK    {no_info}
                    // 1970-01-01T00:14:53.768000Z     nfs     PingSession
                    // 0.000279s       S_OK    {no_info}
                    auto timediff = (request.GetTimestampMcs() - TimestampMcs) *
                                    Spec.GetTimeScale();
                    // DUMP(request.GetTimestampMcs(),TimestampMcs,timediff);
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

                        Sleep(slp);   // TODO: calculate correct timescale here
                    }
                    // DUMP(current, Started, diff.MicroSeconds());
                    Started = current;

                    //                      DurationMcs =
                    //                      request.GetDurationMcs();

                    STORAGE_DEBUG(
                        "Processing message "
                        << EventMessageNumber
                        << " typename=" << request.GetTypeName()
                        << " type=" << request.GetRequestType() << " "
                        << RequestName(request.GetRequestType()));

                    {
                        const auto& action = request.GetRequestType();
                        switch (static_cast<EFileStoreRequest>(action)) {
                            case EFileStoreRequest::
                                ReadData:   // NProto::ACTION_READ:
                                return DoReadData(request);
                            case EFileStoreRequest::
                                WriteData:   // NProto::ACTION_WRITE:
                                return DoWrite(request);
                                // static_cast<EFileStoreRequest>(requestType)
                            case EFileStoreRequest::
                                CreateNode:   //  NProto::ACTION_CREATE_NODE:
                                return DoCreateNode(request);
                            case EFileStoreRequest::
                                RenameNode:   // NProto::ACTION_RENAME_NODE:
                                return DoRenameNode(request);
                            case EFileStoreRequest::
                                UnlinkNode:   // NProto::ACTION_REMOVE_NODE:
                                return DoUnlinkNode(request);
                            case EFileStoreRequest::
                                CreateHandle:   // NProto::ACTION_CREATE_HANDLE:
                                return DoCreateHandle(request);
                            case EFileStoreRequest::
                                DestroyHandle:   //  NProto::ACTION_DESTROY_HANDLE:
                                return DoDestroyHandle(request);
                            case EFileStoreRequest::
                                GetNodeAttr:   // NProto::ACTION_GET_NODE_ATTR:
                                return DoGetNodeAttr(request);
                            case EFileStoreRequest::
                                AcquireLock:   // NProto::ACTION_ACQUIRE_LOCK:
                                return DoAcquireLock();
                            case EFileStoreRequest::
                                ReleaseLock:   // NProto::ACTION_RELEASE_LOCK:
                                return DoReleaseLock();

                            case EFileStoreRequest::
                                AccessNode:   // NProto::ACTION_CREATE_HANDLE:
                                return DoAccessNode(request);

                            case EFileStoreRequest::ReadBlob:
                            case EFileStoreRequest::WriteBlob:
                            case EFileStoreRequest::GenerateBlobIds:
                            case EFileStoreRequest::PingSession:
                            case EFileStoreRequest::Ping:

                                continue;

                                /* TODO:
                                2024-09-19T23:54:21.789965Z :smoke DEBUG:
                                cloud/filestore/tools/testing/replay/lib/request_replay.cpp:475:
                                message
                                json={"TimestampMcs":1726503858215279,"DurationMcs":20865,"RequestType":10001,"ErrorCode":0,"BlobsInfo":[{"CommitId":4295169028,"Unique":1037938976620549,"Ranges":[{"NodeId":16064,"Offset":1179648,"Bytes":81920},{"NodeId":16065,"Offset":1179648,"Bytes":81920},{"NodeId":16066,"Offset":1179648,"Bytes":77824}]},{"CommitId":4295169028,"Unique":1636073302130950,"Ranges":[{"NodeId":16050,"Offset":0,"Bytes":262144},{"NodeId":16051,"Offset":0,"Bytes":16384},{"NodeId":16052,"Offset":0,"Bytes":49152},{"NodeId":16060,"Offset":0,"Bytes":20480},{"NodeId":16061,"Offset":0,"Bytes":32768}]},{"CommitId":4295169028,"Unique":1125899906843139,"Ranges":[{"NodeId":16050,"Offset":262144,"Bytes":262144}]},{"CommitId":4295169028,"Unique":211106232533764,"Ranges":[{"NodeId":16040,"Offset":0,"Bytes":49152}]},{"CommitId":4295169028,"Unique":1125899906843653,"Ranges":[{"NodeId":16050,"Offset":524288,"Bytes":262144}]},{"CommitId":4295169028,"Unique":1125899906843910,"Ranges":[{"NodeId":16050,"Offset":786432,"Bytes":262144}]},{"CommitId":4295169028,"Unique":1952732650931715,"Ranges":[{"NodeId":16050,"Offset":1048576,"Bytes":262144},{"NodeId":16062,"Offset":1179648,"Bytes":114688},{"NodeId":16063,"Offset":1179648,"Bytes":77824}]},{"CommitId":4295169028,"Unique":1125899906844420,"Ranges":[{"NodeId":16050,"Offset":1310720,"Bytes":262144}]},{"CommitId":4295169028,"Unique":1125899906844677,"Ranges":[{"NodeId":16050,"Offset":1572864,"Bytes":262144}]},{"CommitId":4295169028,"Unique":52776558135558,"Ranges":[{"NodeId":16041,"Offset":1822720,"Bytes":12288}]},{"CommitId":4295169028,"Unique":1125899906845187,"Ranges":[{"NodeId":16050,"Offset":1835008,"Bytes":262144}]},{"CommitId":4295169028,"Unique":1125899906845444,"Ranges":[{"NodeId":16041,"Offset":1835008,"Bytes":262144}]},{"CommitId":4295169028,"Unique":1125899906845701,"Ranges":[{"NodeId":16050,"Offset":2097152,"Bytes":262144}]},{"CommitId":4295169028,"Unique":1125899906845958,"Ranges":[{"NodeId":16041,"Offset":2097152,"Bytes":262144}]},{"CommitId":4295169028,"Unique":1125899906846211,"Ranges":[{"NodeId":16050,"Offset":2359296,"Bytes":262144}]},{"CommitId":4295169028,"Unique":439804651114244,"Ranges":[{"NodeId":16041,"Offset":2359296,"Bytes":102400}]},{"CommitId":4295169028,"Unique":1125899906846725,"Ranges":[{"NodeId":16050,"Offset":2621440,"Bytes":262144}]},{"CommitId":4295169028,"Unique":175921860448518,"Ranges":[{"NodeId":16050,"Offset":2883584,"Bytes":40960}]},{"CommitId":4295169028,"Unique":123145302315523,"Ranges":[{"NodeId":16067,"Offset":0,"Bytes":28672}]}]}
                                2024-09-19T23:54:21.789970Z :smoke DEBUG:
                                cloud/filestore/tools/testing/replay/lib/request_replay.cpp:517:
                                Processing message 4294
                                typename=NCloud.NFileStore.NProto.TProfileLogRequestInfo
                                type=10001 Flush
                                */

                                // listnodes ->. noeid -> createdir

                                // 9: CreateSession

                            default:
                                STORAGE_INFO(
                                    "Uninmplemented action="
                                    << action << " "
                                    << RequestName(request.GetRequestType()));
                                // Y_ABORT("unexpected action: %u",
                                // (ui32)action);
                                continue;
                        }
                    }
                }

                // break;

                //    message.get
            }
            STORAGE_DEBUG(
                "Finished n=" << EventMessageNumber << " ptr=" << !!EventPtr);
            //} while (EventMessageNumber = 0,                     EventPtr =
            // EventlogIterator->Next());

            /*
                        // const auto& action = PeekNextAction();
                        */
            /*
                        return MakeFuture(TCompletedRequest{
                            NProto::EAction::ACTION_READ,
                            TInstant::Now(),
                            NProto::TError{}});
                        // return DoRead();
            */

            return MakeFuture(TCompletedRequest(true));
        }
    }

private:
    /*
        NProto::EAction PeekNextAction()
        {
            auto number = RandomNumber(TotalRate);
            auto it = LowerBound(
                Actions.begin(),
                Actions.end(),
                number,
                [](const auto& pair, ui64 b) { return pair.first < b; });

            Y_ABORT_UNLESS(it != Actions.end());
            return it->second;
        }
    */

    TFuture<TCompletedRequest> DoAccessNode(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& r)
    {
        if (Spec.GetNoRead()) {
            return {};
        }

        // nfs     AccessNode      0.002297s       S_OK    {mask=4, node_id=36}
        TGuard<TMutex> guard(StateLock);
        auto started = TInstant::Now();
        if (UseFs()) {
            const auto node = NodeIdMapped(r.GetNodeInfo().GetNodeId());

            if (!node) {
                STORAGE_ERROR(
                    "access fail: " << " no node="
                                    << r.GetNodeInfo().GetNodeId());
                return MakeFuture(TCompletedRequest{
                    NProto::ACTION_ACCESS_NODE,
                    started,
                    MakeError(E_FAIL, "cancelled")});
            }

            auto fname = Spec.GetReplayRoot() + "/" + PathByNode(node);
            int res = access(fname.c_str(), R_OK);
            STORAGE_DEBUG(
                "access " << node << " <- " << r.GetNodeInfo().GetNodeId()
                          << " = " << res);
            return MakeFuture(
                TCompletedRequest{NProto::ACTION_ACCESS_NODE, started, {}});
        }
        return {};
    }

    // Recursive, no infinity loop check
    TNodeLocal CreateDirIfMissingByNodeLog(TNodeLog nodeIdLog)
    {
        if (const auto& nodeIdLocal = NodeIdMapped(nodeIdLog)) {
            // DUMP("Already created", nodeIdLog, "->", nodeIdLocal);
            return nodeIdLocal;
        }

        // DUMP(nodeIdLog);

        const auto& it = KnownLogNodes.find(nodeIdLog);
        if (it == KnownLogNodes.end()) {
            // DUMP("not known", NodePath, "but", NodeIdMapped(nodeIdLog));
            //   maybe create in tmpdir
            return 0;
        }
        /*
                DUMP(
                    "WANACREEEEE",
                    nodeIdLog,
                    it->second.Name,
                    it->second.ParentLog,
                    NodeIdMapped(it->second.ParentLog));
        */
        auto parent = NodeIdMapped(it->second.ParentLog);

        if (!parent && it->second.ParentLog &&
            nodeIdLog != it->second.ParentLog)
        {
            // NodeIdMapped()
            parent = CreateDirIfMissingByNodeLog(it->second.ParentLog);
            // DUMP("create miss parent", it->second.ParentLog, " => ", parent);
        }

        // if (!parent)
        {
            auto parentPath = PathByNode(NodeIdMapped(it->second.ParentLog));
            /*
                        DUMP(
                            "try1",
                            parentPath,
                            parent,
                            it->second.ParentLog,
                            parentPath,
                            NodeIdMapped(it->second.ParentLog));
            */
            // KnownLogNodes.contains()
            if (parentPath.empty() && parent) {
                parentPath = PathByNode(parent);
                // DUMP("try2", parent, parentPath);
            }
            if (parentPath.empty()) {
                parentPath = "/__lost__/";
            }
            // DUMP("tryR", parent, it->second.ParentLog, parentPath);

            // const auto name = (parent ? PathByNode(parent) : "__lost__/") +
            // ToString(logNodeId) + "/"; const auto name = parentPath +
            // ToString(logNodeId) + "/";
            const auto name = parentPath +
                              (it->second.Name.empty() ? ToString(nodeIdLog)
                                                       : it->second.Name) +
                              "/";
            /*
                        DUMP(
                            "namegen",
                            parent,
                            PathByNode(parent),
                            it->second.Parent,
                            PathByNode(it->second.Parent),
                            parentPath,
                            logNodeId);
            */
            const auto nodeId = Mkdir(Spec.GetReplayRoot() + name);
            NodePath[nodeId] = name;
            // DUMP("savepath", nodeId, name);
            NodesLogToLocal[nodeIdLog] = nodeId;

            // DUMP("created lost", nodeid, name);
            return nodeId;
        }
        /*
                const auto name = PathByNode(parent) + "/" + it->second.Name;
                const auto nodeid = Mkdir(Spec.GetReplayRoot() + "/" + name);
                NodePath[nodeid] = name;

                return nodeid;
                */
    }

    TFuture<TCompletedRequest> DoCreateHandle(
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& r)
    {
        // message
        // json={"TimestampMcs":1726503808715698,"DurationMcs":2622,"RequestType":38,"ErrorCode":0,"NodeInfo":{"ParentNodeId":13882,"NodeName":"compile_commands.json.tmpdf020","Flags":38,"Mode":436,"NodeId":15553,"Handle":46923415058768564,"Size":0}}
        TGuard<TMutex> guard(StateLock);
        auto started = TInstant::Now();
        if (UseFs()) {
            /// DUMP("hhhhhhh================================================");
            TString relativePathName;
            if (r.GetNodeInfo().GetNodeId()) {
                if (auto path = PathByNode(r.GetNodeInfo().GetNodeId())) {
                    relativePathName = path;
                }
                // DUMP("open by nodeid", relativePathName);
                //  TODO: maybe open by fhandle
            }

            if (relativePathName.empty()) {
                auto parentNode =
                    NodeIdMapped(r.GetNodeInfo().GetParentNodeId());
                /*
                                DUMP(
                                    "mapped",
                                    parentNode,
                                    r.GetNodeInfo().GetParentNodeId(),
                                    PathByNode(parentNode));
                */
                if (!parentNode) {
                    parentNode = NodeIdMapped(
                        KnownLogNodes[r.GetNodeInfo().GetParentNodeId()]
                            .ParentLog);
                    // DUMP("known", parentNode);
                }

                if (!parentNode && r.GetNodeInfo().GetParentNodeId() !=
                                       r.GetNodeInfo().GetNodeId())
                {
                    parentNode = CreateDirIfMissingByNodeLog(
                        r.GetNodeInfo().GetParentNodeId());
                    // DUMP("create", parentNode);
                }

                if (!parentNode) {
                    STORAGE_ERROR(
                        "create handle fail :"
                        << r.GetNodeInfo().GetHandle()
                        << " no parent=" << r.GetNodeInfo().GetParentNodeId());
                    // DUMP(
                    // "noparent",r.GetNodeInfo().GetParentNodeId(),r.GetNodeInfo().GetHandle());
                    //  DUMP(NodesLogToActual);
                    return MakeFuture(TCompletedRequest{
                        NProto::ACTION_CREATE_HANDLE,
                        started,
                        MakeError(E_FAIL, "cancelled")});
                }

                auto nodename = r.GetNodeInfo().GetNodeName();
                if (nodename.empty() && r.GetNodeInfo().GetNodeId() !=
                                            r.GetNodeInfo().GetParentNodeId())
                {
                    nodename = KnownLogNodes[r.GetNodeInfo().GetNodeId()].Name;
                    // DUMP("node name known", nodename);
                }
                auto parentpath = PathByNode(parentNode);

                if (nodename.empty() &&
                    IsDir(Spec.GetReplayRoot() + parentpath))
                {
                    nodename =
                        KnownLogNodes[r.GetNodeInfo().GetParentNodeId()].Name;
                }

                // DUMP("can add
                // name",KnownLogNodes[r.GetNodeInfo().GetParentNodeId()].Name);
                // DUMP(parentpath,nodename,NFs::Exists(Spec.GetReplayRoot() +
                // parentpath));
                relativePathName =
                    //"/" +
                    // PathByNode(parentNode) +
                    // r.GetNodeInfo().GetNewNodeName();
                    parentpath + nodename;
                /*
                                DUMP(
                                    "fname",
                                    relativePathName,
                                    parentNode,
                                    parentpath,
                                    PathByNode(parentNode),
                                    nodename);
                */
            }
            // PathByNode(r.GetNodeInfo().GetNodeId())+
            // r.GetNodeInfo().GetNewNodeName();
            STORAGE_DEBUG(
                "open "
                << relativePathName   // << " in " << parentNode << " <- "
                << " handle=" << r.GetNodeInfo().GetHandle()
                << " flags=" << r.GetNodeInfo().GetFlags()
                << " mode=" << r.GetNodeInfo().GetMode()
                << " node=" << r.GetNodeInfo().GetNodeId()

            )

                ;

            // ui64 id = 0;
            // DUMP("file", fname);

            /*
                        if (OpenFiles.contains(fname)) {
                            DUMP(
                                "already opened",
                                fname,
                                OpenFiles[fname],
                                r.GetNodeInfo().GetHandle());
                            // return MakeFuture(TCompletedRequest{});
                        }
            */
            // DUMP(NFs::Exists(Spec.GetReplayRoot() + relativePathName));
            //  TFileHandle
            TFile FileHandle(
                Spec.GetReplayRoot() + relativePathName,
                // static_cast<EOpenModeFlag>(r.GetNodeInfo().GetMode())
                OpenAlways | RdWr   // RdOnly

            );

            if (!FileHandle.IsOpen()) {
                TFile test{
                    Spec.GetReplayRoot() + relativePathName,
                    OpenAlways | RdWr};
                /*
                                DUMP(
                                    " seconddtest",
                                    test.IsOpen(),
                                    test.GetName(),
                                    test.GetLength(),
                                    test.GetPosition(),
                                    test.GetHandle());
                */
            }

            /*
                        DUMP(
                            FileHandle.IsOpen(),
                            FileHandle.GetPosition(),
                            FileHandle.GetLength());
            */

            if (!FileHandle.IsOpen()) {
                // DUMP("handle open fail", r.GetNodeInfo().GetHandle());
                return MakeFuture(TCompletedRequest{
                    NProto::ACTION_CREATE_HANDLE,
                    started,
                    MakeError(E_FAIL, "fail")});
            }
            // const auto fh = (FHANDLE)FileHandle;
            const auto fh = FileHandle.GetHandle();
            // DUMP(fh);
            if (!fh) {
                return MakeFuture(TCompletedRequest{
                    NProto::ACTION_CREATE_HANDLE,
                    started,
                    MakeError(E_FAIL, "no filehandle")});
            }

            // if (fh) {

            /*
                        auto fh = ::open(
                            fname.c_str(),
                            r.GetNodeInfo().GetFlags() | O_CREAT,
                            r.GetNodeInfo().GetMode());
                        DUMP(fh);
                        if (fh <= 0) {
                            DUMP("fail to create handle");
                            return MakeFuture(TCompletedRequest{});
                        }
            */
            // OpenFiles[fname] = fh;
            OpenHandles[fh] = std::move(FileHandle);
            //        TFileStat{};
            //          const auto id = (FHANDLE)(fh);
            // TFileStat stat{id};
            /*
                        TFileStat stat{fh};
                        // GetStatByHandle(stat, id);
                        auto id = stat.INode;

                        FileHandles[id] = std::move(fh);
            */
            // DUMP(stat.INode, stat.IsDir(), stat.IsFile());

            // DUMP(id, fh.GetLength(), fh.GetPosition());

            // CreateIfMissing(PathByNode())
            HandlesLogToActual[r.GetNodeInfo().GetHandle()] = fh;
            // DUMP("savehandle", r.GetNodeInfo().GetHandle(), fh);
            const auto inode =
                TFileStat{Spec.GetReplayRoot() + relativePathName}.INode;
            if (r.GetNodeInfo().GetNodeId()) {
                NodesLogToLocal[r.GetNodeInfo().GetNodeId()] = inode;

                // NodePath[id] = PathByNode(parentNode)
                // +r.GetNodeInfo().GetNewNodeName() +(isDir ? "/" : "");
                NodePath[inode] = relativePathName;
                /*
                                DUMP(
                                    "savepath2",
                                    r.GetNodeInfo().GetNodeId(),
                                    NodesLogToLocal[r.GetNodeInfo().GetNodeId()],
                                    inode,
                                    relativePathName);
                */
            }
            // NodePath[id] = PathByNode(parentNode)
            // +r.GetNodeInfo().GetNewNodeName() +(isDir ? "/" : "");
            // DUMP(NodePath[id], parentNode);
            STORAGE_DEBUG(
                "open " << fh << "<-" << r.GetNodeInfo().GetHandle()

                        << " known handles=" << HandlesLogToActual.size()
                        << " opened=" << OpenHandles.size()
                        << " inode=" << inode)

                ;

            //            DUMP(NodePath);
            // DUMP("open ok", HandlesLogToActual);
            // return {};
            return MakeFuture(
                TCompletedRequest{NProto::ACTION_CREATE_HANDLE, started, {}});
        }

        /*
                static const int flags =
                    ProtoFlag(NProto::TCreateHandleRequest::E_CREATE) |
                    ProtoFlag(NProto::TCreateHandleRequest::E_EXCLUSIVE) |
                    ProtoFlag(NProto::TCreateHandleRequest::E_READ) |
                    ProtoFlag(NProto::TCreateHandleRequest::E_WRITE);
        */
        /*
                auto name = GenerateNodeName();
        */
        auto request = CreateRequest<NProto::TCreateHandleRequest>();
        // {"TimestampMcs":1725895168384258,"DurationMcs":2561,"RequestType":38,"ErrorCode":0,"NodeInfo":{"ParentNodeId":12527,"NodeName":"index.lock","Flags":15,"Mode":436,"NodeId":12584,"Handle":65382484937735195,"Size":0}}
        // nfs     CreateHandle    0.004161s       S_OK    {parent_node_id=65,
        // node_name=ini, flags=14, mode=436, node_id=66,
        // handle=11024287581389312, size=0}
        /*
                request->SetNodeId(RootNodeId);
                request->SetName(name);
                request->SetFlags(flags);
        */
        auto name = r.GetNodeInfo().GetNodeName();

        const auto node = NodeIdMapped(r.GetNodeInfo().GetParentNodeId());
        if (!node) {
            // DUMP("nop", r.GetNodeInfo().GetParentNodeId());
            return MakeFuture(TCompletedRequest{});
        }
        request->SetNodeId(node);
        request->SetName(r.GetNodeInfo().GetNodeName());
        request->SetFlags(r.GetNodeInfo().GetFlags());
        request->SetMode(r.GetNodeInfo().GetMode());

        Cerr << "crrate=" << *request << "\n";

        auto self = weak_from_this();
        return Session->CreateHandle(CreateCallContext(), std::move(request))
            .Apply(
                [=, name = std::move(name)](
                    const TFuture<NProto::TCreateHandleResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr
                            ->HandleCreateHandle(future, name, started, r);
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
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& r)
    {
        try {
            auto response = future.GetValue();
            CheckResponse(response);

            auto handle = response.GetHandle();
            auto& infos = InitialFileSize ? IncompleteHandleInfos : HandleInfos;
            with_lock (StateLock) {
                TSegmentsPtr segments;

                /*
                                if (Spec.GetValidationEnabled()) {
                                    segments = new TSegments();
                                    segments->resize(Spec.GetInitialFileSize()
                   / SEGMENT_SIZE);
                                }
                */

                infos.emplace_back(name, handle, 0, std::move(segments));
                HandleInfosh[handle] = {name, handle, 0, std::move(segments)};
                HandlesLogToActual[r.GetNodeInfo().GetHandle()] = handle;
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
                [=, this](const TFuture<NProto::TSetNodeAttrResponse>& f)
                { return HandleResizeAfterCreateHandle(f, name, started); });
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

    TCompletedRequest HandleResizeAfterCreateHandle(
        const TFuture<NProto::TSetNodeAttrResponse>& future,
        const TString& name,
        TInstant started)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);
            bool handleFound = false;
            with_lock (StateLock) {
                ui32 handleIdx = 0;
                while (handleIdx < IncompleteHandleInfos.size()) {
                    auto& hinfo = IncompleteHandleInfos[handleIdx];
                    if (hinfo.Name == name) {
                        handleFound = true;

                        hinfo.Size =
                            Max(hinfo.Size, response.GetNode().GetSize());

                        STORAGE_INFO(
                            "updated file size for handle %lu and file %s"
                            ", size=%lu",
                            hinfo.Handle,
                            name.Quote().c_str(),
                            hinfo.Size);

                        break;
                    }

                    ++handleIdx;
                }

                if (handleIdx < IncompleteHandleInfos.size()) {
                    DoSwap(
                        IncompleteHandleInfos[handleIdx],
                        IncompleteHandleInfos.back());

                    HandleInfos.push_back(
                        std::move(IncompleteHandleInfos.back()));

                    IncompleteHandleInfos.pop_back();
                }
            }

            if (!handleFound) {
                STORAGE_WARN(
                    "handle for file %s not found",
                    name.Quote().c_str());
            }

            return {NProto::ACTION_CREATE_HANDLE, started, response.GetError()};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "set node attr handle for %s has failed: %s",
                name.Quote().c_str(),
                FormatError(error).c_str());

            return {NProto::ACTION_CREATE_HANDLE, started, error};
        }
    }

    static constexpr ui32 BlockSize = 4_KB;
    std::shared_ptr<char> Acalloc(ui64 dataSize)
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
        NCloud::NFileStore::NProto::TProfileLogRequestInfo& r)
    {
        if (Spec.GetNoRead()) {
            return {};
        }

        TGuard<TMutex> guard(StateLock);
        const auto started = TInstant::Now();
        if (UseFs()) {
            const auto handle = HandleIdMapped(r.GetRanges(0).GetHandle());
            // DUMP(handle, r.GetRanges(0).GetHandle());
            if (!handle) {
                STORAGE_WARN(
                    "read: no handle "
                    << r.GetRanges(0).GetHandle()
                    << " ranges size=" << r.GetRanges().size()
                    << " map size=" << HandlesLogToActual.size());
                return MakeFuture(TCompletedRequest{
                    NProto::ACTION_READ,
                    started,
                    MakeError(E_FAIL, "cancelled")});
            }
            // auto bytes = r.GetRanges(0).GetBytes();
            // auto buffer = NUnitTest::RandomString(bytes);

            // auto fh = TFileHandle{static_cast<FHANDLE>(handle)};
            auto& fh = OpenHandles[handle];
            STORAGE_DEBUG(
                "Read from " << handle << " fh.len=" << fh.GetLength()
                             << " fh.pos=" << fh.GetPosition());
            // AsyncIO.Write( (FHANDLE)(handle), buffer.data(), bytes,
            // r.GetRanges(0).GetOffset());
            auto buffer = Acalloc(r.GetRanges().cbegin()->GetBytes());

            //[[maybe_unused]] const auto reserved =
            fh.Reserve(
                r.GetRanges().cbegin()->GetOffset() +
                r.GetRanges().cbegin()->GetBytes());

            // DUMP("gorrread",reserved,r.GetRanges().cbegin()->GetOffset(),r.GetRanges().cbegin()->GetBytes());
            TFileHandle FileHandle{fh.GetHandle()};

            const auto future = AsyncIO.Read(
                // fh,
                FileHandle,
                {},
                r.GetRanges().cbegin()->GetBytes(),
                r.GetRanges().cbegin()->GetOffset())

                //.Apply([](const auto& v){DUMP("rrrres", v.GetValue());})
                ;
            FileHandle.Release();
            // fh.Release();

            return future.Apply(
                [started]([[maybe_unused]] const auto& future) mutable
                {
                    // DUMP("writeresult", future.GetValue());
                    return TCompletedRequest(NProto::ACTION_READ, started, {});
                });

            // return MakeFuture(TCompletedRequest{NProto::ACTION_READ, started,
            // {}});
        }

        // maybe before?
        /*
                if (HandleInfos.empty()) {
                    return DoCreateHandle();
                }
        */
        // auto handleInfo = GetHandleInfo();
        /*
                if (handleInfo.Size < ReadBytes) {
                    return DoWrite(handleInfo);
                }
        */

        // return {};

        /*
                const ui64 slotOffset = PickSlot(handleInfo, ReadBytes);
                const ui64 byteOffset = slotOffset * ReadBytes;
        */
        auto request = CreateRequest<NProto::TReadDataRequest>();
        /*
              request->SetHandle(handleInfo.Handle);
              request->SetOffset(byteOffset);
              request->SetLength(ReadBytes);
      */
        // ensure exists
        const auto handle = HandleIdMapped(r.GetNodeInfo().GetHandle());
        if (!handle) {
            return MakeFuture(TCompletedRequest{});
        }
        request->SetHandle(handle);
        request->SetOffset(r.GetRanges().cbegin()->GetOffset());
        request->SetLength(r.GetRanges().cbegin()->GetBytes());

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
        THandleInfo handleInfo,
        TInstant started
        //,ui64 byteOffset
    )
    {
        try {
            auto response = future.GetValue();
            CheckResponse(response);

            /*
            const auto& buffer = response.GetBuffer();

            // DUMP("read",buffer.Size(),TStringBuf(buffer, 0,
            // std::min<size_t>(10, buffer.size())));

                        ui64 segmentId = byteOffset / SEGMENT_SIZE;
                        for (ui64 offset = 0; offset < ReadBytes; offset +=
               SEGMENT_SIZE) { const TSegment* segment = reinterpret_cast<const
               TSegment*>(buffer.data() + offset); if
               (Spec.GetValidationEnabled()) {
                                Y_ABORT_UNLESS(handleInfo.Segments);

                                auto& segments = *handleInfo.Segments;
                                Y_ABORT_UNLESS(segmentId < segments.size());

                                TString message;
                                if (!Compare(segments[segmentId], *segment,
               &message)) { throw TServiceError(E_FAIL) << Sprintf( "Validation
               failed: %s", message.c_str());
                                }
                            }
                            ++segmentId;
                        }
            */

            /*
                        with_lock (StateLock) {
                            HandleInfos.emplace_back(std::move(handleInfo));
                        }
            */

            return {NProto::ACTION_READ, started, response.GetError()};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "read for %s has failed: %s",
                handleInfo.Name.Quote().c_str(),
                FormatError(error).c_str());

            return {NProto::ACTION_READ, started, error};
        }
    }

    TString MakeBuffer(ui64 bytes, ui64 offset = 0, const TString& start = {})
    {
        TStringBuilder ret;
        ret << start;
        while (ret.size() < bytes) {
            ret << " . " << offset + ret.size();
        }
        return ret.substr(0, bytes);
    }

    TFuture<TCompletedRequest> DoWrite(
        NCloud::NFileStore::NProto::TProfileLogRequestInfo& r
        // THandleInfo handleInfo = {}
    )
    {
        if (Spec.GetNoWrite()) {
            return {};
        }
        TGuard<TMutex> guard(StateLock);
        const auto started = TInstant::Now();

        if (UseFs()) {
            const auto logHandle = r.GetRanges(0).GetHandle();
            const auto handle = HandleIdMapped(logHandle);
            // DUMP(handle, r.GetRanges(0).GetHandle());
            if (!handle) {
                return MakeFuture(TCompletedRequest(
                    NProto::ACTION_WRITE,
                    started,
                    MakeError(
                        E_CANCELLED,
                        TStringBuilder{} << "write cancelled: no handle ="
                                         << logHandle)));   // todo
            }
            const auto bytes = r.GetRanges(0).GetBytes();
            const auto offset = r.GetRanges(0).GetOffset();

            TString buffer;

            if (Spec.GetWriteRandom()) {
                buffer = NUnitTest::RandomString(bytes, logHandle);
            } else if (Spec.GetWriteEmpty()) {
                buffer = TString{bytes, ' '};
            } else {
                buffer = MakeBuffer(
                    bytes,
                    offset,
                    TStringBuilder{} << "[ handle=" << logHandle
                                     << " node=" << r.GetNodeInfo().GetNodeId()
                                     << " bytes=" << bytes
                                     << " offset=" << offset);
            }

            // auto fh = TFileHandle{static_cast<FHANDLE>(handle)};

            auto& fh = OpenHandles[handle];

            STORAGE_DEBUG(
                "Write to " << handle << " fh.length=" << fh.GetLength()
                            << " fh.pos=" << fh.GetPosition());
            // AsyncIO.Write( (FHANDLE)(handle), buffer.data(), bytes,
            // r.GetRanges(0).GetOffset());
            // const auto fut = AsyncIO.Write(fh, buffer.data(), bytes,
            // r.GetRanges(0).GetOffset()).Apply([](const auto& v) {
            // DUMP("wwwwwwres", v.GetValue()); });
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
                [started]([[maybe_unused]] const auto& future) mutable
                {
                    // DUMP("writeresult", future.GetValue());
                    return TCompletedRequest(NProto::ACTION_WRITE, started, {});
                });

            //            return MakeFuture(TCompletedRequest{});
        }

        /*
                if (!handleInfo.Handle) {
                    if (HandleInfos.empty()) {
                        return DoCreateHandle();
                    }

                    handleInfo = GetHandleInfo();
                }

                const auto started = TInstant::Now();
                ui64 byteOffset = handleInfo.Size;
                if (RandomNumber<double>() < AppendProbability) {
                    handleInfo.Size += WriteBytes;
                } else {
                    handleInfo.Size = Max(handleInfo.Size, WriteBytes);
                    const ui64 slotOffset = PickSlot(handleInfo, WriteBytes);
                    byteOffset = slotOffset * WriteBytes;
                }

                auto request = CreateRequest<NProto::TWriteDataRequest>();
                request->SetHandle(handleInfo.Handle);
                request->SetOffset(byteOffset);
        */
        // request->SetBuffer();
        auto request = CreateRequest<NProto::TWriteDataRequest>();
        /*if (HandlesLogToActual.contains(r.GetRanges(0).GetHandle())) {
            DUMP("unknown handle", r.GetRanges(0).GetHandle());
        }
        request->SetHandle(HandlesLogToActual[r.GetRanges(0).GetHandle()]);
*/
        const auto handle = HandleIdMapped(r.GetRanges(0).GetHandle());

        if (!handle) {
            return MakeFuture(TCompletedRequest{});
        }
        request->SetHandle(handle);

        // auto byteOffset = r.GetRanges(0).GetOffset();
        request->SetOffset(r.GetRanges(0).GetOffset());
        auto bytes = r.GetRanges(0).GetBytes();
        //{"TimestampMcs":1465489895000,"DurationMcs":2790,"RequestType":44,"Ranges":[{"NodeId":2,"Handle":20680158862113389,"Offset":13,"Bytes":12}]}
        // request.set
        // TString buffer(bytes, '\0');
        // NUnitTest::RandomString()
        // GenerateRandomString
        // GenerateRandomData
        auto buffer = NUnitTest::RandomString(bytes);

        *request->MutableBuffer() = std::move(buffer);

        ++LastWriteRequestId;
        /*
                TString buffer(WriteBytes, '\0');
                ui64 segmentId = byteOffset / SEGMENT_SIZE;
                for (ui64 offset = 0; offset < WriteBytes; offset +=
           SEGMENT_SIZE) { TSegment* segment =
                        reinterpret_cast<TSegment*>(buffer.begin() + offset);
                    segment->Handle = handleInfo.Handle;
                    segment->LastWriteRequestId = LastWriteRequestId;

                    if (Spec.GetValidationEnabled()) {
                        Y_ABORT_UNLESS(handleInfo.Segments);

                        auto& segments = *handleInfo.Segments;
                        if (segments.size() <= segmentId) {
                            segments.emplace_back();
                        }

                        segments[segmentId] = *segment;
                    }

                    ++segmentId;
                }
                *request->MutableBuffer() = std::move(buffer);
        */
        auto self = weak_from_this();
        return Session->WriteData(CreateCallContext(), std::move(request))
            .Apply(
                [=](const TFuture<NProto::TWriteDataResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleWrite(
                            future,
                            // handleInfo,
                            started);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_WRITE,
                        started,
                        MakeError(E_FAIL, "cancelled")};
                });
    }

    TCompletedRequest HandleWrite(
        const TFuture<NProto::TWriteDataResponse>& future,
        // THandleInfo handleInfo,
        TInstant started)
    {
        try {
            auto response = future.GetValue();
            CheckResponse(response);

            with_lock (StateLock) {
                //      HandleInfos.emplace_back(std::move(handleInfo));
            }

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

    /*
    void CreateIfMissing(TString path)
    {
        TFsPath(path).MkDirs();
    }
    */

    static TNodeLocal Mkdir(const TString& name)
    {
        //[[maybe_unused]] const auto mkdires =
        NFs::MakeDirectoryRecursive(name);
        // mkdir(fname.c_str(), r.GetNodeInfo().GetMode());
        const auto inode = TFileStat{name}.INode;
        // DUMP("mkdir", inode, name);
        return inode;
    }

    TFuture<TCompletedRequest> DoCreateNode(
        NCloud::NFileStore::NProto::TProfileLogRequestInfo& r)
    {
        if (Spec.GetNoWrite()) {
            return {};
        }

        TGuard<TMutex> guard(StateLock);
        const auto started = TInstant::Now();

        /*
                auto name = GenerateNodeName();
                StagedNodes[name] = {};
        */
        if (UseFs()) {
            // DUMP("========================================================");

            auto parentNode =
                NodeIdMapped(r.GetNodeInfo().GetNewParentNodeId());

            parentNode = CreateDirIfMissingByNodeLog(
                r.GetNodeInfo().GetNewParentNodeId());

            // DUMP(parentNode, PathByNode(parentNode));

            auto fname = Spec.GetReplayRoot() + "/" + PathByNode(parentNode) +
                         r.GetNodeInfo().GetNewNodeName();
            // DUMP(Spec.GetReplayRoot(), fname);

            ui64 nodeid = 0;
            bool isDir = false;
            switch (r.GetNodeInfo().GetType()) {
                case NProto::E_REGULAR_NODE: {
                    // DUMP("file", fname);

                    TFileHandle fh(
                        fname,
                        // static_cast<EOpenModeFlag>(r.GetNodeInfo().GetMode())
                        OpenAlways | RdWr   // RdOnly
                    );
                    fh.Reserve(r.GetNodeInfo().GetSize());
                    //        TFileStat{};
                    //          const auto id = (FHANDLE)(fh);
                    // TFileStat stat{id};
                    if (fh) {
                        nodeid = TFileStat{fh}.INode;
                    } else {
                        nodeid = TFileStat{fname}.INode;
                    }
                    // GetStatByHandle(stat, id);
                } break;
                case NProto::E_DIRECTORY_NODE: {
                    isDir = true;
                    nodeid = Mkdir(fname);
                    // id = stat;
                    // DUMP("mkdir=", mkdires, id);

                    // DUMP("dir");
                } break;
                case NProto::E_LINK_NODE:
                    // NFs::HardLink(const TString &existingPath, const TString
                    // &newPath) NFs::SymLink(const TString &targetPath, const
                    // TString &linkPath)
                    // DUMP("link");
                    break;
                case NProto::E_SOCK_NODE:
                    // DUMP("sock");
                    break;
                case NProto::E_INVALID_NODE:
                    break;
            }

            if (!nodeid) {
                nodeid = TFileStat{fname}.INode;
            }

            // DUMP(stat.INode, stat.IsDir(), stat.IsFile());

            // DUMP(id, fh.GetLength(), fh.GetPosition());

            // CreateIfMissing(PathByNode())
            if (nodeid) {
                NodesLogToLocal[r.GetNodeInfo().GetNodeId()] = nodeid;
                NodePath[nodeid] = PathByNode(parentNode) +
                                   r.GetNodeInfo().GetNewNodeName() +
                                   (isDir ? "/" : "");
                // DUMP("savepath", nodeid, NodePath[nodeid]);

                // DUMP(NodePath[id], parentNode);
            }

            // DUMP(NodePath);

            return MakeFuture(
                TCompletedRequest(NProto::ACTION_CREATE_HANDLE, started, {}));
        }
        /*


         / 1
         /dir 2      p=1
         /dir/file 3 p=2







        */
        auto request = CreateRequest<NProto::TCreateNodeRequest>();

        //        request->SetNodeId(r.GetNodeInfo().GetNodeId());
        // request->SetName(r.GetNodeInfo().GetNodeName());
        // if (r.GetNodeInfo().GetNewParentNodeId() == RootNodeId) {
        //    request->SetNodeId(r.GetNodeInfo().GetNewParentNodeId());
        //} else

        const auto parentNode =
            NodeIdMapped(r.GetNodeInfo().GetNewParentNodeId());
        if (!parentNode) {
            // DUMP("nop", r.GetNodeInfo().GetNewParentNodeId());
            return MakeFuture(TCompletedRequest{});
        }
        request->SetNodeId(parentNode);
        // DUMP("pppath", PathByNode(parentNode));
        /*
                if
           (NodesLogToActual.contains(r.GetNodeInfo().GetNewParentNodeId())) {
                    request->SetNodeId(
                        NodesLogToActual[r.GetNodeInfo().GetNewParentNodeId()]);
                } else {
                    DUMP(
                        "unknown parent node",
                        r.GetNodeInfo().GetNewParentNodeId(),
                        NodesLogToActual);
                    return MakeFuture(TCompletedRequest{});
                }
        */
        auto name = r.GetNodeInfo().GetNewNodeName();
        request->SetName(r.GetNodeInfo().GetNewNodeName());

        // request->SetGid();
        // request->SetUid();

        // DUMP(r.GetNodeInfo().GetType());
        switch (r.GetNodeInfo().GetType()) {
            case NProto::E_REGULAR_NODE:
                // DUMP("file");
                request->MutableFile()->SetMode(r.GetNodeInfo().GetMode());
                break;
            case NProto::E_DIRECTORY_NODE:
                // DUMP("dir");
                request->MutableDirectory()->SetMode(r.GetNodeInfo().GetMode());
                break;
            case NProto::E_LINK_NODE:
                // DUMP("link");
                //  request->MutableLink()->SetTargetNode(r.GetNodeInfo().Get...);
                //  request->MutableLink()->SetFollowerNodeName(r.GetNodeInfo().Get...);
                return MakeFuture(TCompletedRequest{});   // TODO
                break;

                // case NProto::E_ symlink?:
                // request->MutableSymlink()->SetTargetPath();

            case NProto::E_SOCK_NODE:
                // DUMP("sock");
                request->MutableSocket()->SetMode(r.GetNodeInfo().GetMode());
                break;
            case NProto::E_INVALID_NODE:
                // DUMP("invv?????");
                return MakeFuture(
                    TCompletedRequest{});   // Do not create files with invalid
                                            // type - too hard to delete them
                break;
        }

        // request->Se

        // nfs     CreateNode      0.006404s       S_OK {new_parent_node_id=1,
        // new_node_name=home, mode=509, node_id=12526, size=0}

        // {"TimestampMcs":1725895166478218,"DurationMcs":6328,"RequestType":26,"ErrorCode":0,"NodeInfo":{"NewParentNodeId":1,"NewNodeName":"home","Mode":509,"NodeId":12526,"Size":0}}

        // DUMP(request);
        Cerr << "crreq=" << *request.get() << "\n";
        // auto started = TInstant::Now();
        auto self = weak_from_this();
        return Session->CreateNode(CreateCallContext(), std::move(request))
            .Apply(
                [=](const TFuture<NProto::TCreateNodeResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleCreateNode(future, name, started, r);
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
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& r)
    {
        TGuard<TMutex> guard(StateLock);
        // Y_ABORT_UNLESS(StagedNodes.erase(name));

        try {
            auto response = future.GetValue();

            // DUMP("crd",name,r.GetNodeInfo().GetNodeId(),response.GetNode().GetId());

            CheckResponse(response);

            Nodes[name] = TNode{name, response.GetNode()};
            // Nodes[name].Attrs.get
            //  NodesLogToActual[r.GetNodeInfo().GetNodeId()] = Nodes[name];
            if (response.GetNode().GetId()) {
                NodesLogToLocal[r.GetNodeInfo().GetNodeId()] =
                    response.GetNode().GetId();

                /*
                                DUMP(
                                    "savepath4",
                                    r.GetNodeInfo().GetNodeId(),
                                    NodesLogToLocal[r.GetNodeInfo().GetNodeId()]);
                */
                /*
                                const auto parentNode =
                                    NodeIdMapped(r.GetNodeInfo().GetNewParentNodeId());
                                NodePath[response.GetNode().GetId()] =
                                    NodePath[parentNode] + "/" +
                                    r.GetNodeInfo().GetNewNodeName();
                                DUMP(NodePath[response.GetNode().GetId()],
                   parentNode);
                */
            }
            // DUMP(r.GetNodeInfo().GetNodeId(),response.GetNode().GetId()//,NodesLogToActual);

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
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& r)

    {
        if (Spec.GetNoRead()) {
            return {};
        }

        TGuard<TMutex> guard(StateLock);

        if (UseFs()) {
            const auto parentnodeid =
                NodeIdMapped(r.GetNodeInfo().GetParentNodeId());

            auto fname = Spec.GetReplayRoot() +
                         //"/" +
                         PathByNode(parentnodeid) +
                         r.GetNodeInfo().GetNodeName();

            const auto newparentnodeid =
                NodeIdMapped(r.GetNodeInfo().GetNewParentNodeId());

            auto newfname = Spec.GetReplayRoot() +   //"/" +
                            PathByNode(newparentnodeid) +
                            r.GetNodeInfo().GetNewNodeName();
            // NodePath[0];
            // DUMP("brename", TFileStat{fname}.Size, TFileStat{newfname}.Size);
            const auto renameres = NFs::Rename(fname, newfname);
            // DUMP("arename", TFileStat{fname}.Size, TFileStat{newfname}.Size);
            //  DUMP(renameres, fname, newfname);
            STORAGE_DEBUG(
                "rename " << fname << " => " << newfname << " : " << renameres);
            return MakeFuture(TCompletedRequest{});
        }

        /*
                if (Nodes.empty()) {
                    // return DoCreateNode({});
                }

                auto started = TInstant::Now();

                auto it = Nodes.begin();
                auto old = it->first;
                auto newbie = GenerateNodeName();

                auto request = CreateRequest<NProto::TRenameNodeRequest>();
                request->SetNodeId(RootNodeId);
                request->SetName(old);
                request->SetNewParentId(RootNodeId);
                request->SetNewName(newbie);

                StagedNodes[newbie] = {};
                StagedNodes[old] = std::move(it->second);
                Nodes.erase(it);
        */

        auto started = TInstant::Now();
        auto request = CreateRequest<NProto::TRenameNodeRequest>();
        // {"TimestampMcs":895166000,"DurationMcs":2949,"RequestType":28,"NodeInfo":{"ParentNodeId":3,"NodeName":"HEAD.lock","NewParentNodeId":3,"NewNodeName":"HEAD"}}
        // nfs     RenameNode      0.002569s       S_OK {parent_node_id=12527,
        // node_name=HEAD.lock, new_parent_node_id=12527, new_node_name=HEAD}
        // request->SetNodeId(NodesLogToActual[r.GetNodeInfo().GetNodeId()]);
        const auto node = NodeIdMapped(r.GetNodeInfo().GetParentNodeId());
        if (!node) {
            return MakeFuture(TCompletedRequest{});
        }
        request->SetNodeId(node);
        request->SetName(r.GetNodeInfo().GetNodeName());
        request->SetNewParentId(
            NodeIdMapped(r.GetNodeInfo().GetNewParentNodeId()));
        request->SetNewName(r.GetNodeInfo().GetNewNodeName());
        request->SetFlags(r.GetNodeInfo().GetFlags());

        // Cerr << "rename=" << *request << "\n";
        auto self = weak_from_this();
        return Session->RenameNode(CreateCallContext(), std::move(request))
            .Apply(
                [=   // ,

                 // old = std::move(old), newbie = std::move(newbie)

        ](const TFuture<NProto::TRenameNodeResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleRenameNode(
                            future,
                            // old, newbie,

                            started,
                            r);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_RENAME_NODE,
                        started,
                        MakeError(E_CANCELLED, "cancelled")};
                });
    }

    TCompletedRequest HandleRenameNode(
        const TFuture<NProto::TRenameNodeResponse>& future,
        // const TString& old,
        // const TString& newbie,
        TInstant started,
        const NCloud::NFileStore::NProto::TProfileLogRequestInfo& r)
    {
        TGuard<TMutex> guard(StateLock);
        /*
                auto node = std::move(StagedNodes[old]);
                StagedNodes.erase(old);
                StagedNodes.erase(newbie);
        */
        // Nodes.erase(r.GetNodeInfo().GetNodeName());
        try {
            auto response = future.GetValue();
            CheckResponse(response);
            Nodes[r.GetNodeInfo().GetNewNodeName()] =
                std::move(Nodes[r.GetNodeInfo().GetNodeName()]);
            Nodes.erase(r.GetNodeInfo().GetNodeName());
            // Nodes[newbie] = std::move(node);
            return {NProto::ACTION_RENAME_NODE, started, response.GetError()};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "rename node %s has failed: %s",
                r.GetNodeInfo().GetNodeName().c_str(),   // old.c_str(),
                FormatError(error).c_str());

            return {NProto::ACTION_RENAME_NODE, started, error};
        }
    }

    TFuture<TCompletedRequest> DoUnlinkNode(
        NCloud::NFileStore::NProto::TProfileLogRequestInfo& r)
    {
        if (Spec.GetNoWrite()) {
            return {};
        }

        // UnlinkNode      0.002605s       S_OK    {parent_node_id=3,
        // node_name=tfrgYZ1}

        TGuard<TMutex> guard(StateLock);
        auto started = TInstant::Now();

        if (UseFs()) {
            // DUMP(NodePath);
            const auto parentNodeId =
                NodeIdMapped(r.GetNodeInfo().GetParentNodeId());
            if (!parentNodeId) {
                STORAGE_WARN(
                    "unlink : no parent orig="
                    << r.GetNodeInfo().GetParentNodeId());
                return MakeFuture(TCompletedRequest(
                    NProto::ACTION_REMOVE_NODE,
                    started,
                    MakeError(E_CANCELLED, "cancelled")));
            }
            const auto fname = Spec.GetReplayRoot() + "/" +
                               PathByNode(parentNodeId) +
                               r.GetNodeInfo().GetNodeName();
            // DUMP(r.GetNodeInfo().GetParentNodeId(),PathByNode(NodeIdMapped(r.GetNodeInfo().GetParentNodeId())));

            // {parent_node_id=3, node_name=tfrgYZ1}
            //            const auto unlinkres = unlink(fname.c_str());
            const auto unlinkres = NFs::Remove(fname);
            STORAGE_DEBUG("unlink " << fname << " = " << unlinkres);
            // DUMP(unlinkres, fname);
            // TODO :
            // NodesLogToActual.erase(...)
            // NodePath.erase(...)
            return MakeFuture(
                TCompletedRequest(NProto::ACTION_REMOVE_NODE, started, {}));
        }

        /*
                if (Nodes.empty()) {
                    // return DoCreateNode();
                }
        */

        /*
                auto it = Nodes.begin();
                auto name = it->first;
        */

        auto name = r.GetNodeInfo().GetNodeName();
        auto request = CreateRequest<NProto::TUnlinkNodeRequest>();
        // request->SetNodeId(RootNodeId);
        request->SetName(name);
        // request.set
        // request->SetNodeId(r.GetNodeInfo().GetNodeId());
        // request->SetNodeId(NodesLogToActual[r.GetNodeInfo().GetParentNodeId()]);
        const auto node = NodeIdMapped(r.GetNodeInfo().GetParentNodeId());
        if (!node) {
            return MakeFuture(TCompletedRequest{});
        }
        request->SetNodeId(node);
        /*
                StagedNodes[name] = std::move(it->second);
                Nodes.erase(it);
        */
        auto self = weak_from_this();
        return Session->UnlinkNode(CreateCallContext(), std::move(request))
            .Apply(
                [=, name = std::move(name)](
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
        with_lock (StateLock) {
            StagedNodes.erase(name);
        }

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
    /*
        TFuture<TCompletedRequest> DoCreateHandle2()
        {
            static const int flags =
                ProtoFlag(NProto::TCreateHandleRequest::E_READ) |
                ProtoFlag(NProto::TCreateHandleRequest::E_WRITE);

            TGuard<TMutex> guard(StateLock);
            if (Nodes.empty()) {
                // return DoCreateNode();
            }

            auto started = TInstant::Now();

            auto it = Nodes.begin();
            if (Nodes.size() > 1) {
                std::advance(it, Min(RandomNumber(Nodes.size() - 1), 64lu));
            }

            auto name = it->first;

            auto request = CreateRequest<NProto::TCreateHandleRequest>();
            request->SetNodeId(RootNodeId);
            request->SetName(name);
            request->SetFlags(flags);

            auto self = weak_from_this();
            return Session->CreateHandle(CreateCallContext(),
       std::move(request)) .Apply(
                    [=, name = std::move(name)](
                        const TFuture<NProto::TCreateHandleResponse>& future)
                    {
                        if (auto ptr = self.lock()) {
                            return ptr->HandleCreateHandle2(future, name,
       started);
                        }

                        return TCompletedRequest{
                            NProto::ACTION_CREATE_HANDLE,
                            started,
                            MakeError(E_CANCELLED, "cancelled")};
                    });
        }

        TCompletedRequest HandleCreateHandle2(
            const TFuture<NProto::TCreateHandleResponse>& future,
            const TString name,
            TInstant started)
        {
            TGuard<TMutex> guard(StateLock);

            try {
                auto response = future.GetValue();
                CheckResponse(response);

                auto handle = response.GetHandle();
                Handles[handle] = THandle{name, handle};

                return {NProto::ACTION_CREATE_HANDLE, started,
       response.GetError()}; } catch (const TServiceError& e) { auto error =
       MakeError(e.GetCode(), TString{e.GetMessage()}); STORAGE_ERROR( "create
       handle for %s has failed: %s", name.c_str(), FormatError(error).c_str());

                return {NProto::ACTION_CREATE_HANDLE, started, error};
            }
        }
    */
    TFuture<TCompletedRequest> DoDestroyHandle(
        NCloud::NFileStore::NProto::TProfileLogRequestInfo r)
    {
        // DestroyHandle   0.002475s       S_OK    {node_id=10,
        // handle=61465562388172112}
        TGuard<TMutex> guard(StateLock);
        auto started = TInstant::Now();

        if (UseFs()) {
            const auto handleid = HandleIdMapped(r.GetNodeInfo().GetHandle());

            const auto& it = OpenHandles.find(handleid);
            if (it == OpenHandles.end()) {
                return MakeFuture(TCompletedRequest(
                    NProto::ACTION_DESTROY_HANDLE,
                    started,
                    MakeError(
                        E_CANCELLED,
                        TStringBuilder{} << "close " << handleid << " <- "
                                         << r.GetNodeInfo().GetHandle()
                                         << " fail: not found in "
                                         << OpenHandles.size())));
            }

            auto& fhandle = it->second;
            const auto len = fhandle.GetLength();
            const auto pos = fhandle.GetPosition();
            // auto closed =
            fhandle.Close();
            OpenHandles.erase(handleid);
            HandlesLogToActual.erase(r.GetNodeInfo().GetHandle());
            STORAGE_DEBUG(
                "Close " << handleid << " orig="
                         << r.GetNodeInfo().GetHandle()
                         //<< " closed=" << closed
                         << " pos=" << pos << " len=" << len
                         << " open map size=" << OpenHandles.size()
                         << " map size=" << HandlesLogToActual.size());
            return MakeFuture(
                TCompletedRequest(NProto::ACTION_DESTROY_HANDLE, started, {}));
        }

        /*
                if (Handles.empty()) {
                    return DoCreateHandle();
                }

                auto started = TInstant::Now();
                auto it = Handles.begin();

                ui64 handle = it->first;
                auto name = it->second.Path;
                Handles.erase(it);
                if (auto it = Locks.find(handle); it != Locks.end()) {
                    Locks.erase(it);
                }
                if (auto it = StagedLocks.find(handle); it != StagedLocks.end())
           { StagedLocks.erase(it);
                }

                auto request = CreateRequest<NProto::TDestroyHandleRequest>();
                request->SetHandle(handle);
        */
        auto name = r.GetNodeInfo().GetNodeName();

        auto request = CreateRequest<NProto::TDestroyHandleRequest>();

        const auto handle = HandleIdMapped(r.GetNodeInfo().GetHandle());
        if (!handle) {
            return MakeFuture(TCompletedRequest{});
        }

        HandlesLogToActual.erase(handle);

        request->SetHandle(handle);

        auto self = weak_from_this();
        return Session->DestroyHandle(CreateCallContext(), std::move(request))
            .Apply(
                [=, name = std::move(name)](
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
        NCloud::NFileStore::NProto::TProfileLogRequestInfo& r)
    {
        if (Spec.GetNoRead()) {
            return {};
        }

        TGuard<TMutex> guard(StateLock);
        const auto started = TInstant::Now();

        // TODO: by parent + name        //
        // {"TimestampMcs":1726503153650998,"DurationMcs":7163,"RequestType":35,"ErrorCode":2147942422,"NodeInfo":{"NodeName":"security.capability","NewNodeName":"","NodeId":5,"Size":0}}

        // {"TimestampMcs":1726615533406265,"DurationMcs":192,"RequestType":33,"ErrorCode":2147942402,"NodeInfo":{"ParentNodeId":17033,"NodeName":"CPackSourceConfig.cmake","Flags":0,"Mode":0,"NodeId":0,"Handle":0,"Size":0}}
        // nfs     GetNodeAttr     0.006847s       S_OK    {parent_node_id=1,
        // node_name=freeminer, flags=0, mode=509, node_id=2, handle=0, size=0}

        if (UseFs()) {
            if (r.GetNodeInfo().GetNodeName()) {
                KnownLogNodes[r.GetNodeInfo().GetNodeId()].Name =
                    r.GetNodeInfo().GetNodeName();
                /*
                                DUMP(
                                    "saveknown1",
                                    r.GetNodeInfo().GetNodeId(),
                                    r.GetNodeInfo().GetNodeName());
                */
            }
            if (r.GetNodeInfo().GetParentNodeId() &&
                r.GetNodeInfo().GetParentNodeId() !=
                    r.GetNodeInfo().GetNodeId())
            {
                KnownLogNodes[r.GetNodeInfo().GetNodeId()].ParentLog =
                    r.GetNodeInfo().GetParentNodeId();
                /*
                                DUMP(
                                    "saveknown2",
                                    r.GetNodeInfo().GetNodeId(),
                                    r.GetNodeInfo().GetParentNodeId());
                */
            }

            // TODO: can create and truncate to size here missing files

            // DUMP(KnownNodes);

            const auto nodeid = NodeIdMapped(r.GetNodeInfo().GetNodeId());

            if (!nodeid) {
                return MakeFuture(TCompletedRequest{
                    NProto::ACTION_GET_NODE_ATTR,
                    started,
                    MakeError(E_CANCELLED, "cancelled")});
            }

            auto fname = Spec.GetReplayRoot() + "/" + PathByNode(nodeid);
            [[maybe_unused]] const auto stat = TFileStat{fname};
            // DUMP("gna", fname, stat.INode);

            return MakeFuture(
                TCompletedRequest(NProto::ACTION_GET_NODE_ATTR, started, {}));
        }
        /*
           if (Nodes.empty()) {
                //return DoCreateNode();
           }
   */
        /*
                auto it = Nodes.begin();
                if (Nodes.size() > 1) {
                    std::advance(it, Min(RandomNumber(Nodes.size() - 1), 64lu));
                }

                auto name = it->first;

                auto request = CreateRequest<NProto::TGetNodeAttrRequest>();
                request->SetNodeId(RootNodeId);
                request->SetName(name);
        */

        /*
                if
           (!NodesLogToActual.contains(r.GetNodeInfo().GetParentNodeId())) {
                    DUMP("skip nodeattr");
                    return MakeFuture(TCompletedRequest{});
                }
        */
        auto request = CreateRequest<NProto::TGetNodeAttrRequest>();
        // {"TimestampMcs":240399000,"DurationMcs":163,"RequestType":33,"NodeInfo":{"ParentNodeId":3,"NodeName":"branches","Flags":0,"Mode":0,"NodeId":0,"Handle":0,"Size":0}}
        // request->SetNodeId(NodesLogToActual[r.GetNodeInfo().GetParentNodeId()]);
        const auto node = NodeIdMapped(r.GetNodeInfo().GetParentNodeId());
        if (!node) {
            return MakeFuture(TCompletedRequest{});
        }
        request->SetNodeId(node);
        auto name = r.GetNodeInfo().GetNodeName();
        request->SetName(r.GetNodeInfo().GetNodeName());
        // request->SetFlags(r.GetNodeInfo().GetFlags());
        request->SetFlags(r.GetNodeInfo().GetFlags());
        /*
                DUMP(
                    "ids?",
                    r.GetNodeInfo().GetParentNodeId(),
                    NodesLogToActual[r.GetNodeInfo().GetParentNodeId()]);
                if (!NodesLogToActual[r.GetNodeInfo().GetParentNodeId()]) {
                    DUMP(NodesLogToActual);
                }
        */
        Cerr << "attr=" << *request << "\n";
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
            /*
                        if (response.GetNode().SerializeAsString() !=
                            Nodes[name].Attrs.SerializeAsString())
                        {
                            auto error = MakeError(
                                E_FAIL,
                                TStringBuilder()
                                    << "attributes are not equal for node " <<
               name << ": "
                                    << response.GetNode().DebugString().Quote()
                                    << " != " <<
               Nodes[name].Attrs.DebugString().Quote());
                            STORAGE_ERROR(error.GetMessage().c_str());
                        }
            */
            return {NProto::ACTION_GET_NODE_ATTR, started, {}};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "get node attr %s has failed: %s",
                name.c_str(),
                FormatError(error).c_str());

            return {NProto::ACTION_GET_NODE_ATTR, started, {}};
            // return {NProto::ACTION_GET_NODE_ATTR, started, error};
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
            // return DoCreateHandle();
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

    TString GenerateNodeName()
    {
        return TStringBuilder()
               << Headers.GetClientId() << ":" << CreateGuidAsString();
    }

    THandleInfo GetHandleInfo()
    {
        Y_ABORT_UNLESS(!HandleInfos.empty());
        ui64 index = RandomNumber(HandleInfos.size());
        std::swap(HandleInfos[index], HandleInfos.back());

        auto handle = HandleInfos.back();
        HandleInfos.pop_back();

        return handle;
    }

    THandleInfo GetHandleInfo(ui64 id)
    {
        return HandleInfos[id];
        /*
        Y_ABORT_UNLESS(!HandleInfos.empty());
        ui64 index = RandomNumber(HandleInfos.size());
        std::swap(HandleInfos[index], HandleInfos.back());

        auto handle = HandleInfos.back();
        HandleInfos.pop_back();

        return handle;
        */
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
    /*
        ui64 PickSlot(THandleInfo& handleInfo, ui64 reqBytes)
        {
            const ui64 slotCount = handleInfo.Size / reqBytes;
            Y_ABORT_UNLESS(slotCount);
            if (Spec.GetSequential()) {
                return handleInfo.LastSlot = (handleInfo.LastSlot + 1) %
       slotCount;
            }

            return RandomNumber(slotCount);
        }
    */
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRequestGeneratorPtr CreateReplayRequestGenerator(
    NProto::TReplaySpec spec,
    ILoggingServicePtr logging,
    ISessionPtr session,
    TString filesystemId,
    NProto::THeaders headers)
{
    return std::make_shared<TReplayRequestGenerator>(
        std::move(spec),
        std::move(logging),
        std::move(session),
        std::move(filesystemId),
        std::move(headers));
}

}   // namespace NCloud::NFileStore::NReplay
