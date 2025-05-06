#include "fs_impl.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>

namespace NCloud::NFileStore::NFuse {

using namespace NCloud::NFileStore::NVFS;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

ELogPriority GetErrorPriority(ui32 code)
{
    if (FACILITY_FROM_CODE(code) == FACILITY_FILESTORE) {
        return TLOG_DEBUG;
    } else {
        return TLOG_ERR;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TFileSystem::TFileSystem(
        ILoggingServicePtr logging,
        IProfileLogPtr profileLog,
        ISchedulerPtr scheduler,
        ITimerPtr timer,
        TFileSystemConfigPtr config,
        IFileStorePtr session,
        IRequestStatsPtr stats,
        ICompletionQueuePtr queue,
        THandleOpsQueuePtr handleOpsQueue,
        TWriteBackCachePtr writeBackCache)
    : Logging(std::move(logging))
    , ProfileLog(std::move(profileLog))
    , Timer(std::move(timer))
    , Scheduler(std::move(scheduler))
    , Session(std::move(session))
    , Config(std::move(config))
    , RequestStats(std::move(stats))
    , CompletionQueue(std::move(queue))
    , FSyncQueue(Config->GetFileSystemId(), Logging)
    , XAttrCache(
        Timer,
        Config->GetXAttrCacheLimit(),
        Config->GetXAttrCacheTimeout())
    , HandleOpsQueue(std::move(handleOpsQueue))
    , WriteBackCache(std::move(writeBackCache))
{
    Log = Logging->CreateLog("NFS_FUSE");
}

TFileSystem::~TFileSystem()
{
    Reset();
}

void TFileSystem::Init()
{
    STORAGE_INFO("scheduling destroy handle queue processing");
    ScheduleProcessHandleOpsQueue();
}

void TFileSystem::Reset()
{
    STORAGE_INFO("resetting filesystem cache");
    ClearDirectoryCache();
}

void TFileSystem::ScheduleProcessHandleOpsQueue()
{
    if (Config->GetAsyncDestroyHandleEnabled()) {
        Scheduler->Schedule(
        Timer->Now() + Config->GetAsyncHandleOperationPeriod(),
        [=, ptr = weak_from_this()] () {
            if (auto self = ptr.lock()) {
                self->ProcessHandleOpsQueue();
            }
        });
    }
}

bool TFileSystem::CheckError(
    TCallContext& callContext,
    fuse_req_t req,
    const NProto::TError& error)
{
    if (HasError(error)) {
        STORAGE_LOG(GetErrorPriority(error.GetCode()),
            "request #" << fuse_req_unique(req)
            << " failed: " << FormatError(error));

        ReplyError(callContext, error, req, ErrnoFromError(error.GetCode()));
        return false;
    }

    return true;
}

bool TFileSystem::ValidateNodeId(
    TCallContext& callContext,
    fuse_req_t req,
    fuse_ino_t ino)
{
    if (Y_UNLIKELY(!ino)) {
        ReplyError(callContext, MakeError(E_FS_INVAL), req, EINVAL);
        return false;
    }
    return true;
}

bool TFileSystem::UpdateNodeCache(
    const NProto::TNodeAttr& attrs,
    fuse_entry_param& entry)
{
    if (attrs.GetId() == InvalidNodeId) {
        return false;
    }

    with_lock (NodeCacheLock) {
        auto* node = NodeCache.TryAddNode(attrs);
        Y_ABORT_UNLESS(node);

        entry.ino = attrs.GetId();
        entry.generation = NodeCache.Generation();
        entry.attr_timeout = Config->GetAttrTimeout().SecondsFloat();
        entry.entry_timeout = Config->GetEntryTimeout().SecondsFloat();

        ConvertAttr(Config->GetPreferredBlockSize(), node->Attrs, entry.attr);
    }

    return true;
}

void TFileSystem::UpdateXAttrCache(
    ui64 ino,
    const TString& name,
    const TString& value,
    ui64 version,
    const NProto::TError& error)
{
    TGuard g{XAttrCacheLock};
    if (HasError(error)) {
        if (STATUS_FROM_CODE(error.GetCode()) == NProto::E_FS_NOXATTR) {
            XAttrCache.AddAbsent(ino, name);
        }
        return;
    }

    XAttrCache.Add(ino, name, value, version);
}

void TFileSystem::ReplyCreate(
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    ui64 handle,
    const NProto::TNodeAttr& attrs)
{
    STORAGE_TRACE("inserting node: " << DumpMessage(attrs));

    fuse_entry_param entry = {};
    if (!UpdateNodeCache(attrs, entry)) {
        ReplyError(callContext, MakeError(E_FS_IO), req, EIO);
        return;
    }

    fuse_file_info fi = {};
    fi.fh = handle;

    const int res = ReplyCreate(callContext, error, req, &entry, &fi);
    if (res == -ENOENT) {
        // syscall was interrupted
        with_lock (NodeCacheLock) {
            NodeCache.ForgetNode(entry.ino, 1);
        }
    }
}

void TFileSystem::ReplyEntry(
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const NProto::TNodeAttr& attrs)
{
    STORAGE_TRACE("inserting node: " << DumpMessage(attrs));

    fuse_entry_param entry = {};
    if (!UpdateNodeCache(attrs, entry)) {
        ReplyError(callContext, MakeError(E_FS_IO), req, EIO);
        return;
    }

    const int res = ReplyEntry(callContext, error, req, &entry);
    if (res == -ENOENT) {
        // syscall was interrupted
        with_lock (NodeCacheLock) {
            NodeCache.ForgetNode(entry.ino, 1);
        }
    }
}

void TFileSystem::ReplyXAttrInt(
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const TString& value,
    size_t size)
{
    if (size >= value.size()) {
        ReplyBuf(callContext, error, req, value.data(), value.size());
    } else if (!size) {
        ReplyXAttr(callContext, error, req, value.size());
    } else {
        ReplyError(
            callContext,
            MakeError(MAKE_FILESTORE_ERROR(NProto::E_FS_RANGE)),
            req,
            ERANGE);
    }
}

void TFileSystem::ReplyAttr(
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const NProto::TNodeAttr& attrs)
{
    fuse_entry_param entry = {};
    if (!UpdateNodeCache(attrs, entry)) {
        ReplyError(callContext, MakeError(E_FS_IO), req, EIO);
        return;
    }

    ReplyAttr(
        callContext,
        error,
        req,
        &entry.attr,
        0);
}

void TFileSystem::CancelRequest(TCallContextPtr callContext, fuse_req_t req)
{
    NFuse::CancelRequest(
        Log,
        *RequestStats,
        *callContext,
        req);

    // notifying CompletionQueue about request completion to decrement inflight
    // request counter and unblock the stopping procedure
    CompletionQueue->Complete(req, [&] (fuse_req_t) { return 0; });
}

void TFileSystem::CompleteAsyncDestroyHandle(
    TCallContext& callContext,
    const NProto::TDestroyHandleResponse& response)
{
    const auto& error = response.GetError();
    RequestStats->RequestCompleted(callContext, error);

    // If destroy request failed, we need to retry it.
    // Otherwise, remove it from queue.
    if (HasError(error)) {
        STORAGE_ERROR(
            "DestroyHandle request failed: "
            << "filesystem " << Config->GetFileSystemId()
            << " error: " << FormatError(error));
        if (GetErrorKind(error) != EErrorKind::ErrorRetriable) {
            ReportAsyncDestroyHandleFailed();
            with_lock (HandleOpsQueueLock) {
                HandleOpsQueue->Pop();
            }
        }
    } else {
        with_lock (HandleOpsQueueLock) {
            HandleOpsQueue->Pop();
        }
        with_lock (DelayedReleaseQueueLock) {
            if (!DelayedReleaseQueue.empty()) {
                const auto& nextRequest = DelayedReleaseQueue.front();
                if (ProcessAsyncRelease(
                        nextRequest.CallContext,
                        nextRequest.Req,
                        nextRequest.Ino,
                        nextRequest.Fh))
                {
                    DelayedReleaseQueue.pop();
                }
            }
        }
    }
    ScheduleProcessHandleOpsQueue();
}

void TFileSystem::ProcessHandleOpsQueue()
{
    TGuard g{HandleOpsQueueLock};
    if (HandleOpsQueue->Empty()) {
        ScheduleProcessHandleOpsQueue();
        return;
    }

    const auto optionalEntry = HandleOpsQueue->Front();
    if (!optionalEntry) {
        ReportHandleOpsQueueProcessError(
            TStringBuilder()
            << "Failed to get TQueueEntry from queue, filesystem: "
            << Config->GetFileSystemId());
        HandleOpsQueue->Pop();
        ScheduleProcessHandleOpsQueue();
        return;
    }

    const auto& entry = optionalEntry.value();
    if (entry.HasDestroyHandleRequest()) {
        const auto& requestInfo = entry.GetDestroyHandleRequest();
        auto request = std::make_shared<NProto::TDestroyHandleRequest>(
            requestInfo);
        STORAGE_DEBUG(
            "Process destroy handle request: "
            << "filesystem " << Config->GetFileSystemId() << " #"
            << requestInfo.GetNodeId() << " @" << requestInfo.GetHandle());

        auto callContext = MakeIntrusive<TCallContext>(
            Config->GetFileSystemId(),
            CreateRequestId());
        callContext->RequestType = EFileStoreRequest::DestroyHandle;
        RequestStats->RequestStarted(Log, *callContext);

        Session->DestroyHandle(callContext, std::move(request))
            .Subscribe(
                [ptr = weak_from_this(), callContext](const auto& future)
                {
                    const auto& response = future.GetValue();
                    if (auto self = ptr.lock()) {
                        self->CompleteAsyncDestroyHandle(*callContext, response);
                    }
                });
    } else {
        // TODO(#1541): process create handle
        ReportHandleOpsQueueProcessError(
            TStringBuilder() << "Unexpected TQueueEntry in queue, filesystem: "
                             << Config->GetFileSystemId());
        HandleOpsQueue->Pop();
        ScheduleProcessHandleOpsQueue();
        return;
    }

}

}   // namespace NCloud::NFileStore::NFuse
