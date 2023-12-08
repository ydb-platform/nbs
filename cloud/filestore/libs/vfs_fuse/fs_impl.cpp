#include "fs_impl.h"

namespace NCloud::NFileStore::NFuse {

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
        ICompletionQueuePtr queue)
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
{
    Log = Logging->CreateLog("NFS_FUSE");
}

TFileSystem::~TFileSystem()
{
    Reset();
}

void TFileSystem::Reset()
{
    STORAGE_INFO("resetting filesystem cache");
    ClearDirectoryCache();
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

        callContext.Error = error;

        ReplyError(
            callContext,
            req,
            ErrnoFromError(error.GetCode()));
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
        ReplyError(callContext, req, EINVAL);
        return false;
    }
    return true;
}

bool TFileSystem::UpdateNodesCache(
    const NProto::TNodeAttr& attrs,
    fuse_entry_param& entry)
{
    if (attrs.GetId() == InvalidNodeId) {
        return false;
    }

    with_lock (CacheLock) {
        auto* node = Cache.TryAddNode(attrs);
        Y_ABORT_UNLESS(node);

        entry.ino = attrs.GetId();
        entry.generation = Cache.Generation();
        entry.attr_timeout = Config->GetAttrTimeout().Seconds();
        entry.entry_timeout = Config->GetEntryTimeout().Seconds();

        ConvertAttr(Config->GetBlockSize(), node->Attrs, entry.attr);
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
    TGuard g{XAttrLock};
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
    fuse_req_t req,
    ui64 handle,
    const NProto::TNodeAttr& attrs)
{
    STORAGE_TRACE("inserting node: " << DumpMessage(attrs));

    fuse_entry_param entry = {};
    if (!UpdateNodesCache(attrs, entry)) {
        ReplyError(callContext, req, EIO);
        return;
    }

    fuse_file_info fi = {};
    fi.fh = handle;

    int res = ReplyCreate(
        callContext,
        req,
        &entry,
        &fi);
    if (res == -ENOENT) {
        // syscall was interrupted
        with_lock (CacheLock) {
            Cache.ForgetNode(entry.ino, 1);
        }
    }
}

void TFileSystem::ReplyEntry(
    TCallContext& callContext,
    fuse_req_t req,
    const NProto::TNodeAttr& attrs)
{
    STORAGE_TRACE("inserting node: " << DumpMessage(attrs));

    fuse_entry_param entry = {};
    if (!UpdateNodesCache(attrs, entry)) {
        ReplyError(callContext, req, EIO);
        return;
    }

    int res = ReplyEntry(
        callContext,
        req,
        &entry);
    if (res == -ENOENT) {
        // syscall was interrupted
        with_lock (CacheLock) {
            Cache.ForgetNode(entry.ino, 1);
        }
    }
}

void TFileSystem::ReplyXAttrInt(
    TCallContext& callContext,
    fuse_req_t req,
    const TString& value,
    size_t size)
{
    if (size >= value.size()) {
        ReplyBuf(
            callContext,
            req,
            value.data(),
            value.size());
    } else if (!size) {
        ReplyXAttr(
            callContext,
            req,
            value.size());
    } else {
        ReplyError(
            callContext,
            req,
            ERANGE);
    }
}

void TFileSystem::ReplyAttr(
    TCallContext& callContext,
    fuse_req_t req,
    const NProto::TNodeAttr& attrs)
{
    fuse_entry_param entry = {};
    if (!UpdateNodesCache(attrs, entry)) {
        ReplyError(callContext, req, EIO);
        return;
    }

    ReplyAttr(
        callContext,
        req,
        &entry.attr,
        Config->GetAttrTimeout().Seconds());
}

}   // namespace NCloud::NFileStore::NFuse
