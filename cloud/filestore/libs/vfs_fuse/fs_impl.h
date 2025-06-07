#pragma once

#include "public.h"

#include "config.h"
#include "fs.h"
#include "handle_ops_queue.h"
#include "node_cache.h"

#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/vfs/config.h>
#include <cloud/filestore/libs/vfs/convert.h>
#include <cloud/filestore/libs/vfs/fsync_queue.h>
#include <cloud/filestore/libs/vfs_fuse/write_back_cache/write_back_cache.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/queue.h>
#include <util/generic/string.h>
#include <util/system/align.h>
#include <util/system/mutex.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TDirectoryHandle;

struct TRangeLock
{
    ui64 Handle = -1;
    ui64 Owner = -1;
    i32 Pid = -1;
    off_t Offset = 0;
    off_t Length = 0;
    NProto::ELockType LockType;
};

////////////////////////////////////////////////////////////////////////////////

struct TReleaseRequest
{
    TCallContextPtr CallContext;
    fuse_req_t Req;
    fuse_ino_t Ino;
    ui64 Fh;
};

////////////////////////////////////////////////////////////////////////////////

class TFileSystem final
    : public IFileSystem
    , public std::enable_shared_from_this<TFileSystem>
{
private:
    static constexpr ui64 MissingNodeId = -1;

private:
    const ILoggingServicePtr Logging;
    const IProfileLogPtr ProfileLog;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const IFileStorePtr Session;
    const TFileSystemConfigPtr Config;

    TLog Log;
    IRequestStatsPtr RequestStats;
    ICompletionQueuePtr CompletionQueue;

    NVFS::TFSyncQueue FSyncQueue;

    TNodeCache NodeCache;
    TMutex NodeCacheLock;

    THashMap<ui64, std::shared_ptr<TDirectoryHandle>> DirectoryHandles;
    TMutex DirectoryHandlesLock;

    TXAttrCache XAttrCache;
    TMutex XAttrCacheLock;

    THandleOpsQueuePtr HandleOpsQueue;
    TMutex HandleOpsQueueLock;

    TQueue<TReleaseRequest> DelayedReleaseQueue;
    TMutex DelayedReleaseQueueLock;

    TWriteBackCachePtr WriteBackCache;

public:
    TFileSystem(
        ILoggingServicePtr logging,
        IProfileLogPtr profileLog,
        ISchedulerPtr scheduler,
        ITimerPtr timer,
        TFileSystemConfigPtr config,
        IFileStorePtr session,
        IRequestStatsPtr stats,
        ICompletionQueuePtr queue,
        THandleOpsQueuePtr handleOpsQueue,
        TWriteBackCachePtr writeBackCache);

    ~TFileSystem();

    void Init() override;

    void Reset() override;

    // filesystem information
    void StatFs(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino) override;

    // nodes
    void Lookup(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t parent,
        TString name) override;
    void Forget(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        unsigned long nlookup) override;
    void ForgetMulti(
        TCallContextPtr callContext,
        fuse_req_t req,
        size_t count,
        fuse_forget_data* forgets) override;

    void MkDir(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t parent,
        TString name,
        mode_t mode) override;
    void RmDir(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t parent,
        TString name) override;
    void MkNode(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t parent,
        TString name,
        mode_t mode,
        dev_t rdev) override;
    void Unlink(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t parent,
        TString name) override;
    void Rename(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t parent,
        TString name,
        fuse_ino_t newparent,
        TString newname,
        int flags) override;
    void SymLink(
        TCallContextPtr callContext,
        fuse_req_t req,
        TString link,
        fuse_ino_t parent,
        TString name) override;
    void Link(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_ino_t newparent,
        TString newname) override;
    void ReadLink(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino) override;
    void Access(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        int mask) override;

    // node attributes
    void SetAttr(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        struct stat* attr,
        int to_set,
        fuse_file_info* fi) override;
    void GetAttr(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_file_info* fi) override;

    // extended node attributes
    void SetXAttr(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        TString name,
        TString value,
        int flags) override;
    void GetXAttr(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        TString name,
        size_t size) override;
    void ListXAttr(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        size_t size) override;
    void RemoveXAttr(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        TString name) override;

    // directory listing
    void OpenDir(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_file_info* fi) override;
    void ReadDir(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        size_t size,
        off_t offset,
        fuse_file_info* fi) override;
    void ReleaseDir(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_file_info* fi) override;

    // read & write files
    void Create(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t parent,
        TString name,
        mode_t mode,
        fuse_file_info* fi) override;
    void Open(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_file_info* fi) override;
    void Read(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        size_t size,
        off_t offset,
        fuse_file_info* fi) override;
    void Write(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        TStringBuf buffer,
        off_t offset,
        fuse_file_info* fi) override;
    void WriteBuf(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_bufvec* bufv,
        off_t offset,
        fuse_file_info* fi) override;
    void FAllocate(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        int mode,
        off_t offset,
        off_t length,
        fuse_file_info* fi) override;
    void Flush(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_file_info* fi) override;
    void FSync(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        int datasync,
        fuse_file_info* fi) override;
    void FSyncDir(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        int datasync,
        fuse_file_info* fi) override;
    void Release(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_file_info* fi) override;

    // locking
    void GetLock(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_file_info* fi,
        struct flock* lock) override;
    void SetLock(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_file_info* fi,
        struct flock* lock,
        bool sleep) override;
    void FLock(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_file_info* fi,
        int op) override;

private:
    template <typename T>
    static std::shared_ptr<T> StartRequest()
    {
        return std::make_shared<T>();
    }

    template <typename T>
    static std::shared_ptr<T> StartRequest(ui64 node)
    {
        auto request = StartRequest<T>();
        request->SetNodeId(node);

        return request;
    }

    template <typename T>
    void SetUserNGroup(T& request, const fuse_ctx* ctx)
    {
        request.SetUid(ctx->uid);
        request.SetGid(ctx->gid);
    }

    template<typename T>
    static bool CheckResponse(
        std::shared_ptr<TFileSystem> self,
        TCallContext& callContext,
        fuse_req_t req,
        const T& response
    ) {
        return self && self->CheckResponse(callContext, req, response);
    }

    template <typename T>
    bool CheckResponse(
        TCallContext& callContext,
        fuse_req_t req,
        const T& response)
    {
        return CheckError(callContext, req, response.GetError());
    }

    bool CheckError(
        TCallContext& callContext,
        fuse_req_t req,
        const NProto::TError& error);

    bool ValidateNodeId(
        TCallContext& callContext,
        fuse_req_t req,
        fuse_ino_t ino);

    bool ValidateDirectoryHandle(
        TCallContext& callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        uint64_t fh);

    bool UpdateNodeCache(
        const NProto::TNodeAttr& attrs,
        fuse_entry_param& entry);

    void UpdateXAttrCache(
        ui64 ino,
        const TString& name,
        const TString& value,
        ui64 version,
        const NProto::TError& error);

    void ReplyCreate(
        TCallContext& callContext,
        const NCloud::NProto::TError& error,
        fuse_req_t req,
        ui64 handle,
        const NProto::TNodeAttr& attrs);
    void ReplyEntry(
        TCallContext& callContext,
        const NCloud::NProto::TError& error,
        fuse_req_t req,
        const NProto::TNodeAttr& attrs);
    void ReplyXAttrInt(
        TCallContext& callContext,
        const NCloud::NProto::TError& error,
        fuse_req_t req,
        const TString& value,
        size_t size);
    void ReplyAttr(
        TCallContext& callContext,
        const NCloud::NProto::TError& error,
        fuse_req_t req,
        const NProto::TNodeAttr& attrs);

    bool ProcessAsyncRelease(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        ui64 handle);
    void ReleaseImpl(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        ui64 handle);
    void CompleteAsyncDestroyHandle(
        TCallContext& callContext,
        const NProto::TDestroyHandleResponse& response);

    void ClearDirectoryCache();

    void ScheduleProcessHandleOpsQueue();
    void ProcessHandleOpsQueue();

#define FILESYSTEM_REPLY_IMPL(name, ...)                                       \
    template<typename... TArgs>                                                \
    int Reply##name(                                                           \
        TCallContext& callContext,                                             \
        const NCloud::NProto::TError& error,                                   \
        fuse_req_t req,                                                        \
        TArgs&&... args)                                                       \
    {                                                                          \
        Y_ABORT_UNLESS(RequestStats);                                          \
        Y_ABORT_UNLESS(CompletionQueue);                                       \
        return CompletionQueue->Complete(req, [&] (fuse_req_t r) {             \
            return NFuse::Reply##name(                                         \
                Log,                                                           \
                *RequestStats,                                                 \
                callContext,                                                   \
                error,                                                         \
                r,                                                             \
                std::forward<TArgs>(args)...);                                 \
        });                                                                    \
    }                                                                          \

    FILESYSTEM_REPLY_METHOD(FILESYSTEM_REPLY_IMPL)

#undef FILESYSTEM_REPLY_IMPL

    void CancelRequest(TCallContextPtr callContext, fuse_req_t req);

    void HandleLock(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        const TRangeLock& range,
        NProto::ELockOrigin origin,
        bool sleep);

    void AcquireLock(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        const TRangeLock& range,
        bool sleep,
        NProto::ELockOrigin origin);
    void ScheduleAcquireLock(
        TCallContextPtr callContext,
        const NCloud::NProto::TError& error,
        fuse_req_t req,
        fuse_ino_t ino,
        const TRangeLock& range,
        bool sleep,
        NProto::ELockOrigin origin);
    void ReleaseLock(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        const TRangeLock& range,
        NProto::ELockOrigin origin);
    void TestLock(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        const TRangeLock& range);
    void ReadLocal(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        size_t size,
        off_t offset,
        fuse_file_info* fi);
    void WriteBufLocal(
        TCallContextPtr callContext,
        fuse_req_t req,
        fuse_ino_t ino,
        fuse_bufvec* bufv,
        off_t offset,
        fuse_file_info* fi);
};

}   // namespace NCloud::NFileStore::NFuse
