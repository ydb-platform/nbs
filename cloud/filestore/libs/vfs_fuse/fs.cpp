#include "fs.h"

#include "fs_impl.h"

#include <cloud/filestore/libs/vfs/probes.h>
#include <cloud/storage/core/libs/common/helpers.h>

namespace NCloud::NFileStore::NFuse {

LWTRACE_USING(FILESTORE_VFS_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

int ReplyNone(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req)
{
    requestStats.ResponseSent(callContext);
    FILESTORE_TRACK(ResponseSent, (&callContext), "None");

    fuse_reply_none(req);

    requestStats.RequestCompleted(log, callContext, error);

    const ui64 now = GetCycleCount();
    const auto ts = callContext.CalcRequestTime(now);
    FILESTORE_TRACK(
        RequestCompleted,
        (&callContext),
        "None",
        ts.TotalTime.MicroSeconds(),
        ts.ExecutionTime.MicroSeconds(),
        0);

    return 0;
}

int ReplyError(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    int errorCode)
{
    requestStats.ResponseSent(callContext);
    FILESTORE_TRACK(ResponseSent, (&callContext), "Error");

    int res = fuse_reply_err(req, errorCode);

    requestStats.RequestCompleted(log, callContext, error);

    const ui64 now = GetCycleCount();
    const auto ts = callContext.CalcRequestTime(now);
    FILESTORE_TRACK(
        RequestCompletedError,
        (&callContext),
        "Error",
        ts.TotalTime.MicroSeconds(),
        ts.ExecutionTime.MicroSeconds(),
        errorCode,
        res);

    return res;
}

int ReplyEntry(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const fuse_entry_param *e)
{
    requestStats.ResponseSent(callContext);
    FILESTORE_TRACK(ResponseSent, (&callContext), "Entry");

    int res = fuse_reply_entry(req, e);

    requestStats.RequestCompleted(log, callContext, error);

    const ui64 now = GetCycleCount();
    const auto ts = callContext.CalcRequestTime(now);
    FILESTORE_TRACK(
        RequestCompleted,
        (&callContext),
        "Entry",
        ts.TotalTime.MicroSeconds(),
        ts.ExecutionTime.MicroSeconds(),
        res);

    return res;
}

int ReplyCreate(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const fuse_entry_param *e,
    const fuse_file_info *fi)
{
    requestStats.ResponseSent(callContext);
    FILESTORE_TRACK(ResponseSent, (&callContext), "Create");

    int res = fuse_reply_create(req, e, fi);

    requestStats.RequestCompleted(log, callContext, error);

    const ui64 now = GetCycleCount();
    const auto ts = callContext.CalcRequestTime(now);
    FILESTORE_TRACK(
        RequestCompleted,
        (&callContext),
        "Create",
        ts.TotalTime.MicroSeconds(),
        ts.ExecutionTime.MicroSeconds(),
        res);

    return res;
}

int ReplyAttr(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const struct stat *attr,
    double attr_timeout)
{
    requestStats.ResponseSent(callContext);
    FILESTORE_TRACK(ResponseSent, (&callContext), "Attr");

    int res = fuse_reply_attr(req, attr, attr_timeout);

    requestStats.RequestCompleted(log, callContext, error);

    const ui64 now = GetCycleCount();
    const auto ts = callContext.CalcRequestTime(now);
    FILESTORE_TRACK(
        RequestCompleted,
        (&callContext),
        "Attr",
        ts.TotalTime.MicroSeconds(),
        ts.ExecutionTime.MicroSeconds(),
        res);

    return res;
}

int ReplyReadLink(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const char *link)
{
    FILESTORE_TRACK(ResponseSent, (&callContext), "ReadLink");
    requestStats.ResponseSent(callContext);
    int res = fuse_reply_readlink(req, link);

    requestStats.RequestCompleted(log, callContext, error);

    const ui64 now = GetCycleCount();
    const auto ts = callContext.CalcRequestTime(now);
    FILESTORE_TRACK(
        RequestCompleted,
        (&callContext),
        "ReadLink",
        ts.TotalTime.MicroSeconds(),
        ts.ExecutionTime.MicroSeconds(),
        res);

    return res;
}

int ReplyOpen(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const fuse_file_info *fi)
{
    FILESTORE_TRACK(ResponseSent, (&callContext), "Open");
    requestStats.ResponseSent(callContext);

    int res = fuse_reply_open(req, fi);

    requestStats.RequestCompleted(log, callContext, error);

    const ui64 now = GetCycleCount();
    const auto ts = callContext.CalcRequestTime(now);
    FILESTORE_TRACK(
        RequestCompleted,
        (&callContext),
        "Open",
        ts.TotalTime.MicroSeconds(),
        ts.ExecutionTime.MicroSeconds(),
        res);

    return res;
}

int ReplyWrite(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    size_t count)
{
    requestStats.ResponseSent(callContext);
    FILESTORE_TRACK(ResponseSent, (&callContext), "Write");

    int res = fuse_reply_write(req, count);

    requestStats.RequestCompleted(log, callContext, error);

    const ui64 now = GetCycleCount();
    const auto ts = callContext.CalcRequestTime(now);
    FILESTORE_TRACK(
        RequestCompleted,
        (&callContext),
        "Write",
        ts.TotalTime.MicroSeconds(),
        ts.ExecutionTime.MicroSeconds(),
        res);

    return res;
}

int ReplyBuf(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const char *buf,
    size_t size)
{
    requestStats.ResponseSent(callContext);
    FILESTORE_TRACK(ResponseSent, (&callContext), "Buf");

    int res = fuse_reply_buf(req, buf, size);

    requestStats.RequestCompleted(log, callContext, error);

    const ui64 now = GetCycleCount();
    const auto ts = callContext.CalcRequestTime(now);
    FILESTORE_TRACK(
        RequestCompleted,
        (&callContext),
        "Buf",
        ts.TotalTime.MicroSeconds(),
        ts.ExecutionTime.MicroSeconds(),
        res);

    return res;
}

int ReplyStatFs(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const struct statvfs *stbuf)
{
    requestStats.ResponseSent(callContext);
    FILESTORE_TRACK(ResponseSent, (&callContext), "StatFs");

    int res = fuse_reply_statfs(req, stbuf);

    requestStats.RequestCompleted(log, callContext, error);

    const ui64 now = GetCycleCount();
    const auto ts = callContext.CalcRequestTime(now);
    FILESTORE_TRACK(
        RequestCompleted,
        (&callContext),
        "StatFs",
        ts.TotalTime.MicroSeconds(),
        ts.ExecutionTime.MicroSeconds(),
        res);

    return res;
}

int ReplyXAttr(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    size_t count)
{
    requestStats.ResponseSent(callContext);
    FILESTORE_TRACK(ResponseSent, (&callContext), "XAttr");

    int res = fuse_reply_xattr(req, count);

    requestStats.RequestCompleted(log, callContext, error);

    const ui64 now = GetCycleCount();
    const auto ts = callContext.CalcRequestTime(now);
    FILESTORE_TRACK(
        RequestCompleted,
        (&callContext),
        "XAttr",
        ts.TotalTime.MicroSeconds(),
        ts.ExecutionTime.MicroSeconds(),
        res);

    return res;
}

int ReplyLock(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    const NCloud::NProto::TError& error,
    fuse_req_t req,
    const struct flock *lock)
{
    requestStats.ResponseSent(callContext);
    FILESTORE_TRACK(ResponseSent, (&callContext), "Lock");

    int res = fuse_reply_lock(req, lock);

    requestStats.RequestCompleted(log, callContext, error);

    const ui64 now = GetCycleCount();
    const auto ts = callContext.CalcRequestTime(now);
    FILESTORE_TRACK(
        RequestCompleted,
        (&callContext),
        "Lock",
        ts.TotalTime.MicroSeconds(),
        ts.ExecutionTime.MicroSeconds(),
        res);

    return res;
}

void CancelRequest(
    TLog& log,
    IRequestStats& requestStats,
    TCallContext& callContext,
    fuse_req_t req)
{
    FILESTORE_TRACK(ResponseSent, (&callContext), "Cancel");
    requestStats.ResponseSent(callContext);

    int res = fuse_cancel_request(
        req,
        static_cast<fuse_cancelation_code>(callContext.CancellationCode));

    ui32 flags = 0;
    SetProtoFlag(flags, NCloud::NProto::EF_SILENT);
    requestStats.RequestCompleted(
        log,
        callContext,
        MakeError(E_CANCELLED, "Driver is stopping", flags));

    const ui64 now = GetCycleCount();
    const auto ts = callContext.CalcRequestTime(now);
    FILESTORE_TRACK(
        RequestCompletedError,
        (&callContext),
        "Cancel",
        ts.TotalTime.MicroSeconds(),
        ts.ExecutionTime.MicroSeconds(),
        callContext.CancellationCode,
        res);
}

////////////////////////////////////////////////////////////////////////////////

IFileSystemPtr CreateFileSystem(
    ILoggingServicePtr logging,
    IProfileLogPtr profileLog,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    TFileSystemConfigPtr config,
    IFileStorePtr session,
    IRequestStatsPtr stats,
    ICompletionQueuePtr queue,
    THandleOpsQueuePtr handleOpsQueue,
    IFuseNotifyOpsPtr fuseNotifyOps)
{
    return std::make_shared<TFileSystem>(
        std::move(logging),
        std::move(profileLog),
        std::move(scheduler),
        std::move(timer),
        std::move(config),
        std::move(session),
        std::move(stats),
        std::move(queue),
        std::move(handleOpsQueue),
        fuseNotifyOps);
}

}   // namespace NCloud::NFileStore::NFuse
