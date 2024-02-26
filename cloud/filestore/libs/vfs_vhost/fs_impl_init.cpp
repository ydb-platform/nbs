#include "fs_impl.h"
#include "fs_impl_reply.h"

#include <cloud/filestore/libs/vfs/convert.h>

#include <util/generic/size_literals.h>

#include <sys/stat.h>
#include <sys/statvfs.h>

namespace NCloud::NFileStore::NVFSVhost {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TError> TFileSystem::Init(
    TVfsRequestContextPtr ctx,
    const struct fuse_in_header& header,
    TSgListInputIterator& in)
{
    Y_UNUSED(header);

    struct fuse_init_in init;
    if (!in.Read(init)) {
        return MakeFuture(ErrorIO());
    }

    STORAGE_INFO(LogTag <<  "got init request:\n"
        << "\tversion: " << init.major << "." << init.minor << "\n"
        << "\tmax_readahead: " << init.max_readahead << "\n"
        << "\tflogs: " << init.flags << "\n"
        << "\tflogs2: " << init.flags2);

    if (init.major < 7 || (init.major == 7 && init.minor < 6)) {
        STORAGE_ERROR("%s unsupported protocol version %u:%u",
            LogTag.c_str(),
            init.major,
            init.minor);

        ReplyError(ctx, EPROTO);
        return MakeFuture(NProto::TError{});
    }

    if (SessionState.GetProtoMajor() > 0) {
        // TODO: CritEvent or smth?
        // however possible due to restart in between reset session state & reply
        STORAGE_WARN(LogTag << "repeated init request, current state: "
            << SessionState.ShortDebugString());
    }

    ui32 want = 0;
    want |= FUSE_ASYNC_DIO;         // asynchronous direct I/O submission
    want |= FUSE_ATOMIC_O_TRUNC;    // O_TRUNC open flag in the filesystem
    want |= FUSE_AUTO_INVAL_DATA;   // automatically invalidate cached pages
    want |= FUSE_DO_READDIRPLUS;    // do READDIRPLUS (READDIR+LOOKUP in one)
    want |= FUSE_FLOCK_LOCKS;       // locking for BSD style file locks
    want |= FUSE_HANDLE_KILLPRIV;   // fs handles killing suid/sgid/cap on write/chown/trunc
    want |= FUSE_MAX_PAGES;         // init_out.max_pages contains the max number of req pages
    want |= FUSE_POSIX_LOCKS;       // locking for POSIX style file locks
    want |= FUSE_READDIRPLUS_AUTO;  // adaptive readdirplus
    want |= FUSE_ASYNC_READ;        // we can handle concurrent requests
    want |= FUSE_BIG_WRITES;        // we can write more than 4kb at once
    want |= FUSE_CACHE_SYMLINKS;    // caching READLINK is good for us
    want |= FUSE_PARALLEL_DIROPS;   // we can handle parallel requests
    // TODO: consider no need in OPEDIR/RELEASE dir, cookie is enough
    // want |= FUSE_NO_OPENDIR_SUPPORT;

    // extra size for header
    struct fuse_init_out out = {};
    out.major = init.major;
    out.minor = init.minor;
    out.flags = want & init.flags;
    out.max_pages = Config->GetMaxWritePages() + 1;
    out.max_background = Config->GetMaxBackground();
    out.congestion_threshold = Config->GetMaxBackground();
    out.max_readahead = init.max_readahead;
    // TODO: consider moving to config (max size + header)
    out.max_write = 4_MB + 4_KB;

    if (want & (~init.flags)) {
        STORAGE_WARN("%s kernel does not support some expected features: 0x%x",
            LogTag.c_str(),
            want & (~init.flags));
    }

    SessionState.SetProtoMajor(init.major);
    SessionState.SetProtoMinor(init.minor);
    SessionState.SetCapable(init.flags);
    SessionState.SetWant(want);
    SessionState.SetBufferSize(out.max_write);

    return ResetSessionState(ctx->CallContext).Apply(
        [=, weak = weak_from_this()] (const auto& future) -> NProto::TError {
            auto self = weak.lock();
            if (!self) {
                return ErrorInvalidState();
            }

            auto error = future.GetValue();
            if (HasError(error)) {
                STORAGE_ERROR(LogTag << " session init failed: "
                    << FormatError(error));

                ReplyError(ctx, EPROTO);
            }

            STORAGE_INFO(LogTag << " session init completed");
            Reply(ctx, out);
            return {};
        });
}

NThreading::TFuture<NProto::TError> TFileSystem::Destroy(
    TVfsRequestContextPtr ctx,
    const struct fuse_in_header& header,
    TSgListInputIterator& in)
{
    Y_UNUSED(header);
    Y_UNUSED(in);

    STORAGE_INFO(LogTag << " got destroy request");
    SessionState.Clear();

    return ResetSessionState(ctx->CallContext).Apply(
        [=, weak = weak_from_this()] (const auto& future) -> NProto::TError {
            auto self = weak.lock();
            if (!self) {
                return ErrorInvalidState();
            }

            auto error = future.GetValue();
            STORAGE_INFO(LogTag << " destroy request completed: "
                << FormatError(error));

            ReplyOk(ctx);
            return {};
        });
}

NThreading::TFuture<NProto::TError> TFileSystem::ResetSessionState(
    const TCallContextPtr& callContext)
{
    auto request = CreateRequest<NProto::TResetSessionRequest>();
    request->SetSessionState(SessionState.SerializeAsString());

    return Session->ResetSession(callContext, std::move(request)).Apply(
        [] (const auto& future) {
            return future.GetValue().GetError();
        });
}

NThreading::TFuture<NProto::TError> TFileSystem::StatFs(
    TVfsRequestContextPtr ctx,
    const struct fuse_in_header& header,
    TSgListInputIterator& in)
{
    Y_UNUSED(in);

    STORAGE_DEBUG("StatFs #" << header.unique);

    auto request = CreateRequest<NProto::TStatFileStoreRequest>();
    return Session->StatFileStore(ctx->CallContext, std::move(request))
        .Apply([=, weak = weak_from_this()] (const auto& future) -> NProto::TError {
            const auto& response = future.GetValue();

            auto self = weak.lock();
            if (CheckFailed(self, ctx, response.GetError())) {
                return {};
            }

            const auto& info = response.GetFileStore();
            const auto& stats = response.GetStats();

            struct statvfs st = {};
            NVFS::ConvertStat(info, stats, st);

            return Reply(ctx, st);
        });
}

}   // namespace NCloud::NFileStore::NVFSVhost
