#pragma once

#include "fs_impl.h"

#include "context.h"
#include "vfs.h"

#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/vfs/convert.h>

#include <cloud/storage/core/libs/common/sglist_iter.h>

#include <contrib/libs/linux-headers/linux/fuse.h>

namespace NCloud::NFileStore::NVFSVhost {

namespace NImpl {

////////////////////////////////////////////////////////////////////////////////

inline void Reply(const TVfsRequestContextPtr& ctx, TSgListOutputIterator& out, int error)
{
    fuse_out_header header = {
        .error = error,
        .unique = ctx->CallContext->RequestId,
    };

    // Should be validated before hand
    Y_ABORT_UNLESS(out.Write(header));
}

inline ELogPriority GetErrorPriority(ui32 code)
{
    if (FACILITY_FROM_CODE(code) == FACILITY_FILESTORE) {
        return TLOG_DEBUG;
    } else {
        return TLOG_ERR;
    }
}

}   // NImpl

////////////////////////////////////////////////////////////////////////////////

inline void TFileSystem::ReplyOk(const TVfsRequestContextPtr& ctx)
{
    auto guard = ctx->VfsRequest->Out.Acquire();
    if (!guard) {
        return;
    }

    TSgListOutputIterator out(guard.Get());
    NImpl::Reply(ctx, out, 0);
}

inline void TFileSystem::ReplyError(const TVfsRequestContextPtr& ctx, int error)
{
    auto guard = ctx->VfsRequest->Out.Acquire();
    if (!guard) {
        return;
    }

    TSgListOutputIterator out(guard.Get());
    return NImpl::Reply(ctx, out, -error);
}

inline NProto::TError TFileSystem::ReplyEntry(
    const TVfsRequestContextPtr& ctx,
    const NProto::TNodeAttr& attrs)
{
    auto guard = ctx->VfsRequest->Out.Acquire();
    if (!guard) {
        return ErrorInvalidState();
    }

    TSgListOutputIterator out(guard.Get());
    NImpl::Reply(ctx, out, 0);


    fuse_entry_out entry = {};
    entry.nodeid = attrs.GetId();
    entry.attr_valid = FsConfig->GetAttrTimeout().Seconds();
    entry.entry_valid = FsConfig->GetEntryTimeout().Seconds();
    // TODO: pass session generation
    entry.generation = 1;
    NVFS::ConvertAttr(FsConfig->GetBlockSize(), attrs, entry.attr);

    if (!out.Write(entry)) {
        return ErrorIO();
    }

    return {};
}

template<typename... Args>
NProto::TError TFileSystem::Reply(const TVfsRequestContextPtr& ctx, const Args&... args)
{
    auto guard = ctx->VfsRequest->Out.Acquire();
    if (!guard) {
        return ErrorInvalidState();
    }

    TSgListOutputIterator out(guard.Get());
    NImpl::Reply(ctx, out, 0);

    if (!out.WriteAll(args...)) {
        return ErrorIO();
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

inline bool TFileSystem::CheckFailed(const TVfsRequestContextPtr& ctx, const NProto::TError& error)
{
    if (HasError(error)) {
        STORAGE_LOG(NImpl::GetErrorPriority(error.GetCode()),
            "request #" << ctx->CallContext->RequestId
            << " failed: " << FormatError(error));

        ctx->CallContext->Error = error;

        ReplyError(
            ctx,
            NVFS::ErrnoFromError(error.GetCode()));

        return true;
    }

    return false;
}

}   // namespace NCloud::NFileStore::NVFSVhost
