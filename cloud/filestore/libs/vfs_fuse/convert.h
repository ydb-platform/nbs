#pragma once

#include "public.h"

#include "fuse.h"

#include <cloud/filestore/libs/service/filestore.h>

#include <util/datetime/base.h>

struct stat;
struct statvfs;

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

int ErrnoFromError(ui32 code);

void ConvertAttr(
    ui32 blockSize,
    const NProto::TNodeAttr& attr,
    struct stat& st);
void ConvertStat(
    const NProto::TFileStore& info,
    const NProto::TFileStoreStats& stats,
    struct statvfs& st);

inline TInstant ConvertTimeSpec(const timespec& t)
{
    return TInstant::Seconds(t.tv_sec)
         + TDuration::MicroSeconds(t.tv_nsec / 1000);
}

inline timespec ConvertTimeSpec(TInstant t)
{
    timespec time = {};
    time.tv_sec = t.Seconds();
    time.tv_nsec = t.MicroSecondsOfSecond() * 1000;
    return time;
}

template <typename T>
inline void SetUserNGroup(T& request, const fuse_ctx* ctx)
{
    request.SetUid(ctx->uid);
    request.SetGid(ctx->gid);
}

}   // namespace NCloud::NFileStore::NFuse
