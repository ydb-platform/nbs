#pragma once

#include "public.h"

#include <util/datetime/base.h>

struct stat;
struct statvfs;

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

struct TFileStore;
struct TFileStoreStats;
struct TNodeAttr;

}   // namespace NProto

namespace NCloud::NFileStore::NVFS {

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
    return TInstant::Seconds(t.tv_sec) +
           TDuration::MicroSeconds(t.tv_nsec / 1000);
}

inline timespec ConvertTimeSpec(TInstant t)
{
    timespec time = {};
    time.tv_sec = t.Seconds();
    time.tv_nsec = t.MicroSecondsOfSecond() * 1000;
    return time;
}

}   // namespace NCloud::NFileStore::NVFS
