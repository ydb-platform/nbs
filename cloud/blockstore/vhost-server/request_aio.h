#pragma once

#include "stats.h"

#include <cloud/contrib/vhost/include/vhost/blockdev.h>
#include <cloud/contrib/vhost/include/vhost/server.h>
#include <cloud/contrib/vhost/include/vhost/types.h>
#include <cloud/contrib/vhost/platform.h>

#include <util/generic/vector.h>
#include <util/system/file.h>

#include <atomic>

#include <libaio.h>
#include <sys/uio.h>    // iovec

class TLog;

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

struct TAioDevice
{
    i64 StartOffset = 0;
    i64 EndOffset = 0;
    TFileHandle File;
    i64 FileOffset = 0;
};

// Single IO request. Also map libvhost's vhd_buffer to iovec.
struct TAioRequest
    : iocb
{
    vhd_io* Io;
    TCpuCycles SubmitTs;
    bool BounceBuf;
    iovec Data[ /* Bio->sglist.nbuffers */ ];
};

// Compound IO request.
struct TAioCompoundRequest
{
    std::atomic<int> Inflight;
    std::atomic<int> Errors;
    vhd_io* Io;
    TCpuCycles SubmitTs;
    char* Buffer;
};

////////////////////////////////////////////////////////////////////////////////

void PrepareIO(
    TLog& log,
    const TVector<TAioDevice>& devices,
    vhd_io* io,
    TVector<iocb*>& batch,
    TCpuCycles now);

void SgListCopy(const vhd_sglist& src, char* dst);
void SgListCopy(const char* src, const vhd_sglist& dst);

}   // namespace NCloud::NBlockStore::NVHostServer
