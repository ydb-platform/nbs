#pragma once

#include "stats.h"

#include <cloud/blockstore/libs/encryption/encryptor.h>
#include <cloud/contrib/vhost/include/vhost/blockdev.h>
#include <cloud/contrib/vhost/include/vhost/server.h>
#include <cloud/contrib/vhost/include/vhost/types.h>
#include <cloud/contrib/vhost/platform.h>

#include <util/generic/vector.h>
#include <util/system/file.h>

#include <libaio.h>
#include <sys/uio.h>   // iovec

#include <atomic>

class TLog;

namespace NCloud::NBlockStore::NVHostServer {

////////////////////////////////////////////////////////////////////////////////

struct TAioRequest;
struct TAioSubRequest;
struct TAioCompoundRequest;

// The unique_ptr<> deleter that performs release via std std::free(). It used
// for memory blocks allocated via std::calloc().
struct TFreeDeleter
{
    void operator()(void* obj);
};

// The unique_ptr<> deleter for TAioRequest.
struct TAioRequestDeleter
{
    void operator()(TAioRequest* obj);
};

using TAioRequestHolder = std::unique_ptr<TAioRequest, TAioRequestDeleter>;
using TAioSubRequestHolder = std::unique_ptr<TAioSubRequest, TFreeDeleter>;
using TAioCompoundRequestHolder = std::unique_ptr<TAioCompoundRequest>;

////////////////////////////////////////////////////////////////////////////////

struct TAioDevice
{
    ui64 StartOffset = 0;
    ui64 EndOffset = 0;
    TFileHandle File;
    ui64 FileOffset = 0;
    ui32 BlockSize = 0;
    bool NullBackend = false;
};

// Single IO request. Also map libvhost's vhd_buffer to iovec.
// The memory for these objects is allocated via std::calloc.
// The size of the allocated memory is enough so that the Data can keep all the
// request buffers. Therefore, Data is the last field in the class.
struct TAioRequest
    : iocb
{
    vhd_io* Io;
    TCpuCycles SubmitTs;
    bool BufferAllocated = false;
    bool Unaligned = false;
    iovec Data[ /* Bio->sglist.nbuffers */ ];

    static TAioRequestHolder CreateNew(
        size_t bufferCount,
        size_t allocatedBufferSize,
        ui32 blockSize,
        vhd_io* io,
        TCpuCycles submitTs);
    static TAioRequestHolder FromIocb(iocb* cb);

private:
    TAioRequest(
        size_t allocatedBufferSize,
        ui32 blockSize,
        vhd_io* io,
        TCpuCycles submitTs);
};

// Cross-device sub IO request.
// The memory for these objects is allocated via std::calloc.
struct TAioSubRequest: public iocb
{
    static TAioSubRequestHolder CreateNew();
    static TAioSubRequestHolder FromIocb(iocb* cb);

    [[nodiscard]] TAioCompoundRequest* GetParentRequest() const;
    [[nodiscard]] TAioCompoundRequestHolder TakeParentRequest();

private:
    TAioSubRequest() = default;;
};

// Cross-device request shared info.
struct TAioCompoundRequest
{
    std::atomic<ui32> Inflight;
    std::atomic<ui32> Errors;
    vhd_io* Io;
    TCpuCycles SubmitTs;
    std::unique_ptr<char, TFreeDeleter> Buffer;

    TAioCompoundRequest(
        ui32 inflight,
        ui32 blockSize,
        vhd_io* io,
        size_t bufferSize,
        TCpuCycles submitTs);

    static TAioCompoundRequestHolder CreateNew(
        ui32 inflight,
        ui32 blockSize,
        vhd_io* io,
        size_t bufferSize,
        TCpuCycles submitTs);
};

////////////////////////////////////////////////////////////////////////////////

void PrepareIO(
    TLog& log,
    IEncryptor* encryptor,
    const TVector<TAioDevice>& devices,
    vhd_io* io,
    TVector<iocb*>& batch,
    TCpuCycles now,
    TSimpleStats& queueStats);

// Copies the data, and if an encryptor is specified, encrypt it. Returns true
// if successful.
[[nodiscard]] bool SgListCopyWithOptionalEncryption(
    TLog& Log,
    const vhd_sglist& src,
    char* dst,
    IEncryptor* encryptor,
    ui64 startSector);

// Copies the data, and if an encryptor is specified, decrypt it. Returns true
// if successful.
[[nodiscard]] bool SgListCopyWithOptionalDecryption(
    TLog& Log,
    const char* src,
    const vhd_sglist& dst,
    IEncryptor* encryptor,
    ui64 startSector);

}   // namespace NCloud::NBlockStore::NVHostServer
