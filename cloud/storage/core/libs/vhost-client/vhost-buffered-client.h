#pragma once

#include "chunked_allocator.h"
#include "vhost-client.h"

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/mutex.h>

#include <memory>
#include <mutex>
#include <optional>

#include <span>

namespace NVHost {

////////////////////////////////////////////////////////////////////////////////

struct TBufferedClientParams
{
    ui32 QueueCount = 2;
    ui32 QueueSize = 128;
    ui64 QueueBufferSize = 8192;
    TDuration WriteTimeout;
};

class TBufferedClient
{
private:
    const ui64 QueueBufferSize;
    const TDuration WriteTimeout;

    std::optional<TChunkedAllocator> Allocator;

    TClient Impl;

public:
    explicit TBufferedClient(TString sockPath, TBufferedClientParams params = {});

    TBufferedClient(const TBufferedClient&) = delete;
    TBufferedClient& operator = (const TBufferedClient&) = delete;

    TBufferedClient(TBufferedClient&&) = default;
    TBufferedClient& operator = (TBufferedClient&&) = default;

    ui64 GetBufferSize() const;

    bool Init();
    void DeInit();

    bool Write(const TVector<char>& inBuffer);
    bool Write(const TVector<char>& inBuffer, TVector<char>& outBuffer);
    bool Write(
        const TVector<TVector<char>>& inBuffers,
        TVector<TVector<char>>& outBuffers);
};

} // namespace NVHost
