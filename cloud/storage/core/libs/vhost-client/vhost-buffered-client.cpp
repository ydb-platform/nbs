#include "vhost-buffered-client.h"

#include <util/generic/scope.h>

namespace NVHost {

////////////////////////////////////////////////////////////////////////////////

TBufferedClient::TBufferedClient(TString sockPath, TBufferedClientParams params)
    : QueueBufferSize {params.QueueBufferSize}
    , WriteTimeout(params.WriteTimeout)
    , Impl {std::move(sockPath), TClientParams {
        .QueueCount = params.QueueCount,
        .QueueSize = params.QueueSize,
        .MemorySize = QueueBufferSize * params.QueueSize
    }}
{}

ui64 TBufferedClient::GetBufferSize() const
{
    return QueueBufferSize;
}

bool TBufferedClient::Init()
{
    if (!Impl.Init()) {
        return false;
    }

    Allocator.emplace(Impl.GetMemory(), QueueBufferSize);

    return true;
}

void TBufferedClient::DeInit()
{
    Impl.DeInit();
}

bool TBufferedClient::Write(const TVector<char>& inBuffer)
{
    TVector<char> outBuffer;
    return Write(inBuffer, outBuffer);
}

bool TBufferedClient::Write(
    const TVector<char>& inBuffer,
    TVector<char>& outBuffer)
{
    TVector<TVector<char>> outBuffers {std::move(outBuffer)};

    const bool ok = Write({ inBuffer }, outBuffers);

    outBuffer = std::move(outBuffers[0]);

    return ok;
}

bool TBufferedClient::Write(
    const TVector<TVector<char>>& inBuffers,
    TVector<TVector<char>>& outBuffers)
{
    TSgList stagingInBuffers;
    TSgList stagingOutBuffers;

    Y_DEFER {
        for (std::span buf: stagingInBuffers) {
            Allocator->Deallocate(buf);
        }
        for (std::span buf: stagingOutBuffers) {
            Allocator->Deallocate(buf);
        }
    };

    stagingInBuffers.reserve(inBuffers.size());
    for (auto& in: inBuffers) {
        std::span buf = Allocator->Allocate(in.size());
        if (buf.empty()) {
            return false;
        }
        std::memcpy(buf.data(), in.data(), in.size());

        stagingInBuffers.push_back(buf);
    }

    size_t totalLen = 0;
    stagingOutBuffers.reserve(outBuffers.size());
    for (auto& out: outBuffers) {
        std::span buf = Allocator->Allocate(out.size());
        if (buf.empty()) {
            return false;
        }
        stagingOutBuffers.push_back(buf);

        totalLen += out.size();
    }

    const auto future = Impl.WriteAsync(
        0,
        stagingInBuffers,
        stagingOutBuffers);

    size_t n = 0;
    if (WriteTimeout) {
        n = future.GetValue(WriteTimeout);
    } else {
        n = future.GetValueSync();
    }

    for (size_t i = 0; i != stagingOutBuffers.size(); ++i) {
        std::memcpy(
            outBuffers[i].data(),
            stagingOutBuffers[i].data(),
            stagingOutBuffers[i].size());
    }
    return 0 < n && n <= totalLen;
}

} // namespace NVHost
