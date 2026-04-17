#pragma once

#include "client.h"
#include "server.h"

#include <cloud/blockstore/config/rdma_common.pb.h>

#include <util/system/yassert.h>

#include <memory>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

inline EWaitMode ConvertRdmaWaitMode(NProto::EWaitMode mode)
{
    switch (mode) {
        case NProto::WAIT_MODE_POLL:
            return EWaitMode::Poll;

        case NProto::WAIT_MODE_BUSY_WAIT:
            return EWaitMode::BusyWait;

        case NProto::WAIT_MODE_ADAPTIVE_WAIT:
            return EWaitMode::AdaptiveWait;

        default:
            Y_ABORT("unsupported wait mode %d", mode);
    }
}

inline TClientConfig CreateClientConfig(const NProto::TRdmaClient& config)
{
    TClientConfig result;
    result.SendQueueSize = 0;
    result.RecvQueueSize = 0;

#define SET(param, ...)                            \
    if (const auto& value = config.Get##param()) { \
        result.param = __VA_ARGS__(value);         \
    }

#define SET_NESTED(param1, param2, ...)                           \
    if (const auto& value = config.Get##param1().Get##param2()) { \
        result.param1.param2 = __VA_ARGS__(value);                \
    }

    SET(QueueSize);
    SET(MaxBufferSize);
    SET(WaitMode, ConvertRdmaWaitMode);
    SET(PollerThreads);
    SET(MaxReconnectDelay, TDuration::MilliSeconds);
    SET(MaxResponseDelay, TDuration::MilliSeconds);
    SET(AdaptiveWaitSleepDelay, TDuration::MicroSeconds);
    SET(AdaptiveWaitSleepDuration, TDuration::MicroSeconds);
    SET(AlignedDataEnabled);
    SET(IpTypeOfService);
    SET(SourceInterface);
    SET(VerbsQP);
    SET(SendQueueSize);
    SET(RecvQueueSize);

    SET_NESTED(BufferPool, ChunkSize);
    SET_NESTED(BufferPool, MaxChunkAlloc);
    SET_NESTED(BufferPool, MaxFreeChunks);

    // Compatibility with the old config.
    if (result.SendQueueSize == 0 && result.QueueSize > 0) {
        result.SendQueueSize = result.QueueSize;
    }
    if (result.RecvQueueSize == 0 && result.QueueSize > 0) {
        result.RecvQueueSize = result.QueueSize;
    }

#undef SET_NESTED
#undef SET

    return result;
}

inline TClientConfigPtr CreateClientConfigPtr(const NProto::TRdmaClient& config)
{
    return std::make_shared<TClientConfig>(CreateClientConfig(config));
}

inline TServerConfig CreateServerConfig(const NProto::TRdmaServer& config)
{
    TServerConfig result;
    result.SendQueueSize = 0;
    result.RecvQueueSize = 0;

#define SET(param, ...)                            \
    if (const auto& value = config.Get##param()) { \
        result.param = __VA_ARGS__(value);         \
    }

#define SET_NESTED(param1, param2, ...)                           \
    if (const auto& value = config.Get##param1().Get##param2()) { \
        result.param1.param2 = __VA_ARGS__(value);                \
    }

    SET(Backlog);
    SET(QueueSize);
    SET(MaxBufferSize);
    SET(KeepAliveTimeout, TDuration::MilliSeconds);
    SET(WaitMode, ConvertRdmaWaitMode);
    SET(PollerThreads);
    SET(MaxInflightBytes);
    SET(AdaptiveWaitSleepDelay, TDuration::MicroSeconds);
    SET(AdaptiveWaitSleepDuration, TDuration::MicroSeconds);
    SET(IpTypeOfService);
    SET(SourceInterface);
    SET(VerbsQP);
    SET(SendQueueSize);
    SET(RecvQueueSize);

    SET_NESTED(BufferPool, ChunkSize);
    SET_NESTED(BufferPool, MaxChunkAlloc);
    SET_NESTED(BufferPool, MaxFreeChunks);

    // Compatibility with old config.
    if (result.SendQueueSize == 0 && result.QueueSize > 0) {
        result.SendQueueSize = result.QueueSize;
    }
    if (result.RecvQueueSize == 0 && result.QueueSize > 0) {
        result.RecvQueueSize = result.QueueSize;
    }

#undef SET_NESTED
#undef SET

    return result;
}

inline TServerConfigPtr CreateServerConfigPtr(const NProto::TRdmaServer& config)
{
    return std::make_shared<TServerConfig>(CreateServerConfig(config));
}

}   // namespace NCloud::NBlockStore::NRdma
