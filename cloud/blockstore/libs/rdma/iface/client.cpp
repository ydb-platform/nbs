#include "client.h"

#include <cloud/blockstore/config/rdma.pb.h>

#include <library/cpp/monlib/service/pages/templates.h>

namespace NCloud::NBlockStore::NRdma {

namespace {

////////////////////////////////////////////////////////////////////////////////

NRdma::EWaitMode Convert(NProto::EWaitMode mode)
{
    switch (mode) {
        case NProto::WAIT_MODE_POLL:
            return NRdma::EWaitMode::Poll;

        case NProto::WAIT_MODE_BUSY_WAIT:
            return NRdma::EWaitMode::BusyWait;

        case NProto::WAIT_MODE_ADAPTIVE_WAIT:
            return NRdma::EWaitMode::AdaptiveWait;;

        default:
            Y_ABORT("unsupported wait mode %d", mode);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TClientConfig::TClientConfig()
{
    // Compatibility with the old config.
    if (SendQueueSize == 0 && QueueSize > 0) {
        SendQueueSize = QueueSize;
    }
    if (RecvQueueSize == 0 && QueueSize > 0) {
        RecvQueueSize = QueueSize;
    }
}

#define SET(param, ...) \
    if (const auto& value = config.Get##param()) { \
        param = __VA_ARGS__(value); \
    }

#define SET_NESTED(param1, param2, ...) \
    if (const auto& value = config.Get##param1().Get##param2()) { \
        param1.param2 = __VA_ARGS__(value); \
    }

TClientConfig::TClientConfig(const NProto::TRdmaClient& config)
{
    SET(QueueSize);
    SET(MaxBufferSize);
    SET(WaitMode, Convert);
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
    if (SendQueueSize == 0 && QueueSize > 0) {
        SendQueueSize = QueueSize;
    }
    if (RecvQueueSize == 0 && QueueSize > 0) {
        RecvQueueSize = QueueSize;
    }
}

#undef SET_NESTED
#undef SET

void TClientConfig::Validate(TLog& log)
{
    BufferPool.Validate(log);
}

void TClientConfig::DumpHtml(IOutputStream& out) const
{
#define ENTRY(name, val, ...)                   \
    TABLER() {                                  \
        TABLED() { out << #name; }              \
        TABLED() { out << val; }                \
    }

    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                ENTRY(QueueSize, QueueSize);
                ENTRY(MaxBufferSize, MaxBufferSize);
                ENTRY(WaitMode, WaitMode);
                ENTRY(PollerThreads, PollerThreads);
                ENTRY(MaxReconnectDelay, MaxReconnectDelay.ToString());
                ENTRY(MaxResponseDelay, MaxResponseDelay.ToString());
                ENTRY(AdaptiveWaitSleepDelay, AdaptiveWaitSleepDelay.ToString());
                ENTRY(AdaptiveWaitSleepDuration, AdaptiveWaitSleepDuration.ToString());
                ENTRY(AlignedDataEnabled, AlignedDataEnabled);
                ENTRY(IpTypeOfService, IpTypeOfService);
                ENTRY(SourceInterface, SourceInterface);
                ENTRY(VerbsQP, VerbsQP);
                ENTRY(SendQueueSize, SendQueueSize);
                ENTRY(RecvQueueSize, RecvQueueSize);
                ENTRY(BufferPool.ChunkSize, BufferPool.ChunkSize);
                ENTRY(BufferPool.MaxChunkAlloc, BufferPool.MaxChunkAlloc);
                ENTRY(BufferPool.MaxFreeChunks, BufferPool.MaxFreeChunks);
            }
        }
    }
#undef ENTRY
}

}   // namespace NCloud::NBlockStore::NRdma

////////////////////////////////////////////////////////////////////////////////

template <>
inline void Out<NCloud::NBlockStore::NRdma::EWaitMode>(
    IOutputStream& o,
    const NCloud::NBlockStore::NRdma::EWaitMode mode)
{
    switch (mode) {
    case NCloud::NBlockStore::NRdma::EWaitMode::Poll:
        o << "POLL";
        break;

    case NCloud::NBlockStore::NRdma::EWaitMode::BusyWait:
        o << "BUSY_WAIT";
        break;

    case NCloud::NBlockStore::NRdma::EWaitMode::AdaptiveWait:
        o << "ADAPTIVE_WAIT";
        break;
    }
}
