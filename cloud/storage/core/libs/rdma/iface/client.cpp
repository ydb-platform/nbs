#include "client.h"

#include <cloud/storage/config/rdma.pb.h>

#include <library/cpp/monlib/service/pages/templates.h>

namespace NCloud::NBlockStore::NRdma {

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
