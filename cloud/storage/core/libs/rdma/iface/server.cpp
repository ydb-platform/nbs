#include "server.h"

#include <library/cpp/monlib/service/pages/templates.h>

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

TServerConfig::TServerConfig()
{
    // Compatibility with old config.
    if (SendQueueSize == 0 && QueueSize > 0) {
        SendQueueSize = QueueSize;
    }
    if (RecvQueueSize == 0 && QueueSize > 0) {
        RecvQueueSize = QueueSize;
    }
}

void TServerConfig::Validate(TLog& log)
{
    BufferPool.Validate(log);
}

void TServerConfig::DumpHtml(IOutputStream& out) const
{
#define ENTRY(name, val, ...)                   \
    TABLER() {                                  \
        TABLED() { out << #name; }              \
        TABLED() { out << val; }                \
    }

    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                ENTRY(Backlog, Backlog);
                ENTRY(QueueSize, QueueSize);
                ENTRY(MaxBufferSize, MaxBufferSize);
                ENTRY(WaitMode, WaitMode);
                ENTRY(PollerThreads, PollerThreads);
                ENTRY(MaxInflightBytes, MaxInflightBytes);
                ENTRY(AdaptiveWaitSleepDelay, AdaptiveWaitSleepDelay.ToString());
                ENTRY(AdaptiveWaitSleepDuration, AdaptiveWaitSleepDuration.ToString());
                ENTRY(AlignedDataEnabled, true);
                ENTRY(IpTypeOfService, static_cast<ui32>(IpTypeOfService));
                ENTRY(SourceInterface, SourceInterface);
                ENTRY(VerbsQP, VerbsQP);
                ENTRY(SendQueueSize, SendQueueSize);
                ENTRY(RecvQueueSize, RecvQueueSize);
                ENTRY(BufferPool.ChunkSize, BufferPool.ChunkSize);
                ENTRY(BufferPool.MaxChunkAlloc, BufferPool.MaxChunkAlloc);
                ENTRY(BufferPool.MaxFreeChunks, BufferPool.MaxFreeChunks);
                ENTRY(StrictValidation, StrictValidation);
            }
        }
    }

#undef ENTRY
}

}   // namespace NCloud::NStorage::NRdma

////////////////////////////////////////////////////////////////////////////////

template <>
inline void Out<NCloud::NStorage::NRdma::EWaitMode>(
    IOutputStream& o,
    const NCloud::NStorage::NRdma::EWaitMode mode)
{
    switch (mode) {
    case NCloud::NStorage::NRdma::EWaitMode::Poll:
        o << "POLL";
        break;

    case NCloud::NStorage::NRdma::EWaitMode::BusyWait:
        o << "BUSY_WAIT";
        break;

    case NCloud::NStorage::NRdma::EWaitMode::AdaptiveWait:
        o << "ADAPTIVE_WAIT";
        break;
    }
}
