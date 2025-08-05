#include "client.h"

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

#define SET(param, ...) \
    if (const auto& value = config.Get##param()) { \
        param = __VA_ARGS__(value); \
    }

TClientConfig::TClientConfig(const NProto::TRdmaClient& config)
{
    SET(QueueSize);
    SET(MaxBufferSize);
    SET(WaitMode, Convert);
    SET(PollerThreads);
    SET(AdaptiveWaitSleepDelay, TDuration::MicroSeconds);
    SET(AdaptiveWaitSleepDuration, TDuration::MicroSeconds);
    SET(AlignedDataEnabled);
    SET(IpTypeOfService);
}

#undef SET

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
