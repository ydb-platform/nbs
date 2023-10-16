#include "client.h"

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
}

#undef SET

}   // namespace NCloud::NBlockStore::NRdma
