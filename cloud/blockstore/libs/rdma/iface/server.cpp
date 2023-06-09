#include "server.h"

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
            Y_FAIL("unsupported wait mode %d", mode);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define SET(param, ...) \
    if (const auto& value = config.Get##param()) { \
        param = __VA_ARGS__(value); \
    }

TServerConfig::TServerConfig(const NProto::TRdmaServer& config)
{
    SET(Backlog);
    SET(QueueSize);
    SET(MaxBufferSize);
    SET(KeepAliveTimeout, TDuration::MilliSeconds);
    SET(WaitMode, Convert);
    SET(PollerThreads);
    SET(MaxInflightBytes);
}

#undef SET

}   // namespace NCloud::NBlockStore::NRdma
