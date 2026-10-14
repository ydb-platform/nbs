#include "config.h"

#include <util/stream/output.h>

namespace NCloud::NBlockStore::NUring {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define SET(param, ...)                            \
    if (const auto& value = config.Get##param()) { \
        result.param = __VA_ARGS__(value);         \
    }

#define SET_OPTIONAL(param)                       \
    if (config.Has##param()) {                    \
        result.param = config.Get##param();       \
    }

EOobMode ConvertOobMode(NProto::EUringOobMode mode)
{
    switch (mode) {
        case NProto::URING_OOB_OFF: return EOobMode::Off;
        case NProto::URING_OOB_REQUIRED: return EOobMode::Required;
        default: return EOobMode::Auto;
    }
}

ESendZeroCopyMode ConvertZcMode(NProto::EUringSendZeroCopyMode mode)
{
    switch (mode) {
        case NProto::URING_SEND_ZC_OFF: return ESendZeroCopyMode::Off;
        case NProto::URING_SEND_ZC_REQUIRED: return ESendZeroCopyMode::Required;
        default: return ESendZeroCopyMode::Auto;
    }
}

template <typename TProto, typename TResult>
void SetCommon(const TProto& config, TResult& result)
{
    SET(IoThreads);
    SET(QueueSize);
    SET(MaxInflightRequests);
    SET(MaxInflightBytes);
    SET(MaxProtoSize);
    SET(MaxDataSize);
    SET(SendWindowBytes);
    SET(OobMode, ConvertOobMode);
    SET(OobThreshold);
    SET(SendZeroCopyMode, ConvertZcMode);
    SET(SendZeroCopyThreshold);
    SET_OPTIONAL(DirectSgIoEnabled);
    SET(TcpUserTimeout, TDuration::MilliSeconds);
    SET(KeepAliveIdle, TDuration::MilliSeconds);
    SET(KeepAliveInterval, TDuration::MilliSeconds);
    SET(KeepAliveCount);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TClientConfig CreateClientConfig(const NProto::TUringClient& config)
{
    TClientConfig result;
    SetCommon(config, result);
    SET(MaxResponseDelay, TDuration::MilliSeconds);
    SET(MaxReconnectDelay, TDuration::MilliSeconds);
    return result;
}

TServerConfig CreateServerConfig(const NProto::TUringServer& config)
{
    TServerConfig result;
    SetCommon(config, result);
    SET(Backlog);
    SET(DataTokenTtl, TDuration::MilliSeconds);
    return result;
}

#undef SET_OPTIONAL
#undef SET

void TTransportConfig::DumpHtml(IOutputStream& out) const
{
    out << "<pre>"
        << "IoThreads: " << IoThreads << "\n"
        << "QueueSize: " << QueueSize << "\n"
        << "MaxInflightRequests: " << MaxInflightRequests << "\n"
        << "MaxInflightBytes: " << MaxInflightBytes << "\n"
        << "MaxProtoSize: " << MaxProtoSize << "\n"
        << "MaxDataSize: " << MaxDataSize << "\n"
        << "SendWindowBytes: " << SendWindowBytes << "\n"
        << "OobMode: " << static_cast<int>(OobMode) << "\n"
        << "OobThreshold: " << OobThreshold << "\n"
        << "SendZeroCopyMode: " << static_cast<int>(SendZeroCopyMode) << "\n"
        << "SendZeroCopyThreshold: " << SendZeroCopyThreshold << "\n"
        << "DirectSgIoEnabled: " << DirectSgIoEnabled << "\n"
        << "TcpUserTimeout: " << TcpUserTimeout << "\n"
        << "</pre>";
}

}   // namespace NCloud::NBlockStore::NUring
