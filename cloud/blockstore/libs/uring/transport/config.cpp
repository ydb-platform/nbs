#include "config.h"

#include <util/system/yassert.h>

namespace NCloud::NBlockStore::NUring {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define SET(param, ...)                                                        \
    if (const auto& value = config.Get##param()) {                             \
        result.param = __VA_ARGS__(value);                                     \
    }

#define SET_OPTIONAL(param)                                                    \
    if (config.Has##param()) {                                                 \
        result.param = config.Get##param();                                    \
    }

EOobMode ConvertOobMode(NProto::EUringOobMode mode)
{
    switch (mode) {
        case NProto::URING_OOB_AUTO:
            return EOobMode::Auto;
        case NProto::URING_OOB_OFF:
            return EOobMode::Off;
        case NProto::URING_OOB_REQUIRED:
            return EOobMode::Required;
        default:
            Y_ABORT("unsupported oob mode %d", mode);
    }
}

ESendZeroCopyMode ConvertZcMode(NProto::EUringSendZeroCopyMode mode)
{
    switch (mode) {
        case NProto::URING_SEND_ZC_AUTO:
            return ESendZeroCopyMode::Auto;
        case NProto::URING_SEND_ZC_OFF:
            return ESendZeroCopyMode::Off;
        case NProto::URING_SEND_ZC_REQUIRED:
            return ESendZeroCopyMode::Required;
        default:
            Y_ABORT("unsupported send zc mode %d", mode);
    }
}

const char* ToString(EOobMode mode)
{
    switch (mode) {
        case EOobMode::Off:
            return "Off";
        case EOobMode::Auto:
            return "Auto";
        case EOobMode::Required:
            return "Required";
        default:
            return "Unknown";
    }
}

const char* ToString(ESendZeroCopyMode mode)
{
    switch (mode) {
        case ESendZeroCopyMode::Off:
            return "Off";
        case ESendZeroCopyMode::Auto:
            return "Auto";
        case ESendZeroCopyMode::Required:
            return "Required";
        default:
            return "Unknown";
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
    out << "IoThreads: " << IoThreads << "\n"
        << "QueueSize: " << QueueSize << "\n"
        << "MaxInflightRequests: " << MaxInflightRequests << "\n"
        << "MaxInflightBytes: " << MaxInflightBytes << "\n"
        << "MaxProtoSize: " << MaxProtoSize << "\n"
        << "MaxDataSize: " << MaxDataSize << "\n"
        << "SendWindowBytes: " << SendWindowBytes << "\n"
        << "OobMode: " << ToString(OobMode) << "\n"
        << "OobThreshold: " << OobThreshold << "\n"
        << "SendZeroCopyMode: " << ToString(SendZeroCopyMode) << "\n"
        << "SendZeroCopyThreshold: " << SendZeroCopyThreshold << "\n"
        << "DirectSgIoEnabled: " << DirectSgIoEnabled << "\n"
        << "TcpUserTimeout: " << TcpUserTimeout << "\n"
        << "KeepAliveIdle: " << KeepAliveIdle << "\n"
        << "KeepAliveInterval: " << KeepAliveInterval << "\n"
        << "KeepAliveCount: " << KeepAliveCount << "\n";
}

void TClientConfig::DumpHtml(IOutputStream& out) const
{
    out << "<pre>";
    TTransportConfig::DumpHtml(out);
    out << "MaxResponseDelay: " << MaxResponseDelay << "\n"
        << "MaxReconnectDelay: " << MaxReconnectDelay << "\n"
        << "</pre>";
}

void TServerConfig::DumpHtml(IOutputStream& out) const
{
    out << "<pre>";
    TTransportConfig::DumpHtml(out);
    out << "Backlog: " << Backlog << "\n"
        << "DataTokenTtl: " << DataTokenTtl << "\n"
        << "</pre>";
}

}   // namespace NCloud::NBlockStore::NUring
