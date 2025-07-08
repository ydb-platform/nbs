#include "config.h"

#include <cloud/storage/core/protos/trace.pb.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <chrono>

namespace NCloud::NBlockStore::NClient {

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_CLIENT_CONFIG(xxx)                                          \
    xxx(Host,                   TString,          "localhost"                 )\
    xxx(Port,                   ui32,             0                           )\
    xxx(InsecurePort,           ui32,             9766                        )\
    xxx(MaxMessageSize,         ui32,             64_MB                       )\
    xxx(ThreadsCount,           ui32,             1                           )\
                                                                               \
    xxx(RequestTimeout,                 TDuration,      30s                   )\
    xxx(RequestTimeoutIncrementOnRetry, TDuration,      30s                   )\
    xxx(RequestTimeoutMax,              TDuration,      2min                  )\
    xxx(RetryTimeout,                   TDuration,      5min                  )\
    xxx(RetryTimeoutIncrement,          TDuration,      500ms                 )\
    xxx(ConnectionErrorMaxRetryTimeout, TDuration,      100ms                 )\
    xxx(GrpcReconnectBackoff,           TDuration,      100ms                 )\
    xxx(DiskRegistryBasedDiskInitialRetryTimeout,  TDuration,  500ms          )\
    xxx(YDBBasedDiskInitialRetryTimeout,           TDuration,  500ms          )\
                                                                               \
    xxx(MemoryQuotaBytes,       ui32,             0                           )\
    xxx(SecurePort,             ui32,             0                           )\
    xxx(RootCertsFile,          TString,          {}                          )\
    xxx(CertFile,               TString,          {}                          )\
    xxx(CertPrivateKeyFile,     TString,          {}                          )\
    xxx(AuthToken,              TString,          {}                          )\
    xxx(UnixSocketPath,         TString,          {}                          )\
    xxx(GrpcThreadsLimit,       ui32,             4                           )\
    xxx(InstanceId,             TString,          {}                          )\
    xxx(MaxRequestSize,         ui32,             4_MB                        )\
    xxx(ClientId,               TString,          {}                          )\
    xxx(IpcType,                NProto::EClientIpcType, NProto::IPC_GRPC      )\
    xxx(NbdThreadsCount,        ui32,             1                           )\
    xxx(NbdSocketSuffix,        TString,          {}                          )\
    xxx(NbdStructuredReply,     bool,             false                       )\
    xxx(NbdUseNbsErrors,        bool,             false                       )\
    xxx(RemountDeadline,        TDuration,        500ms                       )\
    xxx(NvmeDeviceTransportId,  TString,          {}                          )\
    xxx(NvmeDeviceNqn,          TString,          {}                          )\
    xxx(ScsiDeviceUrl,          TString,          {}                          )\
    xxx(ScsiInitiatorIqn,       TString,          {}                          )\
    xxx(RdmaDeviceAddress,      TString,          {}                          )\
    xxx(RdmaDevicePort,         ui32,             0                           )\
    xxx(LocalNonreplDisableDurableClient,         bool, false                 )\
    xxx(SkipCertVerification,   bool,             false                       )\
// BLOCKSTORE_CLIENT_CONFIG

#define BLOCKSTORE_CLIENT_DECLARE_CONFIG(name, type, value)                    \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// BLOCKSTORE_CLIENT_DECLARE_CONFIG

BLOCKSTORE_CLIENT_CONFIG(BLOCKSTORE_CLIENT_DECLARE_CONFIG)

#undef BLOCKSTORE_CLIENT_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

template <typename TTarget, typename TSource>
TTarget ConvertValue(TSource value)
{
    return static_cast<TTarget>(std::move(value));
}

template <>
TDuration ConvertValue<TDuration, ui32>(ui32 value)
{
    return TDuration::MilliSeconds(value);
}

template <>
TRequestThresholds
ConvertValue<TRequestThresholds, TProtoRequestThresholds>(
    const TProtoRequestThresholds value)
{
    return ConvertRequestThresholds(value);
}

////////////////////////////////////////////////////////////////////////////////

IOutputStream& operator <<(IOutputStream& out, NProto::EClientIpcType ipcType)
{
    const auto& s = NProto::EClientIpcType_Name(ipcType);
    if (s.empty()) {
        return out << "(Unknown value " << static_cast<int>(ipcType) << ")";
    }
    return out << s;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TClientAppConfig::TClientAppConfig(NProto::TClientAppConfig appConfig)
    : AppConfig(std::move(appConfig))
    , ClientConfig(AppConfig.GetClientConfig())
    , LogConfig(AppConfig.GetLogConfig())
    , MonitoringConfig(AppConfig.GetMonitoringConfig())
    , IamConfig(AppConfig.GetIamConfig())
{}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TClientAppConfig::Get##name() const                                       \
{                                                                              \
    auto has = ClientConfig.Has##name();                                       \
    return has ? ConvertValue<type>(ClientConfig.Get##name()) : Default##name; \
}                                                                              \
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_CLIENT_CONFIG(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

TRequestThresholds TClientAppConfig::GetRequestThresholds() const {
    return ConvertValue<TRequestThresholds>(ClientConfig.GetRequestThresholds());
}

void TClientAppConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": " << Get##name() << Endl;                               \
// BLOCKSTORE_CONFIG_DUMP

    BLOCKSTORE_CLIENT_CONFIG(BLOCKSTORE_CONFIG_DUMP);

#undef BLOCKSTORE_CONFIG_DUMP
}

void TClientAppConfig::DumpHtml(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    TABLER() {                                                                 \
        TABLED() { out << #name; }                                             \
        TABLED() { out << Get##name(); }                                       \
    }                                                                          \
// BLOCKSTORE_CONFIG_DUMP

    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                BLOCKSTORE_CLIENT_CONFIG(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }

#undef BLOCKSTORE_CONFIG_DUMP
}

}   // namespace NCloud::NBlockStore::NClient
