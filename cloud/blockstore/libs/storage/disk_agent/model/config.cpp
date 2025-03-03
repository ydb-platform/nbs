#include "config.h"

#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/size_literals.h>
#include <util/string/builder.h>
#include <util/system/fs.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_AGENT_CONFIG(xxx)                                           \
    xxx(Enabled,                    bool,               false                 )\
    xxx(AgentId,                    TString,            ""                    )\
    xxx(SeqNumber,                  ui64,               0                     )\
    xxx(DedicatedDiskAgent,         bool,               false                 )\
    xxx(PageSize,                   ui32,               4_MB                  )\
    xxx(MaxPageCount,               ui32,               256                   )\
    xxx(PageDropSize,               ui32,               512_KB                )\
    xxx(RegisterRetryTimeout,       TDuration,          TDuration::Seconds(1) )\
    xxx(SecureEraseTimeout,         TDuration,          TDuration::Minutes(1) )\
    xxx(DeviceIOTimeout,            TDuration,          TDuration::Minutes(1) )\
    xxx(DeviceIOTimeoutsDisabled,   bool,               false                 )\
    xxx(ShutdownTimeout,            TDuration,          TDuration::Seconds(5) )\
    xxx(Backend,                                                               \
        NProto::EDiskAgentBackendType,                                         \
        NProto::DISK_AGENT_BACKEND_SPDK                                       )\
    xxx(DeviceEraseMethod,                                                     \
        NProto::EDeviceEraseMethod,                                            \
        NProto::DEVICE_ERASE_METHOD_ZERO_FILL                                 )\
                                                                               \
    xxx(AcquireRequired,                    bool,       false                 )\
    xxx(ReleaseInactiveSessionsTimeout,     TDuration,  TDuration::Seconds(10))\
    xxx(DirectIoFlagDisabled,               bool,       false                 )\
    xxx(DeviceLockingEnabled,               bool,       false                 )\
    xxx(DeviceHealthCheckDisabled,          bool,       false                 )\
    xxx(CachedConfigPath,                   TString,    ""                    )\
    xxx(CachedSessionsPath,                 TString,    ""                    )\
    xxx(TemporaryAgent,                     bool,       false                 )\
    xxx(IOParserActorCount,                 ui32,       0                     )\
    xxx(OffloadAllIORequestsParsingEnabled, bool,       false                 )\
    xxx(DisableNodeBrokerRegistrationOnDevicelessAgent, bool,          false  )\
    xxx(MaxAIOContextEvents,                ui32,       1024                  )\
    xxx(PathsPerFileIOService,              ui32,       0                     )\
    xxx(DisableBrokenDevices,               bool,       0                     )\
                                                                               \
    xxx(IOParserActorAllocateStorageEnabled, bool,       0                    )\
                                                                               \
    xxx(MaxParallelSecureErasesAllowed,     ui32,       1                     )\
// BLOCKSTORE_AGENT_CONFIG

#define BLOCKSTORE_DECLARE_CONFIG(name, type, value)                           \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// BLOCKSTORE_DECLARE_CONFIG

BLOCKSTORE_AGENT_CONFIG(BLOCKSTORE_DECLARE_CONFIG)

#undef BLOCKSTORE_DECLARE_CONFIG

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

IOutputStream& operator <<(
    IOutputStream& out,
    NProto::EDiskAgentBackendType pt)
{
    switch (pt) {
        case NProto::DISK_AGENT_BACKEND_SPDK:
            return out << "DISK_AGENT_BACKEND_SPDK";
        case NProto::DISK_AGENT_BACKEND_AIO:
            return out << "DISK_AGENT_BACKEND_AIO";
        case NProto:: DISK_AGENT_BACKEND_NULL:
            return out << "DISK_AGENT_BACKEND_NULL";
    }

    return out
        << "(Unknown EDiskAgentBackendType value "
        << static_cast<int>(pt)
        << ")";
}

IOutputStream& operator <<(
    IOutputStream& out,
    NProto::EDeviceEraseMethod pt)
{
    switch (pt) {
        case NProto::DEVICE_ERASE_METHOD_ZERO_FILL:
            return out << "DEVICE_ERASE_METHOD_ZERO_FILL";
        case NProto::DEVICE_ERASE_METHOD_USER_DATA_ERASE:
            return out << "DEVICE_ERASE_METHOD_USER_DATA_ERASE";
        case NProto::DEVICE_ERASE_METHOD_CRYPTO_ERASE:
            return out << "DEVICE_ERASE_METHOD_CRYPTO_ERASE";
        case NProto::DEVICE_ERASE_METHOD_NONE:
            return out << "DEVICE_ERASE_METHOD_NONE";
        case NProto::DEVICE_ERASE_METHOD_DEALLOCATE:
            return out << "DEVICE_ERASE_METHOD_DEALLOCATE";
    }

    return out
        << "(Unknown EDeviceEraseMethod value "
        << static_cast<int>(pt)
        << ")";
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TDiskAgentConfig::Get##name() const                                       \
{                                                                              \
    if (Config.Has##name()) {                                                  \
        return ConvertValue<type>(Config.Get##name());                         \
    }                                                                          \
    return Default##name;                                                      \
}                                                                              \
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_AGENT_CONFIG(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

void TDiskAgentConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": " << Get##name() << Endl;                               \
// BLOCKSTORE_CONFIG_DUMP

    BLOCKSTORE_AGENT_CONFIG(BLOCKSTORE_CONFIG_DUMP);

#undef BLOCKSTORE_CONFIG_DUMP
}

void TDiskAgentConfig::DumpHtml(IOutputStream& out) const
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
                BLOCKSTORE_AGENT_CONFIG(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }

#undef BLOCKSTORE_CONFIG_DUMP
}

////////////////////////////////////////////////////////////////////////////////

auto LoadDiskAgentConfig(
    const TString& path) -> TResultOrError<NProto::TDiskAgentConfig>
{
    if (path.empty()) {
        return MakeError(E_ARGUMENT, "empty path");
    }

    if (!NFs::Exists(path)) {
        return MakeError(E_NOT_FOUND, "file doesn't exist");
    }

    NProto::TDiskAgentConfig proto;

    try {
        ParseProtoTextFromFileRobust(path, proto);
    } catch (...) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "can't load Disk Agent config from a file "
                             << path << ": " << CurrentExceptionMessage());
    }

    return proto;
}

NProto::TError SaveDiskAgentConfig(
    const TString& path,
    const NProto::TDiskAgentConfig& proto)
{
    if (path.empty()) {
        return MakeError(E_ARGUMENT, "empty path");
    }

    const TString tmpPath {path + ".tmp"};

    try {
        SerializeToTextFormat(proto, tmpPath);
    } catch (...) {
        return MakeError(
            E_FAIL,
            TStringBuilder() << "can't save Disk Agent config to a file "
                             << tmpPath << ": " << CurrentExceptionMessage());
    }

    if (!NFs::Rename(tmpPath, path)) {
        const auto ec = errno;
        char buf[64]{};

        return MakeError(
            MAKE_SYSTEM_ERROR(ec),
            TStringBuilder()
                << "can't rename a file from " << tmpPath << " to " << path
                << ::strerror_r(ec, buf, sizeof(buf)));
    }

    return {};
}

[[nodiscard]] auto UpdateDevicesWithSuspendedIO(
    const TString& path,
    const TVector<TString>& uuids) -> NProto::TError
{
    auto [config, error] = LoadDiskAgentConfig(path);
    if (HasError(error)) {
        return error;
    }

    config.MutableDevicesWithSuspendedIO()->Assign(uuids.begin(), uuids.end());

    return SaveDiskAgentConfig(path, config);
}

}   // namespace NCloud::NBlockStore::NStorage
