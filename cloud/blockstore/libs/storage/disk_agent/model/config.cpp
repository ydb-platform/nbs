#include "config.h"

#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/size_literals.h>
#include <util/string/builder.h>
#include <util/system/fs.h>

#include <chrono>

namespace NCloud::NBlockStore::NStorage {

using namespace std::chrono_literals;

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
    xxx(RegisterRetryTimeout,       TDuration,          1s                    )\
    xxx(SecureEraseTimeout,         TDuration,          1min                  )\
    xxx(DeviceIOTimeout,            TDuration,          1min                  )\
    xxx(DeviceIOTimeoutsDisabled,   bool,               false                 )\
    xxx(ShutdownTimeout,            TDuration,          5s                    )\
    xxx(Backend,                                                               \
        NProto::EDiskAgentBackendType,                                         \
        NProto::DISK_AGENT_BACKEND_SPDK                                       )\
    xxx(DeviceEraseMethod,                                                     \
        NProto::EDeviceEraseMethod,                                            \
        NProto::DEVICE_ERASE_METHOD_ZERO_FILL                                 )\
                                                                               \
    xxx(AcquireRequired,                    bool,       false                 )\
    xxx(ReleaseInactiveSessionsTimeout,     TDuration,  10s                   )\
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
    xxx(DisableBrokenDevices,               bool,       false                 )\
                                                                               \
    xxx(IOParserActorAllocateStorageEnabled, bool,      false                 )\
                                                                               \
    xxx(MaxParallelSecureErasesAllowed,     ui32,       1                     )\
    xxx(UseLocalStorageSubmissionThread,    bool,       true                  )\
    xxx(AllowToKickOutOldClients,           bool,       false                  )\
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

IOutputStream& operator<<(IOutputStream& out, NProto::EDiskAgentBackendType pt)
{
    const TString& s = NProto::EDiskAgentBackendType_Name(pt);
    if (s.empty()) {
        return out << "(Unknown EDiskAgentBackendType value "
                   << static_cast<int>(pt) << ")";
    }

    return out << s;
}

IOutputStream& operator<<(IOutputStream& out, NProto::EDeviceEraseMethod pt)
{
    const TString& s = NProto::EDeviceEraseMethod_Name(pt);

    if (s.empty()) {
        return out << "(Unknown EDeviceEraseMethod value "
                   << static_cast<int>(pt) << ")";
    }

    return out << s;
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
