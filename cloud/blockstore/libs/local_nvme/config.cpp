#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/format.h>

#include <chrono>

namespace NCloud::NBlockStore {

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Plain scalar fields: always present via Get##name(), fall back to
// Default##name when not set in the proto.
#define BLOCKSTORE_LOCAL_NVME_CONFIG(xxx)                                      \
    xxx(DevicesSourceUri, TString, "")                                         \
    xxx(StateCacheFilePath, TString, "")                                       \
    xxx(UpdateDevicesInterval, TDuration, 1min)                                \
    xxx(UpdateCountersInterval, TDuration, 15s)                                \
    // BLOCKSTORE_LOCAL_NVME_CONFIG

// Fields wrapped into their own config-object type: no sensible default,
// so Get##name() returns std::optional<type> and is std::nullopt when the
// submessage is not set in the proto.
#define BLOCKSTORE_LOCAL_NVME_CONFIG_OPT(xxx)                                  \
    xxx(LockdownConfig, TNVMeLockdownConfig)                                   \
    // BLOCKSTORE_LOCAL_NVME_CONFIG_OPT

#define BLOCKSTORE_LOCAL_NVME_DECLARE_CONFIG(name, type, value)                \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
    // BLOCKSTORE_LOCAL_NVME_DECLARE_CONFIG

BLOCKSTORE_LOCAL_NVME_CONFIG(BLOCKSTORE_LOCAL_NVME_DECLARE_CONFIG)

#undef BLOCKSTORE_LOCAL_NVME_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

template <typename TTarget, typename TSource>
TTarget ConvertValue(const TSource& value)
{
    return TTarget(value);
}

template <>
TDuration ConvertValue<TDuration, ui32>(const ui32& value)
{
    return TDuration::MilliSeconds(value);
}

void DumpValue(IOutputStream& out, const TVector<ui8>& opcodes)
{
    out << "[ ";
    for (ui8 opcode: opcodes) {
        out << Hex(opcode, HF_ADDX) << " ";
    }
    out << "]";
}

template <typename T>
void DumpValue(IOutputStream& out, const T& value)
{
    out << value;
}

template <typename T>
void DumpValueHtml(IOutputStream& out, const T& value)
{
    DumpValue(out, value);
}

template <typename T>
void DumpValue(IOutputStream& out, const std::optional<T>& value)
{
    if (value) {
        DumpValue(out, *value);
    } else {
        out << "(not set)";
    }
}

template <typename T>
void DumpValueHtml(IOutputStream& out, const std::optional<T>& value)
{
    if (value) {
        DumpValueHtml(out, *value);
    } else {
        out << "(not set)";
    }
}

}   //  namespace

////////////////////////////////////////////////////////////////////////////////

TNVMeLockdownConfig::TNVMeLockdownConfig(
    const NProto::TLocalNVMeConfig::TLockdownConfig& proto)
    : Proto(proto)
{}

TNVMeLockdownConfig::~TNVMeLockdownConfig() = default;

TVector<ui8> TNVMeLockdownConfig::GetAllowedAdminOpcodes() const
{
    const auto& value = Proto.GetAllowedAdminOpcodes();
    return {value.begin(), value.end()};
}

TVector<ui8> TNVMeLockdownConfig::GetAllowedSetFeatureIds() const
{
    const auto& value = Proto.GetAllowedSetFeatureIds();
    return {value.begin(), value.end()};
}

bool TNVMeLockdownConfig::GetBlockLockdownCommand() const
{
    return Proto.GetBlockLockdownCommand();
}

void DumpValue(IOutputStream& out, const TNVMeLockdownConfig& config)
{
    out << Endl;

    out << "  AllowedAdminOpcodes: ";
    DumpValue(out, config.GetAllowedAdminOpcodes());
    out << Endl;

    out << "  AllowedSetFeatureIds: ";
    DumpValue(out, config.GetAllowedSetFeatureIds());
    out << Endl;

    out << "  BlockLockdownCommand: " << config.GetBlockLockdownCommand();
}

void DumpValueHtml(IOutputStream& out, const TNVMeLockdownConfig& config)
{
    HTML (out) {
        TABLE_CLASS ("table table-condensed") {
            TABLEBODY () {
                TABLER () {
                    TABLED () {
                        out << "AllowedAdminOpcodes";
                    }
                    TABLED () {
                        DumpValueHtml(out, config.GetAllowedAdminOpcodes());
                    }
                }

                TABLER () {
                    TABLED () {
                        out << "AllowedSetFeatureIds";
                    }
                    TABLED () {
                        DumpValueHtml(out, config.GetAllowedSetFeatureIds());
                    }
                }

                TABLER () {
                    TABLED () {
                        out << "BlockLockdownCommand";
                    }
                    TABLED () {
                        out << config.GetBlockLockdownCommand();
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TLocalNVMeConfig::TLocalNVMeConfig(NProto::TLocalNVMeConfig proto)
    : Proto(std::move(proto))
{}

TLocalNVMeConfig::~TLocalNVMeConfig() = default;

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
    type TLocalNVMeConfig::Get##name() const                                   \
    {                                                                          \
        if (Proto.Has##name()) {                                               \
            return ConvertValue<type>(Proto.Get##name());                      \
        }                                                                      \
        return Default##name;                                                  \
    }                                                                          \
    // BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_LOCAL_NVME_CONFIG(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

#define BLOCKSTORE_CONFIG_GETTER_OPT(name, type)                               \
    std::optional<type> TLocalNVMeConfig::Get##name() const                    \
    {                                                                          \
        if (!Proto.Has##name()) {                                              \
            return std::nullopt;                                               \
        }                                                                      \
        return type(Proto.Get##name());                                        \
    }                                                                          \
    // BLOCKSTORE_CONFIG_GETTER_OPT

BLOCKSTORE_LOCAL_NVME_CONFIG_OPT(BLOCKSTORE_CONFIG_GETTER_OPT)

#undef BLOCKSTORE_CONFIG_GETTER_OPT

void TLocalNVMeConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": ";                                                      \
    DumpValue(out, Get##name());                                               \
    out << Endl;                                                               \
    // BLOCKSTORE_CONFIG_DUMP

    BLOCKSTORE_LOCAL_NVME_CONFIG(BLOCKSTORE_CONFIG_DUMP);
    BLOCKSTORE_LOCAL_NVME_CONFIG_OPT(BLOCKSTORE_CONFIG_DUMP);

#undef BLOCKSTORE_CONFIG_DUMP
}

void TLocalNVMeConfig::DumpHtml(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    TABLER () {                                                                \
        TABLED () {                                                            \
            out << #name;                                                      \
        }                                                                      \
        TABLED () {                                                            \
            DumpValueHtml(out, Get##name());                                   \
        }                                                                      \
    }                                                                          \
    // BLOCKSTORE_CONFIG_DUMP

    HTML (out) {
        TABLE_CLASS ("table table-condensed") {
            TABLEBODY () {
                BLOCKSTORE_LOCAL_NVME_CONFIG(BLOCKSTORE_CONFIG_DUMP);
                BLOCKSTORE_LOCAL_NVME_CONFIG_OPT(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }

#undef BLOCKSTORE_CONFIG_DUMP
}

}   // namespace NCloud::NBlockStore
