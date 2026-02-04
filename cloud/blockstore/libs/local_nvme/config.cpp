#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_LOCAL_NVME_CONFIG(xxx)                                      \
    xxx(DevicesSourceUri,                TString,          ""                 )\
    xxx(StateCacheFilePath,              TString,          ""                 )\
// BLOCKSTORE_LOCAL_NVME_CONFIG

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

}   //  namespace

////////////////////////////////////////////////////////////////////////////////

TLocalNVMeConfig::TLocalNVMeConfig(NProto::TLocalNVMeConfig proto)
    : Proto(std::move(proto))
{}

TLocalNVMeConfig::~TLocalNVMeConfig() = default;

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TLocalNVMeConfig::Get##name() const                                       \
{                                                                              \
    const auto value = Proto.Get##name();                                      \
    return !value ? Default##name : ConvertValue<type>(value);                 \
}                                                                              \
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_LOCAL_NVME_CONFIG(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

void TLocalNVMeConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": ";                                                      \
    out << Get##name();                                                        \
    out << Endl;                                                               \
// BLOCKSTORE_CONFIG_DUMP

    BLOCKSTORE_LOCAL_NVME_CONFIG(BLOCKSTORE_CONFIG_DUMP);

#undef BLOCKSTORE_CONFIG_DUMP
}

void TLocalNVMeConfig::DumpHtml(IOutputStream& out) const
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
                BLOCKSTORE_LOCAL_NVME_CONFIG(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }

#undef BLOCKSTORE_CONFIG_DUMP
}

}   // namespace NCloud::NBlockStore
