#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

namespace NCloud::NFileStore::NSpdk {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_SPDK_ENV_CONFIG(xxx)                                        \
    xxx(CpuMask,                        TString,          ""                  )\
    xxx(HugeDir,                        TString,          ""                  )\
// BLOCKSTORE_SPDK_ENV_CONFIG

#define BLOCKSTORE_SPDK_ENV_DECLARE_CONFIG(name, type, value)                  \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// BLOCKSTORE_SPDK_ENV_DECLARE_CONFIG

BLOCKSTORE_SPDK_ENV_CONFIG(BLOCKSTORE_SPDK_ENV_DECLARE_CONFIG)

#undef BLOCKSTORE_SPDK_ENV_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

template <typename TTarget, typename TSource>
TTarget ConvertValue(TSource value)
{
    return static_cast<TTarget>(std::move(value));
}

template <typename T>
bool IsEmpty(const T& t)
{
    return !t;
}

template <typename T>
void DumpImpl(const T& t, IOutputStream& os)
{
    os << t;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TSpdkEnvConfig::TSpdkEnvConfig(NProto::TSpdkEnvConfig config)
    : Config(std::move(config))
{}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TSpdkEnvConfig::Get##name() const                                         \
{                                                                              \
    const auto value = Config.Get##name();                                     \
    return IsEmpty(value) ? Default##name : ConvertValue<type>(value);         \
}                                                                              \
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_SPDK_ENV_CONFIG(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

void TSpdkEnvConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": ";                                                      \
    DumpImpl(Get##name(), out);                                                \
    out << Endl;                                                               \
// BLOCKSTORE_CONFIG_DUMP

    BLOCKSTORE_SPDK_ENV_CONFIG(BLOCKSTORE_CONFIG_DUMP);

#undef BLOCKSTORE_CONFIG_DUMP
}

void TSpdkEnvConfig::DumpHtml(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    TABLER() {                                                                 \
        TABLED() { out << #name; }                                             \
        TABLED() { DumpImpl(Get##name(), out); }                               \
    }                                                                          \
// BLOCKSTORE_CONFIG_DUMP

    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                BLOCKSTORE_SPDK_ENV_CONFIG(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }

#undef BLOCKSTORE_CONFIG_DUMP
}

}   // namespace NCloud::NFileStore::NSpdk
