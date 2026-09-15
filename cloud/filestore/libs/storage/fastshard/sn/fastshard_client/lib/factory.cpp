#include "factory.h"

#include "acquire_devices.h"
#include "advance_lsn_low_watermark.h"
#include "read_journal_tail.h"
#include "read_pages.h"
#include "release_devices.h"
#include "write_log_record.h"

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/string/subst.h>

namespace NCloud::NFileStore::NStorage::NFastShard::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TFactoryFunc = TCommandPtr (*)(IStorageNodePtr client);

// One command per IStorageNode method, registered off SN_METHODS so that
// a method added to the protocol without a command here fails to compile.
const THashMap<TString, TFactoryFunc>& GetFactoryMap()
{
    static const THashMap<TString, TFactoryFunc> map = {
#define SN_REGISTER_COMMAND(name, ...)                                         \
    {NormalizeCommand(#name), New##name##Command},                             \
    // SN_REGISTER_COMMAND

        SN_METHODS(SN_REGISTER_COMMAND)

#undef SN_REGISTER_COMMAND
    };
    return map;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TString NormalizeCommand(TString name)
{
    name.to_lower();
    SubstGlobal(name, "-", TStringBuf{});
    SubstGlobal(name, "_", TStringBuf{});
    return name;
}

TCommandPtr GetCommand(const TString& name, IStorageNodePtr client)
{
    if (const auto* func = GetFactoryMap().FindPtr(name)) {
        return (*func)(std::move(client));
    }

    return nullptr;
}

TVector<TString> GetCommandNames()
{
    const auto& map = GetFactoryMap();

    TVector<TString> names;
    names.reserve(map.size());
    for (const auto& [name, func]: map) {
        names.push_back(name);
    }

    Sort(names);
    return names;
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard::NClient
