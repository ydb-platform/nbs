#include "factory.h"

#include <util/generic/map.h>
#include <util/string/subst.h>

#include <functional>

namespace NCloud::NFileStore::NProfileTool {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDumpEventsCommand();
TCommandPtr NewFindBytesAccessCommand();
TCommandPtr NewMaskSensitiveData();

////////////////////////////////////////////////////////////////////////////////

using TFactoryFunc = std::function<TCommandPtr()>;
using TFactoryMap = TMap<TString, TFactoryFunc>;

static const TFactoryMap Commands = {
    {"dumpevents", NewDumpEventsCommand},
    {"findbytesaccess", NewFindBytesAccessCommand},
    {"masksensitivedata", NewMaskSensitiveData},
};

////////////////////////////////////////////////////////////////////////////////

TString NormalizeCommand(TString name)
{
    name.to_lower();
    SubstGlobal(name, "-", TStringBuf{});
    SubstGlobal(name, "_", TStringBuf{});
    return name;
}

TCommandPtr GetCommand(const TString& name)
{
    if (auto* func = Commands.FindPtr(name)) {
        return (*func)();
    }

    return nullptr;
}

TVector<TString> GetCommandNames()
{
    TVector<TString> names;
    names.reserve(Commands.size());

    for (const auto& [name, _]: Commands) {
        names.push_back(name);
    }

    return names;
}

}   // namespace NCloud::NFileStore::NProfileTool
