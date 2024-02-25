#include "factory.h"

#include <util/generic/map.h>
#include <util/string/subst.h>

#include <functional>

namespace NCloud::NFileStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewAddClusterNodeCommand();
TCommandPtr NewCreateCommand();
TCommandPtr NewDescribeCommand();
TCommandPtr NewDestroyCommand();
TCommandPtr NewKickEndpointCommand();
TCommandPtr NewListClusterNodesCommand();
TCommandPtr NewListEndpointsCommand();
TCommandPtr NewListFileStoresCommand();
TCommandPtr NewLsCommand();
TCommandPtr NewMkDirCommand();
TCommandPtr NewMountCommand();
TCommandPtr NewReadCommand();
TCommandPtr NewRemoveClusterNodeCommand();
TCommandPtr NewResizeCommand();
TCommandPtr NewRmCommand();
TCommandPtr NewStartEndpointCommand();
TCommandPtr NewStopEndpointCommand();
TCommandPtr NewTouchCommand();
TCommandPtr NewWriteCommand();
TCommandPtr NewExecuteActionCommand();
TCommandPtr NewCreateSessionCommand();
TCommandPtr NewResetSessionCommand();

////////////////////////////////////////////////////////////////////////////////

using TFactoryFunc = std::function<TCommandPtr()>;
using TFactoryMap = TMap<TString, TFactoryFunc>;

static const TMap<TString, TFactoryFunc> Commands = {
    { "addclusternode", NewAddClusterNodeCommand },
    { "create", NewCreateCommand },
    { "describe", NewDescribeCommand },
    { "destroy", NewDestroyCommand },
    { "kickendpoint", NewKickEndpointCommand },
    { "listclusternodes", NewListClusterNodesCommand },
    { "listendpoints", NewListEndpointsCommand },
    { "listfilestores", NewListFileStoresCommand },
    { "ls", NewLsCommand },
    { "mkdir", NewMkDirCommand },
    { "mount", NewMountCommand },
    { "read", NewReadCommand },
    { "removeclusternode", NewRemoveClusterNodeCommand },
    { "resize", NewResizeCommand },
    { "rm", NewRmCommand },
    { "startendpoint", NewStartEndpointCommand },
    { "stopendpoint", NewStopEndpointCommand },
    { "touch", NewTouchCommand },
    { "write", NewWriteCommand },
    { "executeaction", NewExecuteActionCommand },
    { "createsession", NewCreateSessionCommand },
    { "resetsession", NewResetSessionCommand },
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
    if (const auto* func = Commands.FindPtr(name)) {
        return (*func)();
    }

    return nullptr;
}

TVector<TString> GetCommandNames()
{
    TVector<TString> names;
    names.reserve(Commands.size());

    for (const auto& kv: Commands) {
        names.push_back(kv.first);
    }

    return names;
}

}   // namespace NCloud::NFileStore::NClient
