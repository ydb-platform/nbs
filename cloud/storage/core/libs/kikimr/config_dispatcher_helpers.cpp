#include "config_dispatcher_helpers.h"

#include <contrib/ydb/core/protos/console_config.pb.h>

namespace NCloud::NStorage {

using namespace NKikimr::NConfig;

////////////////////////////////////////////////////////////////////////////////

TResultOrError<std::set<ui32>> SetupConfigDispatcher(
    const NProto::TConfigDispatcherSettings& settings)
{
    const auto& names = settings.HasAllowList()
        ? settings.GetAllowList().GetNames()
        : settings.GetDenyList().GetNames();

    std::set<ui32>

    for (const auto& name: names) {
        NKikimrConsole::TConfigItem::EKind value;
        if (!NKikimrConsole::TConfigItem::EKind_Parse(name, &value)) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Failed to parse "
                    << name
                    << " as NKikimrConsole::TConfigItem::EKind value");
        }
        result.Items.emplace(value);
    }

    return MakeError(S_OK);
}

}   // namespace NCloud::NStorage
