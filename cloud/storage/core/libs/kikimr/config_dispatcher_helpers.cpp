#include "config_dispatcher_helpers.h"

#include <contrib/ydb/core/protos/console_config.pb.h>

namespace NCloud::NStorage {

using namespace NKikimr::NConfig;

////////////////////////////////////////////////////////////////////////////////

void SetupConfigDispatcher(
    const NProto::TYdbConfigDispatcherSettings& settings,
    NKikimr::NConfig::TConfigsDispatcherInitInfo* config)
{
    const auto& names = settings.HasAllowList()
        ? settings.GetAllowList().GetNames()
        : settings.GetDenyList().GetNames();

    std::set<ui32> items;

    for (const auto& name: names) {
        NKikimrConsole::TConfigItem::EKind value {};
        if (!NKikimrConsole::TConfigItem::EKind_Parse(name, &value)) {
            ReportConfigDispatcherItemParseError(TStringBuilder()
                << "Failed to parse "
                << name
                << " as NKikimrConsole::TConfigItem::EKind value");
            return;
        }
        items.emplace(value);
    }

    auto& rules = config->ItemsServeRules;

    if (settings.HasAllowList()) {
        rules.emplace<TAllowList>(items);
    } else {
        rules.emplace<TDenyList>(items);
    }
}

}   // namespace NCloud::NStorage
