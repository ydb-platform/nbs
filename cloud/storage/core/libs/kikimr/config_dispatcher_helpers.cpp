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
    bool anyFailed = false;
    TVector<TString> failedItems;

    for (const auto& name: names) {
        NKikimrConsole::TConfigItem::EKind value {};
        if (!NKikimrConsole::TConfigItem::EKind_Parse(name, &value)) {
            anyFailed = true;
            failedItems.push_back(name);
            continue;
        }
        items.emplace(value);
    }

    if (anyFailed) {
        ReportConfigDispatcherItemParseError(TStringBuilder()
            << "Failed to parse: ("
            << JoinRange(",", failedItems.begin(), failedItems.end())
            << ") as NKikimrConsole::TConfigItem::EKind value");
    }

    if (items.empty()) {
        return;
    }

    auto& rules = config->ItemsServeRules;

    if (settings.HasAllowList()) {
        rules.emplace<TAllowList>(std::move(items));
    } else {
        rules.emplace<TDenyList>(std::move(items));
    }
}

}   // namespace NCloud::NStorage
