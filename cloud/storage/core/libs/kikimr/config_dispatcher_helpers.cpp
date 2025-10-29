#include "config_dispatcher_helpers.h"

#include <ydb/core/protos/console_config.pb.h>

namespace NCloud::NStorage {

using namespace NKikimr::NConfig;

////////////////////////////////////////////////////////////////////////////////

static const TString tenantLabel = "tenant";
static const TString nodeNameLabel = "node_type";

TRegisterDynamicNodeOptions::TNodeLabels GetLabels(
    const NCloud::NProto::TConfigDispatcherSettings& settings,
    const TString& tenantName,
    const TString& nodeType)
{
    TRegisterDynamicNodeOptions::TNodeLabels result{
        {tenantLabel, tenantName},
        {nodeNameLabel, nodeType},
    };
    for (const auto& label: settings.GetAdditionalNodeLabels()) {
        result.emplace(label.GetKey(), label.GetValue());
    }
    return result;
}

void SetupConfigDispatcher(
    const NProto::TConfigDispatcherSettings& settings,
    const TString& tenantName,
    const TString& nodeType,
    NKikimr::NConfig::TConfigsDispatcherInitInfo* config)
{
    config->Labels = GetLabels(settings, tenantName, nodeType);

    if (!settings.HasAllowList() && !settings.HasDenyList()) {
        return;
    }

    const auto& names = settings.HasAllowList()
        ? settings.GetAllowList().GetNames()
        : settings.GetDenyList().GetNames();

    std::set<ui32> items;
    TVector<TString> failedItemNames;

    for (const auto& name: names) {
        NKikimrConsole::TConfigItem::EKind value {};
        if (!NKikimrConsole::TConfigItem::EKind_Parse(name, &value)) {
            failedItemNames.push_back(name);
            continue;
        }
        items.emplace(value);
    }

    if (!failedItemNames.empty()) {
        ReportConfigDispatcherItemParseError(TStringBuilder()
            << "Failed to parse: ("
            << JoinRange(",", failedItemNames.begin(), failedItemNames.end())
            << ") as NKikimrConsole::TConfigItem::EKind value");
    }

    auto& rules = config->ItemsServeRules;

    if (settings.HasAllowList()) {
        rules.emplace<TAllowList>(std::move(items));
    } else {
        rules.emplace<TDenyList>(std::move(items));
    }
}

}   // namespace NCloud::NStorage
