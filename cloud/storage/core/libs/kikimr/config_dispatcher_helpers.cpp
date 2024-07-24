#include "config_dispatcher_helpers.h"

#include <contrib/ydb/core/protos/console_config.pb.h>

namespace NCloud::NStorage {

using namespace NKikimr::NConfig;

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TAllowList> ParseConfigDispatcherItems(
    const TVector<TString>& items)
{
    TAllowList result;
    for (const auto& item: items) {
        NKikimrConsole::TConfigItem::EKind value;
        if (!NKikimrConsole::TConfigItem::EKind_Parse(item, &value)) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder()
                    << "Failed to parse "
                    << item
                    << " as NKikimrConsole::TConfigItem::EKind value");
        }
        result.Items.emplace(value);
    }
    return result;
}

}   // namespace NCloud::NStorage
