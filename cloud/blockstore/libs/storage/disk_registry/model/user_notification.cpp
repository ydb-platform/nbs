#include "user_notification.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TString GetEntityId(const NProto::TUserNotification& notif)
{
    using EEventCase = NProto::TUserNotification::EventCase;

    switch (notif.GetEventCase()) {
        case EEventCase::kDiskError:
            return notif.GetDiskError().GetDiskId();
        case EEventCase::kDiskBackOnline:
            return notif.GetDiskBackOnline().GetDiskId();
        case EEventCase::EVENT_NOT_SET:
            return {};
    }
}

TString GetEventName(const NProto::TUserNotification& notif)
{
    auto* field = NProto::TUserNotification::descriptor()->FindFieldByNumber(
        notif.GetEventCase());

    if (field) {
        return field->name();
    }

    return {};
}

}   // namespace NCloud::NBlockStore::NStorage
