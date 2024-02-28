#pragma once

#include "public.h"

#include <contrib/ydb/core/base/events.h>
#include <contrib/ydb/library/services/services.pb.h>

#include <contrib/ydb/library/actors/core/events.h>

#include <util/generic/string.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

#define STORAGE_ACTORS(xxx)                                                    \
    xxx(HIVE_PROXY)                                                            \
    xxx(AUTH)                                                                  \
    xxx(USER_STATS)                                                            \
// STORAGE_ACTORS

////////////////////////////////////////////////////////////////////////////////

struct TStorageEvents
{
    enum
    {
        START = EventSpaceBegin(NKikimr::TKikimrEvents::ES_CLOUD_STORAGE),

#define STORAGE_DECLARE_COMPONENT(component)                                   \
        component##_START,                                                     \
        component##_END = component##_START + 100,                             \
// STORAGE_DECLARE_COMPONENT

        STORAGE_ACTORS(STORAGE_DECLARE_COMPONENT)

#undef STORAGE_DECLARE_COMPONENT

        END
    };

    static_assert(END < EventSpaceEnd(NKikimr::TKikimrEvents::ES_CLOUD_STORAGE),
        "END expected to be < EventSpaceEnd(NKikimr::TKikimrEvents::ES_CLOUD_STORAGE)");
};

////////////////////////////////////////////////////////////////////////////////

struct TStoragePrivateEvents
{
    enum
    {
        START = EventSpaceBegin(NKikimr::TKikimrEvents::ES_CLOUD_STORAGE_PRIVATE),

#define STORAGE_DECLARE_COMPONENT(component)                                   \
        component##_START,                                                     \
        component##_END = component##_START + 100,                             \
// STORAGE_DECLARE_COMPONENT

        STORAGE_ACTORS(STORAGE_DECLARE_COMPONENT)

#undef STORAGE_DECLARE_COMPONENT

        END
    };

    static_assert(END < EventSpaceEnd(NKikimr::TKikimrEvents::ES_CLOUD_STORAGE_PRIVATE),
        "END expected to be < EventSpaceEnd(NKikimr::TKikimrEvents::ES_CLOUD_STORAGE_PRIVATE)");
};

////////////////////////////////////////////////////////////////////////////////

struct TStorageComponents
{
    enum
    {
        START = 3096,   // TODO

#define STORAGE_DECLARE_COMPONENT(component)                                   \
        component,                                                             \
// STORAGE_DECLARE_COMPONENT

        STORAGE_ACTORS(STORAGE_DECLARE_COMPONENT)

#undef STORAGE_DECLARE_COMPONENT

        END
    };
};

}   // namespace NCloud
