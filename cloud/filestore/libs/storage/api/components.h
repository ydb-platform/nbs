#pragma once

#include "public.h"

#include <cloud/storage/core/libs/kikimr/components_start.h>

#include <contrib/ydb/core/base/events.h>
#include <contrib/ydb/library/services/services.pb.h>

#include <contrib/ydb/library/actors/core/events.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_ACTORS(xxx)                                                  \
    xxx(SCHEMESHARD)                                                           \
    xxx(SERVICE)                                                               \
    xxx(SERVICE_WORKER)                                                        \
    xxx(SERVICE_PROXY)                                                         \
    xxx(TABLET)                                                                \
    xxx(TABLET_WORKER)                                                         \
    xxx(TABLET_PROXY)                                                          \
    xxx(SS_PROXY)                                                              \
    xxx(DISK_REGISTRY_PROXY)                                                   \
// FILESTORE_ACTORS

#define FILESTORE_COMPONENTS(xxx)                                              \
    xxx(SERVER)                                                                \
    xxx(TRACE)                                                                 \
    xxx(HIVE_PROXY)                                                            \
    FILESTORE_ACTORS(xxx)                                                      \
    xxx(VHOST)                                                                 \
    xxx(FUSE)                                                                  \
    xxx(CLIENT)                                                                \
    xxx(AUTH)                                                                  \
    xxx(USER_STATS)                                                            \
    xxx(TLS_CERTIFICATE_PROVIDER)                                              \
// FILESTORE_COMPONENTS

////////////////////////////////////////////////////////////////////////////////

struct TFileStoreComponents
{
    enum
    {
        START = TComponentsStart::FileStoreComponentsStart,

#define FILESTORE_DECLARE_COMPONENT(component)                                 \
        component,                                                             \
// FILESTORE_DECLARE_COMPONENT

        FILESTORE_COMPONENTS(FILESTORE_DECLARE_COMPONENT)

#undef FILESTORE_DECLARE_COMPONENT

        END
    };
};

const TString& GetComponentName(int component);

////////////////////////////////////////////////////////////////////////////////

struct TFileStoreEvents
{
    // NB: adding new event ranges should be done at the end of this enum in
    // order to maintain compatibility with existing events
    enum
    {
        START = EventSpaceBegin(NKikimr::TKikimrEvents::ES_FILESTORE),

        SCHEMESHARD_START = START + 1,
        SCHEMESHARD_END = SCHEMESHARD_START + 100,

        SERVICE_START = SCHEMESHARD_END + 1,
        SERVICE_END = SERVICE_START + 100,

        SERVICE_WORKER_START = SERVICE_END + 1,
        SERVICE_WORKER_END = SERVICE_WORKER_START + 100,

        SERVICE_PROXY_START = SERVICE_WORKER_END + 1,
        SERVICE_PROXY_END = SERVICE_PROXY_START + 100,

        TABLET_START = SERVICE_PROXY_END + 1,
        TABLET_END = TABLET_START + 100,

        TABLET_WORKER_START = TABLET_END + 1,
        TABLET_WORKER_END = TABLET_WORKER_START + 100,

        TABLET_PROXY_START = TABLET_WORKER_END + 1,
        TABLET_PROXY_END = TABLET_PROXY_START + 100,

        SS_PROXY_START = TABLET_PROXY_END + 1,
        SS_PROXY_END = SS_PROXY_START + 100,

        DISK_REGISTRY_PROXY_START = SS_PROXY_END + 1,
        DISK_REGISTRY_PROXY_END = DISK_REGISTRY_PROXY_START + 100,

        SERVICE_EXTRA_START = DISK_REGISTRY_PROXY_END + 1,
        SERVICE_EXTRA_END = SERVICE_EXTRA_START + 100,

        END = SERVICE_EXTRA_END + 1
    };

    static_assert(END < EventSpaceEnd(NKikimr::TKikimrEvents::ES_FILESTORE),
        "END expected to be < EventSpaceEnd(NKikimr::TKikimrEvents::ES_FILESTORE)");
};

////////////////////////////////////////////////////////////////////////////////

struct TFileStoreEventsPrivate
{
    // NB: adding new event ranges should be done at the end of this enum in
    // order to maintain compatibility with existing events
    enum
    {
        START = EventSpaceBegin(NKikimr::TKikimrEvents::ES_FILESTORE_PRIVATE),

        SCHEMESHARD_START = START + 1,
        SCHEMESHARD_END = SCHEMESHARD_START + 100,

        SERVICE_START = SCHEMESHARD_END + 1,
        SERVICE_END = SERVICE_START + 100,

        SERVICE_WORKER_START = SERVICE_END + 1,
        SERVICE_WORKER_END = SERVICE_WORKER_START + 100,

        SERVICE_PROXY_START = SERVICE_WORKER_END + 1,
        SERVICE_PROXY_END = SERVICE_PROXY_START + 100,

        TABLET_START = SERVICE_PROXY_END + 1,
        TABLET_END = TABLET_START + 100,

        TABLET_WORKER_START = TABLET_END + 1,
        TABLET_WORKER_END = TABLET_WORKER_START + 100,

        TABLET_PROXY_START = TABLET_WORKER_END + 1,
        TABLET_PROXY_END = TABLET_PROXY_START + 100,

        SS_PROXY_START = TABLET_PROXY_END + 1,
        SS_PROXY_END = SS_PROXY_START + 100,

        DISK_REGISTRY_PROXY_START = SS_PROXY_END + 1,
        DISK_REGISTRY_PROXY_END = DISK_REGISTRY_PROXY_START + 100,

        END = DISK_REGISTRY_PROXY_END + 1
    };

    static_assert(END < EventSpaceEnd(NKikimr::TKikimrEvents::ES_FILESTORE_PRIVATE),
        "END expected to be < EventSpaceEnd(NKikimr::TKikimrEvents::ES_FILESTORE_PRIVATE)");
};

}   // namespace NCloud::NFileStore::NStorage
