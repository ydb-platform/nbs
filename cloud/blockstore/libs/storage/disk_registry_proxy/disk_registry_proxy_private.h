#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TDiskRegistryChannelKinds
{
    TString SysKind;
    TString LogKind;
    TString IndexKind;
};

////////////////////////////////////////////////////////////////////////////////

struct TEvDiskRegistryProxyPrivate
{
    //
    // LookupTabletRequest
    //

    struct TLookupTabletRequest
    {
    };

    //
    // CreateTabletRequest
    //

    struct TCreateTabletRequest
    {
        TDiskRegistryChannelKinds Kinds;

        explicit TCreateTabletRequest(TDiskRegistryChannelKinds kinds)
            : Kinds(std::move(kinds))
        {}
    };

    //
    // DiskRegistryCreateResult notification
    //

    struct TDiskRegistryCreateResult
    {
        const ui64 TabletId = 0;
        TDiskRegistryChannelKinds Kinds;

        TDiskRegistryCreateResult(
            ui64 tabletId,
            TDiskRegistryChannelKinds kinds)
            : TabletId(tabletId)
            , Kinds(std::move(kinds))
        {}
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStorePrivateEvents::DISK_REGISTRY_PROXY_START,

        EvDiskRegistryCreateResult,

        EvLookupTabletRequest,

        EvCreateTabletRequest,

        EvEnd
    };

    static_assert(
        EvEnd < (int)TBlockStorePrivateEvents::DISK_REGISTRY_PROXY_END,
        "EvEnd expected to be < TBlockStorePrivateEvents::DISK_REGISTRY_END");

    using TEvDiskRegistryCreateResult =
        TResponseEvent<TDiskRegistryCreateResult, EvDiskRegistryCreateResult>;

    using TEvLookupTabletRequest =
        TRequestEvent<TLookupTabletRequest, EvLookupTabletRequest>;

    using TEvCreateTabletRequest =
        TRequestEvent<TCreateTabletRequest, EvCreateTabletRequest>;
};

}   // namespace NCloud::NBlockStore::NStorage
