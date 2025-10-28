#pragma once

#include "public.h"

#include <cloud/storage/core/libs/kikimr/public.h>

#include <contrib/ydb/core/protos/tablet.pb.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TTabletBootInfo
{
    TTabletBootInfo() = default;

    TTabletBootInfo(
            NKikimrTabletBase::TTabletStorageInfo storageInfoProto,
            ui64 suggestedGeneration)
        : StorageInfoProto(std::move(storageInfoProto))
        , SuggestedGeneration(suggestedGeneration)
    {}

    NKikimrTabletBase::TTabletStorageInfo StorageInfoProto;
    ui64 SuggestedGeneration = 0;
};

}   // namespace NCloud::NStorage
