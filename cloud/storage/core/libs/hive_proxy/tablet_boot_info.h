#pragma once

#include "public.h"

#include <cloud/storage/core/libs/kikimr/public.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TTabletBootInfo
{
    TTabletBootInfo() = default;

    TTabletBootInfo(
            NKikimr::TTabletStorageInfoPtr storageInfo,
            ui64 suggestedGeneration)
        : StorageInfo(std::move(storageInfo))
        , SuggestedGeneration(suggestedGeneration)
    {}

    NKikimr::TTabletStorageInfoPtr StorageInfo;
    ui64 SuggestedGeneration = 0;
};

}   // namespace NCloud::NStorage
