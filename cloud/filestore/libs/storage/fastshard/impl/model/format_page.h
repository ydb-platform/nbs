#pragma once

#include "page_store.h"

#include <cloud/filestore/libs/service/error.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////
// format page layout

struct TFormatPageSlot
{
    ui64 Generation = 0;
    ui64 PageNo = 0;
    ui32 MinVersion = 0;
    ui32 Version = 0;
};

static_assert(sizeof(TFormatPageSlot) <= DefaultBlockSize);

////////////////////////////////////////////////////////////////////////////////

class TFormatPage
{
private:
    TFormatPageSlot Slot;
    IPageStorePtr PageStore;

public:
    ui64 Init(ui64 pageNo, IPageStorePtr pageStore);

    NProto::TError RegisterStart(
        ui32 minVersion,
        ui32 version,
        TWriteContext& writeContext);
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
