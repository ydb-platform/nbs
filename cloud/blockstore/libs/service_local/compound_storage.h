#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/public.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateCompoundStorage(
    TVector<IStoragePtr> storages,
    TVector<ui64> offsets,
    ui32 blockSize,
    TString diskId,
    TString clientId,
    IServerStatsPtr serverStats);

}   // namespace NCloud::NBlockStore::NServer
