#pragma once

#include "connection.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

struct IServingCellHostObserver: public ICellConnectionObserver
{
    // the volume is mounted, so it is known to the volume stats
    virtual void Attach(const TString& diskId) = 0;

    // the volume is unmounted
    virtual void Detach() = 0;
};

using IServingCellHostObserverPtr = std::shared_ptr<IServingCellHostObserver>;

////////////////////////////////////////////////////////////////////////////////

IServingCellHostObserverPtr CreateServingCellHostObserver(
    IVolumeStatsPtr volumeStats,
    TString cellId,
    TString clientId);

}   // namespace NCloud::NBlockStore::NCells
