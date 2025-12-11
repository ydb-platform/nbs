#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

#include <cloud/storage/core/libs/common/startable.h>

#include <util/generic/deque.h>
#include <util/generic/string.h>

#include <utility>

namespace NCloud::NBlockStore::NDiscovery {

////////////////////////////////////////////////////////////////////////////////

struct IBanList: IStartable
{
    virtual ~IBanList() = default;

    virtual void Update() = 0;
    virtual bool IsBanned(const TString& host, ui16 port) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

IBanListPtr CreateBanList(
    TDiscoveryConfigPtr config,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring);

IBanListPtr CreateBanListStub(TDeque<std::pair<TString, ui16>> instances = {});

}   // namespace NCloud::NBlockStore::NDiscovery
