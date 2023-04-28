#pragma once

#include "public.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/string.h>
#include <util/system/spinlock.h>

#include <memory>

namespace NCloud::NBlockStore::NDiscovery {

////////////////////////////////////////////////////////////////////////////////

struct TInstanceInfo
{
    enum class EStatus
    {
        Unreachable,
        Reachable,
    };

    struct TStat
    {
        TInstant Ts;
        ui64 Bytes = 0;
        ui64 Requests = 0;

        explicit operator bool() const
        {
            return !!Ts.GetValue();
        }
    };

    TString Host;
    ui16 Port = 0;
    bool IsSecurePort = false;
    TString Tag;
    EStatus Status = EStatus::Unreachable;
    TStat PrevStat;
    TStat LastStat;
    double BalancingScore = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TInstanceList
{
    TDeque<TInstanceInfo> Instances;
    TAdaptiveLock Lock;
    TAtomic HealthCheckDone = false;
};

}   // namespace NCloud::NBlockStore::NDiscovery
