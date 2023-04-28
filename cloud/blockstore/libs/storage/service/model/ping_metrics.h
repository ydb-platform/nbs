#pragma once

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TPingMetrics
{
private:
    double Bytes = 0;
    double Requests = 0;
    TInstant LastUpdateTs;

public:
    ui64 GetBytes() const
    {
        return Bytes;
    }

    ui64 GetRequests() const
    {
        return Requests;
    }

    void Update(TInstant now, TDuration halfDecay, ui64 bytes);
};

}   // namespace NCloud::NBlockStore::NStorage
