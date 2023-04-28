#pragma once

#include "public.h"

#include <util/datetime/base.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TThreadPark
{
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TThreadPark();
    ~TThreadPark();

    void Wait();

    // spurious wake-ups possible
    bool WaitT(TDuration timeout);
    bool WaitD(TInstant deadLine);

    void Signal();
};

}   // namespace NCloud
