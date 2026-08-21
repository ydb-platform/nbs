#pragma once

#include "public.h"
#include "verbs.h"

#include <deque>

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

class TMemoryWindowsPool
{
private:
    NVerbs::IVerbsPtr Verbs;
    ibv_pd* ProtectionDomain;
    size_t Capacity;

    std::deque<NVerbs::TMemoryWindowPtr> Pool;

public:
    void Init(NVerbs::IVerbsPtr, ibv_pd* protectionDomain, size_t capacity);

    NVerbs::TMemoryWindowPtr Acquire();
    void Release(NVerbs::TMemoryWindowPtr window);
    void Clear();
};

}   // namespace NCloud::NStorage::NRdma
