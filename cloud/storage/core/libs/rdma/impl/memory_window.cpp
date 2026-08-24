#include "memory_window.h"

#include <cloud/storage/common/libs/common/error.h>

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

void TMemoryWindowsPool::Init(
    NVerbs::IVerbsPtr verbs,
    ibv_pd* protectionDomain,
    size_t capacity)
{
    Verbs = verbs;
    ProtectionDomain = protectionDomain;
    Capacity = capacity;
}

NVerbs::TMemoryWindowPtr TMemoryWindowsPool::Acquire()
{
    if (Pool.size()) {
        auto window = std::move(Pool.front());
        Pool.pop_front();
        return window;
    }
    if (Verbs && ProtectionDomain) {
        return Verbs->CreateMemoryWindow(ProtectionDomain);
    }
}

void TMemoryWindowsPool::Release(NVerbs::TMemoryWindowPtr window)
{
    if (window && Pool.size() < Capacity) {
        Pool.emplace_back(std::move(window));
    }
}

void TMemoryWindowsPool::Clear()
{
    Pool.clear();
}

}   // namespace NCloud::NStorage::NRdma
