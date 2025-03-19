#pragma once

#include <util/stream/output.h>
#include <util/system/defaults.h>
#include <util/system/yassert.h>

namespace NCloud::NBlockStore::NRdma {

constexpr uintptr_t EVENT_MASK = 3;   // low bits unused because of alignment

enum EPollerEvent
{
    Completion = 0,
    Request = 1,
    Disconnect = 2,
    CancelRequest = 3,
};

template <typename T>
void* PtrEventTag(T* ptr, int event)
{
    auto tag = reinterpret_cast<uintptr_t>(ptr);
    Y_DEBUG_ABORT_UNLESS((tag & EVENT_MASK) == 0);
    return reinterpret_cast<void*>(tag | (event & EVENT_MASK));
}

template <typename T>
T* PtrFromTag(void* tag)
{
    auto ptr = reinterpret_cast<uintptr_t>(tag) & ~EVENT_MASK;
    return reinterpret_cast<T*>(ptr);
}

inline int EventFromTag(void* tag)
{
    return reinterpret_cast<uintptr_t>(tag) & EVENT_MASK;
}

}   // NCloud::NBlockStore::NRdma
