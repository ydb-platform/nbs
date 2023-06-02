#pragma once

#include "public.h"

namespace NCloud::NBlockStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

TSpdkBuffer Allocate(size_t bytesCount, bool zeroInit);

inline TSpdkBuffer AllocateZero(size_t bytesCount)
{
    return Allocate(bytesCount, true);
}

inline TSpdkBuffer AllocateUninitialized(size_t bytesCount)
{
    return Allocate(bytesCount, false);
}

IAllocator* GetHugePageAllocator();

}   // namespace NCloud::NBlockStore::NSpdk
