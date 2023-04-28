#pragma once

#include "public.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IDumpable
{
    virtual ~IDumpable() = default;

    virtual void Dump(IOutputStream& out) const = 0;
    virtual void DumpHtml(IOutputStream& out) const = 0;
};

}   // namespace NCloud::NBlockStore
