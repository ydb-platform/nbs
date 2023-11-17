#pragma once

#include "public.h"

#include <util/generic/vector.h>

////////////////////////////////////////////////////////////////////////////////

void Cleanup(
    IDev& dev,
    const TSuperBlock& sb,
    const TVector<TFreeList>& freesp,
    ui32 threads,
    bool verbose);
