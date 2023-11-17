#pragma once

#include "public.h"

#include <util/generic/vector.h>
#include <util/stream/input.h>

////////////////////////////////////////////////////////////////////////////////

TSuperBlock ParseSuperBlock(IInputStream& stream);

TVector<TFreeList> ParseFreeSpace(IInputStream& stream);
