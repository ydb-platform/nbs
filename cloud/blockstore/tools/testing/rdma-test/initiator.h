#pragma once

#include "private.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IRunnablePtr CreateTestInitiator(TOptionsPtr options, IStoragePtr storage);

}   // namespace NCloud::NBlockStore
