#pragma once

#include "public.h"

#include <cloud/blockstore/libs/encryption/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IRootKmsKeyProviderPtr CreateRootKmsKeyProvider();

}   // namespace NCloud::NBlockStore
