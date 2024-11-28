#pragma once

#include "public.h"

#include <cloud/blockstore/libs/encryption/public.h>
#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IRootKmsKeyProviderPtr CreateRootKmsKeyProvider(
    IRootKmsClientPtr client,
    TString keyId);

}   // namespace NCloud::NBlockStore
