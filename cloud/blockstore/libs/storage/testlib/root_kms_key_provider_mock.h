#pragma once

#include <cloud/blockstore/libs/encryption/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

IRootKmsKeyProviderPtr CreateRootKmsKeyProviderMock();

}   // namespace NCloud::NBlockStore::NStorage
