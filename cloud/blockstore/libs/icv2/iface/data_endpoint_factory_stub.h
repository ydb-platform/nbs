#pragma once

#include "public.h"

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

// Rejects endpoint creation with E_NOT_IMPLEMENTED; does not start transport.
IDataEndpointFactoryPtr CreateDataEndpointFactoryStub();

}   // namespace NCloud::NBlockStore::NIcV2
