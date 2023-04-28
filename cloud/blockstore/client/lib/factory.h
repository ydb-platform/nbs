#pragma once

#include "command.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TCommandPtr GetHandler(const TString& name, IBlockStorePtr client = {});

TVector<TString> GetHandlerNames();

}   // namespace NCloud::NBlockStore::NClient
