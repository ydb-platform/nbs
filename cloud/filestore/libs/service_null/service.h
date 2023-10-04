#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/public.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

IFileStoreServicePtr CreateNullFileStore();

}   // namespace NCloud::NFileStore
