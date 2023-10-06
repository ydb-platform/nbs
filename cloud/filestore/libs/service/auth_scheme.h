#pragma once

#include "public.h"

#include "request.h"

#include <cloud/storage/core/libs/auth/auth_scheme.h>

#include <util/generic/bitmap.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

TPermissionList GetRequestPermissions(EFileStoreRequest requestType);

}   // namespace NCloud::NFileStore
