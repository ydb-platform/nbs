#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/public.h>

#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

IFileSystemEventHandlerPtr CreateFileSystemEventHandler(
    TLog log,
    TString fileSystemId);

}   // namespace NCloud::NFileStore::NFuse
