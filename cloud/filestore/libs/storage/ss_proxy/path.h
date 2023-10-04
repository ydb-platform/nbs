#pragma once

#include "public.h"

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TString ComputeFolder(const TString& fileSystemId);
TString GetFileSystemPath(const TString& rootPath, const TString& fileSystemId);

}   // namespace NCloud::NFileStore::NStorage
