#pragma once

#include "public.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NProfileTool {

////////////////////////////////////////////////////////////////////////////////

TString NormalizeCommand(TString name);

TCommandPtr GetCommand(const TString& name);

TVector<TString> GetCommandNames();

}   // namespace NCloud::NFileStore::NProfileTool
