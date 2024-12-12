#include "public.h"

#include "command.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NClient {

////////////////////////////////////////////////////////////////////////////////

TString NormalizeCommand(TString name);

TCommandPtr GetCommand(const TString& name);
TVector<TString> GetCommandNames();

}   // namespace NCloud::NFileStore::NClient
