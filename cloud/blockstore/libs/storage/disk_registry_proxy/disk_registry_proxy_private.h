#include "public.h"

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TDiskRegistryChannelKinds
{
    TString SysKind;
    TString LogKind;
    TString IndexKind;
};

}   // namespace NCloud::NBlockStore::NStorage
