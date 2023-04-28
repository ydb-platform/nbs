#include "hive_proxy.h"

namespace NCloud::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeHiveProxyServiceId()
{
    return TActorId(0, "sto-hproxy");
}

}   // namespace NCloud::NStorage
