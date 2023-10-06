#include "authorizer.h"

namespace NCloud::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeAuthorizerServiceId()
{
    return TActorId(0, "blk-auth");
}

}   // namespace NCloud::NStorage
