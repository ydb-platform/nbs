#include "authorizer.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TActorId MakeAuthorizerServiceId()
{
    return TActorId(0, "blk-auth");
}

}   // namespace NCloud::NBlockStore::NStorage
