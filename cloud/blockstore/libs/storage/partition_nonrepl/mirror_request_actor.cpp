#include "mirror_request_actor.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void ProcessMirrorActorError(NProto::TError& error)
{
    if (HasError(error) && error.GetCode() != E_REJECTED) {
        // We believe that all errors of the mirrored disk can be fixed by
        // repeating the request.
        error = MakeError(E_REJECTED, FormatError(error));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
