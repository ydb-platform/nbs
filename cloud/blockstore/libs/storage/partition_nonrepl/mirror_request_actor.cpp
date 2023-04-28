#include "mirror_request_actor.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void ProcessMirrorActorError(NProto::TError& error)
{
    if (error.GetCode() == E_IO || error.GetCode() == E_IO_SILENT) {
        error = MakeError(E_REJECTED, FormatError(error));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
