#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NNvme {

////////////////////////////////////////////////////////////////////////////////

// Calculates which lockable opcodes should be prohibited, excluding explicitly
// allowed and already prohibited opcodes.
TVector<ui8> CalculateOpcodesToLock(
    TVector<ui8> allowedOpcodes,
    TVector<ui8> lockable,
    TVector<ui8> prohibited);

}   // namespace NCloud::NBlockStore::NNvme
