#include "utils.h"

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NNvme {

////////////////////////////////////////////////////////////////////////////////

TVector<ui8> CalculateOpcodesToLock(
    TVector<ui8> allowedOpcodes,
    TVector<ui8> lockable,
    TVector<ui8> prohibited)
{
    SortUnique(lockable);
    SortUnique(prohibited);
    SortUnique(allowedOpcodes);

    TVector<ui8> supportedOpcodesToLock;
    std::ranges::set_difference(
        lockable,
        allowedOpcodes,
        std::back_inserter(supportedOpcodesToLock));

    TVector<ui8> opcodesToLock;
    std::ranges::set_difference(
        supportedOpcodesToLock,
        prohibited,
        std::back_inserter(opcodesToLock));

    return opcodesToLock;
}

}   // namespace NCloud::NBlockStore::NNvme
