#include "config.h"

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

bool TDataConfig::IsValid() const
{
    return MaxInflightRequests != 0 && MaxInflightBytes != 0;
}

}   // namespace NCloud::NBlockStore::NIcV2
