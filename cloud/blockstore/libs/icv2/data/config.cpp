#include "config.h"

#include <limits>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

bool TDataConfig::IsValid() const
{
    return MaxRequestBytes != 0 &&
           MaxRequestBytes <= std::numeric_limits<int>::max() &&
           MaxInflightRequests != 0 && MaxInflightBytes != 0;
}

}   // namespace NCloud::NBlockStore::NIcV2
