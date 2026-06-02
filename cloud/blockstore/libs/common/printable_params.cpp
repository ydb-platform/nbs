#include "printable_params.h"

namespace NCloud::NBlockStore {

///////////////////////////////////////////////////////////////////////////////

TString PrintKeyValue(TPrintableParams keyValues)
{
    TStringBuilder sb;
    bool first = true;
    for (const auto& [key, value]: keyValues) {
        if (!first) {
            sb << " ";
        }
        sb << key;
        std::visit(
            [&sb](const auto& v)
            {
                using T = std::decay_t<decltype(v)>;
                if constexpr (!std::is_same_v<T, std::monostate>) {
                    sb << ":" << v;
                }
            },
            value);
        first = false;
    }
    return sb;
}

}   // namespace NCloud::NBlockStore
