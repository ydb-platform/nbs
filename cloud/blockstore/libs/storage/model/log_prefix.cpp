#include "log_prefix.h"

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TLogPrefix::TLogPrefix(
    std::initializer_list<std::pair<TString, TLogPrefix::TValue>> tags)
{
    for (const auto& [k, v]: tags) {
        Tags.emplace_back(k, v);
    }
    Rebuild();
}

void TLogPrefix::AddTags(
    std::initializer_list<std::pair<TString, TLogPrefix::TValue>> tags)
{
    for (const auto& [k, v]: tags) {
        bool updated = false;
        for (auto& [ek, ev]: Tags) {
            if (ek == k) {
                ev = v;
                updated = true;
                break;
            }
        }
        if (!updated) {
            Tags.emplace_back(k, v);
        }
    }
    Rebuild();
}

TString TLogPrefix::Get() const
{
    return CachedPrefix;
}

TString TLogPrefix::ValueToString(const TValue& val) const
{
    return std::visit(
        [](auto&& arg) -> TString
        {
            TStringBuilder sb;
            sb << arg;
            return sb;
        },
        val);
}

void TLogPrefix::Rebuild()
{
    TStringBuilder sb;
    sb << "[";

    bool first = true;
    for (const auto& [k, v]: Tags) {
        if (!first) {
            sb << " ";
        }
        sb << k << ":" << ValueToString(v);
        first = false;
    }

    sb << "]";
    CachedPrefix = sb;
}

}   // namespace NCloud::NBlockStore::NStorage
