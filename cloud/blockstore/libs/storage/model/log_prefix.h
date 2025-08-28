#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

#include <initializer_list>
#include <variant>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TLogPrefix
{
public:
    using TValue = std::variant<TString, int, ui64, double, bool>;

private:
    TVector<std::pair<TString, TValue>> Tags;
    TString CachedPrefix;

public:
    TLogPrefix(std::initializer_list<std::pair<TString, TValue>> tags);

    template <typename TV>
    void AddTag(const TString& key, TV&& value)
    {
        for (auto& [k, v]: Tags) {
            if (k == key) {
                v = TValue{std::forward<TV>(value)};
                Rebuild();
                return;
            }
        }
        Tags.emplace_back(key, TValue{std::forward<TV>(value)});
        Rebuild();
    }

    void AddTags(std::initializer_list<std::pair<TString, TValue>> tags);

    TString Get() const;

private:
    void Rebuild();
    TString ValueToString(const TValue& val) const;
};

}   // namespace NCloud::NBlockStore::NStorage
