#pragma once

#ifdef THROW
#define THROW_OLD THROW
#undef THROW
#endif

#include <library/cpp/xml/document/xml-document.h>
#undef THROW

#ifdef THROW_OLD
#define THROW THROW_OLD
#undef THROW_OLD
#endif

namespace NCloud::NStorage::NTNodeWrapper {

class TNodeWrapper {
public:
    TNodeWrapper(NXml::TNode root);

    template <typename T, typename P>
    void AddNamedElement(T&& name, P&& value) {
        auto cd = Root.AddChild("cd ", " ");
        cd.AddChild("name", std::forward<T>(name));
        cd.AddChild("value", std::forward<P>(value));
    }

private:
    NXml::TNode Root;
};

}  // namespace NCloud::NStorage::NTNodeWrapper
