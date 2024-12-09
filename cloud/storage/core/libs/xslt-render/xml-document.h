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

namespace NCloud::NNodeWrapper {

class TNodeWrapper
{
public:
    explicit TNodeWrapper(NXml::TNode root);

    TNodeWrapper& AddNamedElement(auto&& name, auto&& value)
    {
        auto cd = Root.AddChild("cd");
        cd.AddChild("name", std::forward<decltype(name)>(name));
        cd.AddChild("value", std::forward<decltype(value)>(value));
        return *this;
    }

private:
    NXml::TNode Root;
};

}   // namespace NCloud::NNodeWrapper
