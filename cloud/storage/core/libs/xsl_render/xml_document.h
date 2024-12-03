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

class TFieldAdder {
public:
    TFieldAdder(NXml::TNode root);

    template <typename T>
    TFieldAdder AddFieldIn(TZtStringBuf name, T&& value) {
        return TFieldAdder(Root.AddChild(name, std::forward<T>(value)));
    }

    template <typename T>
    TFieldAdder& operator()(TZtStringBuf name, T&& value) {
        Root.AddChild(name, std::forward<T>(value));
        return *this;
    }

private:
    NXml::TNode Root;
};

}  // namespace NCloud::NStorage::NTNodeWrapper
