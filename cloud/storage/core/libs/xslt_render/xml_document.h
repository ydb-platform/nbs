#pragma once

#include <util/string/cast.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TXmlNodeWrapper final
{
public:
    enum class ESource
    {
        ROOT_NAME,
        FILE,
        STRING,
    };

    TXmlNodeWrapper(const TString& source, ESource type);

    ~TXmlNodeWrapper();

    TXmlNodeWrapper AddChild(const auto& tag, const auto& content)
    {
        return AddChildImpl(::ToString(tag), ::ToString(content));
    }

    TXmlNodeWrapper& AddNamedElement(const auto& name, const auto& value)
    {
        auto item = AddChild("item", "");
        item.AddChild("name", name);
        item.AddChild("value", value);
        return *this;
    }

    TString ToString(TString encoding = "") const;

private:
    struct TImpl;

    std::unique_ptr<TImpl> Impl;

    explicit TXmlNodeWrapper(std::unique_ptr<TImpl> impl);

    TXmlNodeWrapper AddChildImpl(TString tag, TString content);
};

}   // namespace NCloud
