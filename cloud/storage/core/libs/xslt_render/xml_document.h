#pragma once

#include <util/generic/fwd.h>
#include <util/string/cast.h>

#include <memory.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TXmlNodeWrapper final
{
public:
    enum class ESource
    {
        FILE,
        STRING,
        ROOT_NAME,
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

    TString ToString(TString enc = "") const;

private:
    struct TImpl;

    explicit TXmlNodeWrapper(std::unique_ptr<TImpl> impl);

    std::unique_ptr<TImpl> Impl;

    TXmlNodeWrapper AddChildImpl(TString tag, TString content);
};

}   // namespace NCloud
