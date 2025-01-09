#pragma once

#include <util/generic/fwd.h>
#include <util/stream/str.h>

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
        TStringStream out;
        out << tag;
        auto tagStr = std::move(out.Str());
        out.Clear();
        out << content;
        return AddChildImpl(std::move(tagStr), std::move(out.Str()));
    }

    TXmlNodeWrapper& AddNamedElement(const auto& name, const auto& value)
    {
        auto cd = AddChild("item", "");
        cd.AddChild("name", name);
        cd.AddChild("value", value);
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
