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

    TXmlNodeWrapper AddChild(const auto& name, const auto& value)
    {
        TStringStream out;
        out << name;
        auto nameStr = out.Str();
        out.Clear();
        out << value;
        return AddChildImpl(nameStr, out.Str());
    }

    TXmlNodeWrapper& AddNamedElement(const auto& name, const auto& value)
    {
        auto cd = AddChild("cd", "");
        cd.AddChild("name", name);
        cd.AddChild("value", value);
        return *this;
    }

    TString ToString(TString enc = "") const;

private:
    struct TImpl;

    explicit TXmlNodeWrapper(std::unique_ptr<TImpl> impl);

    std::unique_ptr<TImpl> Impl;

    TXmlNodeWrapper AddChildImpl(TString name, TString value);
};

}   // namespace NCloud
