#pragma once

#include <library/cpp/string_utils/ztstrbuf/ztstrbuf.h>

#include <util/generic/fwd.h>
#include <util/stream/str.h>

#include <memory.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TXmlNodeWrapper final
{
public:
    enum ESource
    {
        FILE,
        STRING,
        ROOT_NAME,
    };

    TXmlNodeWrapper(const TString& source, ESource type);

    ~TXmlNodeWrapper();

    TXmlNodeWrapper AddChild(TZtStringBuf name, TZtStringBuf value);

    template <typename T, typename P>
    std::enable_if<
        !std::is_convertible_v<T, TZtStringBuf> ||
            !std::is_convertible_v<P, TZtStringBuf>,
        TXmlNodeWrapper>::type
    AddChild(P name, T value)
    {
        TStringStream out;
        out << name;
        auto nameStr = out.Str();
        out.Clear();
        out << value;
        return AddChild(nameStr, out.Str());
    }

    TXmlNodeWrapper& AddNamedElement(const auto& name, const auto& value)
    {
        auto cd = AddChild("cd", "");
        cd.AddChild("name", name);
        cd.AddChild("value", value);
        return *this;
    }

    TString ToString(TZtStringBuf enc = "") const;

private:
    struct TData;

    explicit TXmlNodeWrapper(std::unique_ptr<TData> data);

    std::unique_ptr<TData> Data = nullptr;
};

}   // namespace NCloud
