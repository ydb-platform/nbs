#pragma once

#include "format_page.h"
#include "page_store.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct IComponent
{
    virtual ~IComponent() = default;

    [[nodiscard]] virtual TString Describe() const = 0;
    virtual NProto::TError CheckFormat(TWriteContext& writeContext) = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <ui32 MinVersion, ui32 Version>
class TComponentBase: public IComponent
{
protected:
    TFormatPage FormatPage;
    TString Description;

public:
    [[nodiscard]] TString Describe() const override
    {
        return FormatPage.Describe();
    }

    NProto::TError CheckFormat(TWriteContext& writeContext) override
    {
        return FormatPage.RegisterStart(
            MinVersion,
            Version,
            Description,
            writeContext);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDescriptionBuilder
{
private:
    TStringBuilder S;

public:
    explicit TDescriptionBuilder(const TStringBuf componentName)
    {
        S << "C=" << componentName;
    }

    TDescriptionBuilder& RegisterOffset(const TStringBuf label, ui64 offset)
    {
        S << " O[" << label << "]=" << offset;
        return *this;
    }

    TString Build()
    {
        return S;
    }
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
