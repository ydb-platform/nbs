#pragma once

#include "public.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NMetrics {

////////////////////////////////////////////////////////////////////////////////

class TLabel
{
private:
    TString Name;
    TString Value;

public:
    TLabel(TString name, TString value);

    const TString& GetName() const;
    const TString& GetValue() const;
    size_t GetHash() const;

    friend bool operator==(const TLabel& lhv, const TLabel& rhv) = default;
};

////////////////////////////////////////////////////////////////////////////////

TLabel CreateLabel(TString name, TString value);
TLabel CreateSensor(TString value);

////////////////////////////////////////////////////////////////////////////////

// WARNING: The order of TLabel in TLabels is important!!!
using TLabels = TVector<TLabel>;

}   // namespace NCloud::NFileStore::NMetrics

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NCloud::NFileStore::NMetrics::TLabel>
{
    size_t operator()(
        const NCloud::NFileStore::NMetrics::TLabel& label) const noexcept;
};

template <>
struct THash<NCloud::NFileStore::NMetrics::TLabels>
{
    size_t operator()(
        const NCloud::NFileStore::NMetrics::TLabels& labels) const noexcept;
};
