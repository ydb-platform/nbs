#include "label.h"

#include <util/digest/multi.h>
#include <util/digest/sequence.h>

namespace NCloud::NFileStore::NMetrics {

////////////////////////////////////////////////////////////////////////////////

TLabel::TLabel(TString name, TString value)
    : Name(std::move(name))
    , Value(std::move(value))
{}

const TString& TLabel::GetName() const
{
    return Name;
}

const TString& TLabel::GetValue() const
{
    return Value;
}

size_t TLabel::GetHash() const
{
    return MultiHash(Name, Value);
}

////////////////////////////////////////////////////////////////////////////////

TLabel CreateLabel(TString name, TString value)
{
    return TLabel(std::move(name), std::move(value));
}

TLabel CreateSensor(TString value)
{
    return TLabel("sensor", std::move(value));
}

}   // namespace NCloud::NFileStore::NMetrics

////////////////////////////////////////////////////////////////////////////////

size_t THash<NCloud::NFileStore::NMetrics::TLabel>::operator()(
    const NCloud::NFileStore::NMetrics::TLabel& label) const noexcept
{
    return label.GetHash();
}

size_t THash<NCloud::NFileStore::NMetrics::TLabels>::operator()(
    const NCloud::NFileStore::NMetrics::TLabels& labels) const noexcept
{
    return TSimpleRangeHash()(labels);
}
