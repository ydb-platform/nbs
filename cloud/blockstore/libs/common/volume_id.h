#pragma once

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/str_stl.h>

#include <memory>
#include <tuple>
#include <type_traits>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TVolumeId
{
    TString DiskId;
    TString CloudId;
    TString FolderId;
};

using TVolumeIdPtr = std::shared_ptr<TVolumeId>;
using TVolumeIdConstPtr = std::shared_ptr<const TVolumeId>;

inline bool operator==(const TVolumeId& lhs, const TVolumeId& rhs)
{
    return std::tie(lhs.DiskId, lhs.CloudId, lhs.FolderId) ==
           std::tie(rhs.DiskId, rhs.CloudId, rhs.FolderId);
}

inline bool operator<(const TVolumeId& lhs, const TVolumeId& rhs)
{
    return std::tie(lhs.DiskId, lhs.CloudId, lhs.FolderId) <
           std::tie(rhs.DiskId, rhs.CloudId, rhs.FolderId);
}

TVolumeIdPtr MakeVolumeId(
    const TString& diskId,
    const TString& cloudId,
    const TString& folderId);

}   // namespace NCloud::NBlockStore

template <>
struct THash<NCloud::NBlockStore::TVolumeId>
{
    size_t operator()(const NCloud::NBlockStore::TVolumeId& val) const
    {
        auto a = std::tie(val.DiskId, val.CloudId, val.FolderId);
        return THash<std::decay_t<decltype(a)>>{}(a);
    }
};
