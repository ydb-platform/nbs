#pragma once

#include "public.h"

#include <util/digest/multi.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TNodeRefKey
{
    ui64 ParentNodeId;
    TString Name;

    TNodeRefKey(ui64 parentNodeId, const TString& name)
        : ParentNodeId(parentNodeId)
        , Name(name)
    {}

    size_t GetHash() const
    {
        return MultiHash(ParentNodeId, Name);
    }

    TNodeRefKey(const TNodeRefKey&) = default;
    TNodeRefKey& operator=(const TNodeRefKey&) = default;
    TNodeRefKey(TNodeRefKey&&) = default;
    TNodeRefKey& operator=(TNodeRefKey&&) = default;

    bool operator==(const TNodeRefKey& rhs) const = default;
};

struct TNodeRefKeyHash
{
    size_t operator()(const TNodeRefKey& key) const noexcept
    {
        return key.GetHash();
    }
};

}   // namespace NCloud::NFileStore::NStorage
