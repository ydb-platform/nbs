#pragma once

#include "public.h"

#include <util/digest/multi.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TInternalRequestId
{
    ui64 ClientTabletId;
    ui64 RequestId;

    TInternalRequestId(ui64 clientTabletId, ui64 requestId)
        : ClientTabletId(clientTabletId)
        , RequestId(requestId)
    {}

    [[nodiscard]] size_t GetHash() const
    {
        return MultiHash(ClientTabletId, RequestId);
    }

    TInternalRequestId(const TInternalRequestId&) = default;
    TInternalRequestId& operator=(const TInternalRequestId&) = default;
    TInternalRequestId(TInternalRequestId&&) = default;
    TInternalRequestId& operator=(TInternalRequestId&&) = default;

    bool operator==(const TInternalRequestId& rhs) const = default;
};

struct TInternalRequestIdHash
{
    size_t operator()(const TInternalRequestId& x) const noexcept
    {
        return x.GetHash();
    }
};

}   // namespace NCloud::NFileStore::NStorage
