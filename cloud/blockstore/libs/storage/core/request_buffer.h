#pragma once

#include "public.h"

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <typename TData>
struct TRequestInBuffer
{
    size_t Weight;
    TData Data;
};

template <typename TData>
class TRequestBuffer
{
private:
    TVector<TRequestInBuffer<TData>> Requests;
    size_t Weight = 0;

public:
    void Put(TRequestInBuffer<TData> request)
    {
        Y_DEBUG_ABORT_UNLESS(request.Weight);
        Weight += request.Weight;
        Requests.push_back(std::move(request));
    }

    class TGuard
    {
    private:
        TRequestBuffer<TData>& Parent;

    public:
        TGuard(TRequestBuffer<TData>& parent)
            : Parent(parent)
        {}

        TGuard(const TGuard& rhs) = delete;
        TGuard(TGuard&& rhs) = delete;

        ~TGuard()
        {
            Parent.Weight = 0;
            Parent.Requests.clear();
        }

    public:
        TVector<TRequestInBuffer<TData>>& Get()
        {
            return Parent.Requests;
        }
    };

    TGuard Flush()
    {
        return {*this};
    }

    size_t GetWeight() const
    {
        return Weight;
    }
};

}   // namespace NCloud::NBlockStore::NStorage
