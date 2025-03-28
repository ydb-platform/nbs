#pragma once

#include "common_constants.h"
#include "request_in_progress_impl.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

class TEmptyType
{
};

///////////////////////////////////////////////////////////////////////////////

class IRequestsInProgress
{
public:
    virtual ~IRequestsInProgress() = default;

    [[nodiscard]] virtual bool WriteRequestInProgress() const = 0;

    virtual void WaitForInFlightWrites() = 0;
    [[nodiscard]] virtual bool IsWaitingForInFlightWrites() const = 0;
};

///////////////////////////////////////////////////////////////////////////////

template <typename TValue = TEmptyType>
struct TIsWriteValueWrapper
{
    TValue Value;
    bool IsWrite = false;
};

template <EAllowedRequests TKind, typename TKey, typename TValue = TEmptyType>
class TRequestsInProgress
    : public IRequestsInProgress
    , private TRequestsInProgressImpl<TKey, TIsWriteValueWrapper<TValue>>
{
public:
    using TRequest = TIsWriteValueWrapper<TValue>;

private:
    using TImpl = TRequestsInProgressImpl<TKey, TRequest>;

public:
    ~TRequestsInProgress() = default;

    using TImpl::AllRequests;
    using TImpl::Empty;
    using TImpl::GenerateRequestId;
    using TImpl::GetRequestCount;
    using TImpl::SetRequestIdentityKey;

    void AddReadRequest(const TKey& key, TValue value = {})
        requires(IsReadAllowed<TKind>)
    {
        TImpl::AddRequest(
            key,
            TRequest{.Value = std::move(value), .IsWrite = false});
    }

    TKey AddReadRequest(TValue value)
        requires(IsReadAllowed<TKind>)
    {
        return TImpl::AddRequest(
            TRequest{.Value = std::move(value), .IsWrite = false});
    }

    void AddWriteRequest(const TKey& key, TValue value = {})
        requires(IsWriteAllowed<TKind>)
    {
        TImpl::AddRequest(
            key,
            TRequest{.Value = std::move(value), .IsWrite = true});
    }

    TKey AddWriteRequest(TValue value)
        requires(IsWriteAllowed<TKind>)
    {
        return TImpl::AddRequest(
            TRequest{.Value = std::move(value), .IsWrite = true});
    }

    TValue GetRequest(const TKey& key) const
    {
        return TImpl::GetRequest(key).Value;
    }

    bool RemoveRequest(const TKey& key)
    {
        return ExtractRequest(key).has_value();
    }

    std::optional<TValue> ExtractRequest(const TKey& key)
    {
        auto maybeRequest = TImpl::ExtractRequest(key);
        if (!maybeRequest) {
            return std::nullopt;
        }
        return maybeRequest->Value;
    }

    // IRequestsInProgress

    [[nodiscard]] bool WriteRequestInProgress() const override
    {
        return TImpl::WriteRequestInProgress();
    }

    void WaitForInFlightWrites() override
    {
        TImpl::WaitForInFlightWrites();
    }

    [[nodiscard]] bool IsWaitingForInFlightWrites() const override
    {
        return TImpl::IsWaitingForInFlightWrites();
    }
};

}   // namespace NCloud::NBlockStore::NStorage
