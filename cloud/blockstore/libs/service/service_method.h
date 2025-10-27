#pragma once

#include "service.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest>
struct TBlockStoreMethodTraits
{
    static constexpr bool IsReadRequest()
    {
        return std::is_same_v<TRequest, NProto::TReadBlocksRequest> ||
               std::is_same_v<TRequest, NProto::TReadBlocksLocalRequest>;
    }

    static constexpr bool IsWriteRequest()
    {
        return std::is_same_v<TRequest, NProto::TWriteBlocksRequest> ||
               std::is_same_v<TRequest, NProto::TWriteBlocksLocalRequest> ||
               std::is_same_v<TRequest, NProto::TZeroBlocksRequest>;
    }

    static constexpr bool IsReadWriteRequest()
    {
        return IsReadRequest() || IsWriteRequest();
    }

    static TString GetName()
    {
        return GetBlockStoreRequestName(GetBlockStoreRequest<TRequest>());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest, typename TResponse>
struct TBlockStoreMethod;

template <typename TRequest>
struct TBlockStoreMethods;

// Generate for each type of request (NProto::TReadBlocksRequest,
// NProto::TWriteBlocksRequest, etc.) structure that stores the type of request
// and return value, the name and basic characteristics of the method, which are
// calculated as constexpr.
// Also add the specialization of the TBlockStoreMethods template by request
// type, so that you can take the TBlockStoreMethod specialization by having
// only the request type.
// Examples:
//  using TMethod = TBlockStoreMethod<NProto::TWriteBlocksRequest,
//                                    NProto::TWriteBlocksResponse>;
//  using TMethod = TBlockStoreWriteBlocksMethod;
//  using TMethod = TBlockStoreMethods<NProto::TWriteBlocksRequest>::TMethod;
//  TStirng methodName = TString(TMethod::Name);
//  TStirng methodName = TMethod::GetName();
//  TMethod::Execute(blockstore, std::move(callContext), std::move(request));
//  if constexpr (TMethod::IsReadWriteRequest()) {}

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                \
    template <>                                                             \
    struct TBlockStoreMethod<                                               \
        NProto::T##name##Request,                                           \
        NProto::T##name##Response>                                          \
        : public TBlockStoreMethodTraits<NProto::T##name##Request>          \
    {                                                                       \
        using TRequest = NProto::T##name##Request;                          \
        using TResponse = NProto::T##name##Response;                        \
                                                                            \
        static constexpr const char* Name = #name;                          \
                                                                            \
        [[nodiscard]] static NThreading::TFuture<NProto::T##name##Response> \
        Execute(                                                            \
            IBlockStore* blockStore,                                        \
            TCallContextPtr callContext,                                    \
            std::shared_ptr<NProto::T##name##Request> request)              \
        {                                                                   \
            return blockStore->name(                                        \
                std::move(callContext),                                     \
                std::move(request));                                        \
        }                                                                   \
    };                                                                      \
    using TBlockStore##name##Method = TBlockStoreMethod<                    \
        NProto::T##name##Request,                                           \
        NProto::T##name##Response>;                                         \
    template <>                                                             \
    struct TBlockStoreMethods<NProto::T##name##Request>                     \
    {                                                                       \
        using TMethod = TBlockStoreMethod<                                  \
            NProto::T##name##Request,                                       \
            NProto::T##name##Response>;                                     \
    };                                                                      \
    // BLOCKSTORE_DECLARE_METHOD

BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

// Inheritance from TBlockStoreImpl<> allows you to implement the processing of
// all virtual IBlockStore methods (ReadBlocks, WriteBlocks, etc.) through a
// single template method without using macros.
// Example:
// class TEndpointBase: public TBlockStoreImpl<TEndpointBase, IBlockStore>
// {
// public:
//     template <typename TMethod>
//     TFuture<typename TMethod::TResponse> Execute(
//         TCallContextPtr callContext,
//         std::shared_ptr<typename TMethod::TRequest> request)
//     {}
// };

template <typename T, typename U>
struct TBlockStoreImpl: public U
{
#define BLOCKSTORE_DECLARE_METHOD(name, ...)                        \
    NThreading::TFuture<NProto::T##name##Response> name(            \
        TCallContextPtr callContext,                                \
        std::shared_ptr<NProto::T##name##Request> request) override \
    {                                                               \
        using TMethod = TBlockStoreMethod<                          \
            NProto::T##name##Request,                               \
            NProto::T##name##Response>;                             \
        return static_cast<T*>(this)->template Execute<TMethod>(    \
            std::move(callContext),                                 \
            std::move(request));                                    \
    }                                                               \
    // BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD
};

}   // namespace NCloud::NBlockStore
