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
