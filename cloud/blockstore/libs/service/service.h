#pragma once

#include "public.h"

#include "request.h"

#include <cloud/storage/core/libs/common/startable.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IBlockStore
    : public IStartable
{
    virtual TStorageBuffer AllocateBuffer(size_t bytesCount) = 0;

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    virtual NThreading::TFuture<NProto::T##name##Response> name(               \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) = 0;                \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD
};

////////////////////////////////////////////////////////////////////////////////

// The adapter helps to execute the request via the generalized overloaded
// Execute() method.
class TBlockStoreRequestAdapter
{
public:
#define BLOCKSTORE_DECLARE_METHOD(name, ...)                       \
    static NThreading::TFuture<NProto::T##name##Response> Execute( \
        IBlockStore* blockstore,                                   \
        TCallContextPtr callContext,                               \
        std::shared_ptr<NProto::T##name##Request> request);        \
    // BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD
};

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateBlockStoreStub();

}   // namespace NCloud::NBlockStore
