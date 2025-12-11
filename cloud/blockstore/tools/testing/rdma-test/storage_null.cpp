#include "storage.h"

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TNullStorage final: public IStorage
{
public:
    void Start() override
    {}
    void Stop() override
    {}

    TFuture<NProto::TError> ReadBlocks(
        TCallContextPtr callContext,
        TReadBlocksRequestPtr request,
        TGuardedSgList sglist) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        Y_UNUSED(sglist);

        return MakeFuture(NProto::TError());
    }

    TFuture<NProto::TError> WriteBlocks(
        TCallContextPtr callContext,
        TWriteBlocksRequestPtr request,
        TGuardedSgList sglist) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        Y_UNUSED(sglist);

        return MakeFuture(NProto::TError());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStoragePtr CreateNullStorage()
{
    return std::make_shared<TNullStorage>();
}

}   // namespace NCloud::NBlockStore
