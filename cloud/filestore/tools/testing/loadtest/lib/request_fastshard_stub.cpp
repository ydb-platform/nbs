#include "request.h"

namespace NCloud::NFileStore::NLoadTest {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFastShardRequestGenerator final
    : public IRequestGenerator
    , public std::enable_shared_from_this<TFastShardRequestGenerator>
{
public:
    bool HasNextRequest() override
    {
        return false;
    }

    NThreading::TFuture<TCompletedRequest> ExecuteNextRequest() override
    {
        TCompletedRequest r;
        r.Error = MakeError(E_NOT_IMPLEMENTED);
        return NThreading::MakeFuture(std::move(r));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRequestGeneratorPtr CreateFastShardRequestGenerator(
    NProto::TFastShardLoadSpec spec,
    ui32 maxParallelism,
    ILoggingServicePtr logging)
{
    Y_UNUSED(spec);
    Y_UNUSED(maxParallelism);
    Y_UNUSED(logging);

    return std::make_shared<TFastShardRequestGenerator>();
}

}   // namespace NCloud::NFileStore::NLoadTest
