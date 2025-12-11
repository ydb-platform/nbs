#include "logbroker.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NLogbroker {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TServiceStub final: public IService
{
public:
    TFuture<NProto::TError> Write(
        TVector<TMessage> messages,
        TInstant now) override
    {
        Y_UNUSED(messages);
        Y_UNUSED(now);

        return MakeFuture(NProto::TError());
    }

    void Start() override
    {}

    void Stop() override
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TServiceNull final: public IService
{
private:
    const ILoggingServicePtr Logging;
    TLog Log;

public:
    explicit TServiceNull(ILoggingServicePtr logging)
        : Logging(std::move(logging))
    {}

    TFuture<NProto::TError> Write(
        TVector<TMessage> messages,
        TInstant now) override
    {
        Y_UNUSED(now);

        for (const auto& m: messages) {
            STORAGE_WARN("Discard message #" << m.SeqNo);
        }

        return MakeFuture(NProto::TError());
    }

    void Start() override
    {
        Log = Logging->CreateLog("BLOCKSTORE_LOGBROKER");
    }

    void Stop() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateServiceStub()
{
    return std::make_shared<TServiceStub>();
}

IServicePtr CreateServiceNull(ILoggingServicePtr logging)
{
    return std::make_shared<TServiceNull>(std::move(logging));
}

}   // namespace NCloud::NBlockStore::NLogbroker
