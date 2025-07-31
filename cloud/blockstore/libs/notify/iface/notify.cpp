#include "notify.h"

#include "config.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/iam/iface/client.h>
#include <cloud/storage/core/libs/iam/iface/public.h>

#include <library/cpp/json/writer/json_value.h>

#include <util/generic/overloaded.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NNotify {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TServiceStub final
    : public IService
{
public:
    void Start() override
    {}

    void Stop() override
    {}

    TFuture<NProto::TError> Notify(const TNotification& data) override
    {
        Y_UNUSED(data);

        return MakeFuture(NProto::TError());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TServiceNull final
    : public IService
{
private:
    const ILoggingServicePtr Logging;
    TLog Log;

public:
    explicit TServiceNull(ILoggingServicePtr logging)
        : Logging(std::move(logging))
    {}

    void Start() override
    {
        Log = Logging->CreateLog("BLOCKSTORE_NOTIFY");
    }

    void Stop() override
    {}

    TFuture<NProto::TError> Notify(const TNotification& data) override
    {
        STORAGE_WARN(
            "Discard notification "
            << data.Event << " user: " << data.UserId.Quote() << " cloud: "
            << data.CloudId.Quote() << " folder: " << data.FolderId.Quote());

        return MakeFuture(NProto::TError());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateServiceStub()
{
    return std::make_shared<TServiceStub>();
}

IServicePtr CreateNullService(ILoggingServicePtr logging)
{
    return std::make_shared<TServiceNull>(std::move(logging));
}

}   // namespace NCloud::NBlockStore::NNotify

////////////////////////////////////////////////////////////////////////////////

Y_DECLARE_OUT_SPEC(, NCloud::NBlockStore::NNotify::TEvent, out, event)
{
    using namespace NCloud::NBlockStore::NNotify;

    out << std::visit(
        TOverloaded{
            [&](const TDiskError&) { return "<error>"; },
            [&](const TDiskBackOnline&)
            {
                return "<back-online>";
            }},
        event);
}
