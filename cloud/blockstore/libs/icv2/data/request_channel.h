#pragma once

#include <cloud/storage/core/libs/icv2/request_channel/request_channel.h>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct TDataResponse
{
    std::unique_ptr<NActors::IEventHandle> Event;
    std::weak_ptr<NCloud::NIcV2::IRequestChannel> SourceChannel;
};

using TDataResponseResult = TResultOrError<TDataResponse>;
using TDataResponseFuture = NThreading::TFuture<TDataResponseResult>;

////////////////////////////////////////////////////////////////////////////////

struct IDataRequestChannel
{
    virtual ~IDataRequestChannel();

    // Consume the reply once with ExtractValue(). On a protocol error, close
    // the channel from the reply if it is still alive.
    virtual TDataResponseFuture Request(
        std::unique_ptr<NActors::IEventBase> event,
        TDuration timeout) = 0;

    // Safe to repeat or call from result callbacks. Wait outside callbacks.
    // The future is ready after pending requests and their callbacks finish.
    virtual NThreading::TFuture<void> Close() = 0;
};

using IDataRequestChannelPtr = std::shared_ptr<IDataRequestChannel>;

}   // namespace NCloud::NBlockStore::NIcV2
