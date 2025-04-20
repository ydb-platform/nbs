#pragma once

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/hash.h>
#include <util/system/mutex.h>

#include <atomic>
#include <deque>
#include <functional>
#include <memory>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TSessionSequencer final
    : public std::enable_shared_from_this<TSessionSequencer>
{
private:
    const IFileStorePtr Session;

    struct TRequest
    {
        ui64 Handle = 0;
        ui64 Offset = 0;
        ui64 Length = 0;

        ui64 Id = 0;
        std::function<void()> Execute;
        bool IsRead = false;
    };

    std::atomic<ui64> NextRequestId = 0;

    THashMap<ui64, TRequest> InFlightRequests;
    std::deque<TRequest> WaitingRequests;
    TMutex Lock;

public:
    TSessionSequencer(IFileStorePtr session);

    NThreading::TFuture<NProto::TReadDataResponse> ReadData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> protoRequest);

    NThreading::TFuture<NProto::TWriteDataResponse> WriteData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> protoRequest);

private:
    bool CanExecuteRequest(const TRequest& request);

    std::optional<TRequest> TakeNextRequestToExecute();

    void OnRequestFinished(ui64 id);

    void QueueOrExecuteRequest(TRequest request);
};

}   // namespace NCloud::NFileStore::NFuse
