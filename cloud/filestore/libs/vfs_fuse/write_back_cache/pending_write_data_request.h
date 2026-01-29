#pragma once

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/intrlist.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TPendingWriteDataRequest
    : public TIntrusiveListItem<TPendingWriteDataRequest>
{
    const ui64 SequenceId = 0;
    std::shared_ptr<NProto::TWriteDataRequest> Request;
    NThreading::TPromise<NProto::TWriteDataResponse> Promise =
        NThreading::NewPromise<NProto::TWriteDataResponse>();

    TPendingWriteDataRequest(
        ui64 sequenceId,
        std::shared_ptr<NProto::TWriteDataRequest> request)
        : SequenceId(sequenceId)
        , Request(std::move(request))
    {}

    ~TPendingWriteDataRequest()
    {
        // Y_ABORT_UNLESS(TIntrusiveListItem<TPendingWriteDataRequest>::Empty());
    }
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
