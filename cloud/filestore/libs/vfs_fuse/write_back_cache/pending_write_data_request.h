#pragma once

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/datetime/base.h>
#include <util/generic/intrlist.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct TPendingWriteDataRequest
    : public TIntrusiveListItem<TPendingWriteDataRequest>
{
    const ui64 SequenceId;
    const TInstant Time;
    std::shared_ptr<NProto::TWriteDataRequest> Request;
    NThreading::TPromise<NProto::TWriteDataResponse> Promise =
        NThreading::NewPromise<NProto::TWriteDataResponse>();

    TPendingWriteDataRequest(
        ui64 sequenceId,
        TInstant time,
        std::shared_ptr<NProto::TWriteDataRequest> request)
        : SequenceId(sequenceId)
        , Time(time)
        , Request(std::move(request))
    {}
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
