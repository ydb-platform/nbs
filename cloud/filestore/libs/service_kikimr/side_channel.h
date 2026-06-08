#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/context.h>

#include <cloud/filestore/public/api/protos/headers.pb.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

class ISideChannel
{
public:
    virtual ~ISideChannel() = default;

public:
    virtual bool ExecuteRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request,
        NThreading::TPromise<NProto::TReadDataResponse> response) = 0;

    virtual bool ExecuteRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request,
        NThreading::TPromise<NProto::TWriteDataResponse> response) = 0;

    virtual void Update(const NProto::TBackendInfo& backendInfo) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ISideChannelPtr CreateTCPSideChannel();

}   // namespace NCloud::NFileStore
