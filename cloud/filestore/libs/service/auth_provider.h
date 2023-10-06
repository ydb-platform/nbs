#pragma once

#include "public.h"

#include "auth_scheme.h"
#include "request.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/request_source.pb.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct IAuthProvider
{
    virtual ~IAuthProvider() = default;

    virtual bool NeedAuth(
        NCloud::NProto::ERequestSource requestSource,
        const TPermissionList& permissions) = 0;

    virtual NThreading::TFuture<NProto::TError> CheckRequest(
        TCallContextPtr callContext,
        TPermissionList permissions,
        TString authToken,
        TDuration requestTimeout) = 0;
};

}   // namespace NCloud::NFileStore
