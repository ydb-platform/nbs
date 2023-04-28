#pragma once

#include "public.h"

#include "auth_scheme.h"
#include "request.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IAuthProvider
{
    virtual ~IAuthProvider() = default;

    virtual bool NeedAuth(
        NProto::ERequestSource requestSource,
        const TPermissionList& permissions) = 0;

    virtual NThreading::TFuture<NProto::TError> CheckRequest(
        TCallContextPtr callContext,
        TPermissionList permissions,
        TString authToken,
        EBlockStoreRequest requestType,
        TDuration requestTimeout,
        TString diskId) = 0;
};

}   // namespace NCloud::NBlockStore
