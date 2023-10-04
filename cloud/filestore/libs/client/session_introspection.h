#pragma once

#include "public.h"

#include "session.h"

#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NClient {

////////////////////////////////////////////////////////////////////////////////

struct TSessionIntrospectedState
{
    ui64 MountSeqNumber = 0;
    bool ReadOnly = false;
    NThreading::TFuture<NProto::TCreateSessionResponse> CreateSessionResponse;
    NThreading::TFuture<NProto::TDestroySessionResponse> DestroySessionResponse;
};

////////////////////////////////////////////////////////////////////////////////

TSessionIntrospectedState GetSessionInternalState(const ISessionPtr& session);

}   // namespace NCloud::NFileStore::NClient
