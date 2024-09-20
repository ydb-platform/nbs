#pragma once

#include "public.h"

#include <cloud/filestore/tools/testing/replay/protos/replay.pb.h>

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NReplay {

////////////////////////////////////////////////////////////////////////////////

struct ITest
{
    virtual ~ITest() = default;

    virtual NThreading::TFuture<NProto::TTestStats> Run() = 0;
};

////////////////////////////////////////////////////////////////////////////////

TString MakeTestTag(const TString& name);

ITestPtr CreateLoadTest(
    const TAppContext& ctx,
    const NProto::TLoadTest& config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IClientFactoryPtr clientFactory);

}   // namespace NCloud::NFileStore::NReplay
