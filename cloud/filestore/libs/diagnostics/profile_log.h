#pragma once

#include "public.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>

#include <cloud/storage/core/libs/common/startable.h>

#include <util/datetime/base.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct IProfileLog
    : IStartable
{

    struct TRecord
    {
        TString FileSystemId;
        NProto::TProfileLogRequestInfo Request;
    };

    virtual void Write(TRecord record) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TProfileLogSettings
{
    TString FilePath;
    TDuration TimeThreshold;
    ui64 MaxFlushRecords = 0;
    ui64 MaxFrameFlushRecords = 0;
};

IProfileLogPtr CreateProfileLog(
    TProfileLogSettings settings,
    ITimerPtr timer,
    ISchedulerPtr scheduler);

IProfileLogPtr CreateProfileLogStub();

}   // namespace NCloud::NFileStore
