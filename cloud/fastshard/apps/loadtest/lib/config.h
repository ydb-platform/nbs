#pragma once

#include "public.h"

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/system/types.h>

namespace NCloud::NFastShard::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

// Parameters of one load test. TOptions fills it from the command line
// and validates it.
struct TLoadTestConfig
{
    // Test name in the results.
    TString Name = "fastshard-loadtest";

    // Target.
    TString ClientId = "fastshard-loadtest";
    ui32 RequestTimeoutMs = 0;
    TString DeviceUUID;
    ui64 Generation = 0;
    // Skip AcquireDevices / ReleaseDevices around the run.
    bool NoAcquire = false;

    // Load shape.
    ui32 IoDepth = 1;
    // Zero means "until Requests is reached or the run is stopped".
    TDuration Duration;
    // Zero means "until Duration elapses or the run is stopped".
    ui64 Requests = 0;
    ui32 PageSize = 4096;
    // Page range [0, PageCount) of the device that requests address.
    ui64 PageCount = 1024;
    ui32 WritePages = 1;
    ui32 ReadPages = 1;
    // Share of ReadPages requests, 0..100; the rest are WriteLogRecord.
    ui32 ReadPercent = 0;
    // Every this many written records the low watermark is advanced to
    // the newest one, so the journal on the storage node does not fill
    // up. Zero disables it.
    ui64 AdvanceEvery = 256;

    // Progress is logged this often. Zero disables it.
    TDuration ReportInterval;
};

}   // namespace NCloud::NFastShard::NLoadTest
