#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/system/types.h>

namespace NCloud::NFastShard::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    // Target.
    TString Host = "localhost";
    ui16 Port = 0;
    TString ClientId = "fastshard-loadtest";
    ui32 RequestTimeoutMs = 0;
    TString DeviceUUID;
    ui64 Generation = 0;
    // Skip AcquireDevices / ReleaseDevices around the run.
    bool NoAcquire = false;

    // Load shape.
    ui32 IoDepth = 1;
    // Zero means "until --requests is reached or the run is stopped".
    TDuration Duration;
    // Zero means "until --duration elapses or the run is stopped".
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

    // Reporting.
    TString Name = "fastshard-loadtest";
    TString ResultsFile;
    TDuration ReportInterval;
    bool Verbose = false;

    /**
     * Parses argv and validates the options.
     *
     * @throws NLastGetopt::TUsageException on invalid options.
     */
    void Parse(int argc, const char* argv[]);

    // Validates option values; Parse calls it, tests that fill the struct
    // by hand call it directly.
    void Validate() const;
};

}   // namespace NCloud::NFastShard::NLoadTest
