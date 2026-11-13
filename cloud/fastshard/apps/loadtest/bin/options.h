#pragma once

#include "private.h"

#include <cloud/fastshard/apps/loadtest/lib/config.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/system/types.h>

namespace NCloud::NFastShard::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    // Storage node under test.
    TString Host = "localhost";
    ui16 Port = 0;
    // A request failing with a retriable error (E_REJECTED, E_TIMEOUT, ...)
    // is repeated, a write under the same lsn, until it succeeds or this
    // long has passed since its first attempt. Zero disables retries.
    TDuration RetryTimeout = TDuration::Seconds(30);

    // The load test to run.
    TLoadTestConfig Test;

    // Reporting.
    TString ResultsFile;
    bool Verbose = false;

    /**
     * Parses argv and validates the options.
     *
     * @throws NLastGetopt::TUsageException on invalid options.
     */
    void Parse(int argc, const char* argv[]);

    // Validates option values; Parse calls it.
    void Validate() const;
};

}   // namespace NCloud::NFastShard::NLoadTest
