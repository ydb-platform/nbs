#pragma once

#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/system/types.h>

namespace NCloud::NFastShard::NJdServer {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    // Listen address; an empty host means every interface.
    TString Host;
    ui16 Port = 0;

    // The device: one file split into the journal metadata, the journal data
    // and the data, in this order.
    TString DevicePath;
    TString DeviceUUID = "jdserver-device";
    ui32 BlockSize = 4_KB;
    ui64 LogMetaSize = 1_GB;
    ui64 LogDataSize = 1_GB;
    ui64 DataSize = 1_GB;

    // Serve the device without a journal: writes go straight to the data,
    // the journal parts are not allocated.
    bool NoJournal = false;

    // Threads: the executor the device and the listener run on, plus this
    // many more the connections are spread over, and the AIO completion
    // threads the device file is served by.
    ui32 Executors = 1;
    ui32 AioThreads = 1;

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

    // The sizes of the parts that are allocated on the device, in bytes.
    ui64 GetLogMetaSize() const;
    ui64 GetLogDataSize() const;
    ui64 GetDeviceSize() const;
};

}   // namespace NCloud::NFastShard::NJdServer
