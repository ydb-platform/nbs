#pragma once

#include <cloud/filestore/libs/storage/fastshard/sn/client/client.h>
#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/system/event.h>
#include <util/system/progname.h>
#include <util/system/types.h>

#include <atomic>
#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard::NClient {

////////////////////////////////////////////////////////////////////////////////

/**
 * Base class of a fastshard-client command: one IStorageNode method
 * invoked once against a storage node. Owns the option set shared by
 * every command (target host/port, request headers, --proto and I/O
 * redirection) and the machinery that runs the command body inside a
 * silk fiber — the sn client can only be used from fiber context.
 *
 * Subclasses add their own options in the constructor, validate them in
 * CheckOpts and build/send the request in DoExecute.
 */
class TCommand
{
protected:
    TString Host;
    ui16 Port = 0;

    TString ClientId;
    ui32 RequestTimeoutMs = 0;

    // Read the request from input as protobuf text and print the whole
    // response as protobuf text instead of the human-readable output.
    bool Proto = false;

    bool Verbose = false;

    // Print connect / round trip times of the request to stderr.
    bool Timing = false;

    TString InputFile;
    std::unique_ptr<IInputStream> InputStream;

    TString OutputFile;
    std::shared_ptr<IOutputStream> OutputStream;

    NLastGetopt::TOpts Opts;

    TString ProgramName;

    // Preset by tests; otherwise a TCP client to Host:Port is created
    // inside the fiber right before DoExecute, reporting into Metrics.
    IStorageNodePtr Client;
    TStorageNodeClientMetricsPtr Metrics;

    // Wall time of the Call made by DoExecute, including connection
    // setup; valid once Called is set.
    bool Called = false;
    TDuration CallTime;

    // Set by Shutdown; Run stops waiting for the fiber once it is set.
    std::atomic<bool> StopRequested{false};

    // Run gave up on the fiber because of Shutdown or the request
    // deadline; the fiber may still be blocked in I/O, so the command
    // must not be destroyed until WaitForFiber returns (tests) or the
    // process is left via _exit (TApp).
    bool Stopped = false;

    // Outcome of the fiber, valid once Done is signalled.
    bool Result = false;
    TString Error;
    TManualEvent Done;

public:
    explicit TCommand(IStorageNodePtr client);
    virtual ~TCommand() = default;

    TCommand(const TCommand&) = delete;
    TCommand& operator=(const TCommand&) = delete;

    void ParseOpts(int argc, const char* argv[]);
    bool Run();
    void Shutdown();

    bool IsStopped() const
    {
        return Stopped;
    }

    // Blocks until the fiber has finished with this command.
    void WaitForFiber()
    {
        Done.WaitI();
    }

    void PrintUsage() const
    {
        Opts.PrintUsage(ProgramName ? ProgramName : GetProgramName());
    }

    void SetInputStream(std::unique_ptr<IInputStream> is);
    void SetOutputStream(std::shared_ptr<IOutputStream> os);

protected:
    virtual void CheckOpts() const
    {}

    virtual bool DoExecute() = 0;

    template <typename TRequest, typename TResponse>
    TResponse Call(
        TResponse (IStorageNode::*method)(TRequest),
        TRequest request)
    {
        const TInstant started = TInstant::Now();
        TResponse response = ((*Client).*method)(std::move(request));
        CallTime = TInstant::Now() - started;
        Called = true;
        return response;
    }

    IInputStream& GetInputStream();
    IOutputStream& GetOutputStream();

    void PrepareHeaders(NCloud::NProto::TDeviceRequestHeaders& headers) const;

    template <typename TResponse>
    bool HandleResponse(const TResponse& response)
    {
        if (Proto) {
            SerializeToTextFormat(response, GetOutputStream());
            return !HasError(response);
        }

        if (HasError(response)) {
            Cerr << FormatError(response.GetError()) << Endl;
            return false;
        }

        return true;
    }

private:
    void PrintTiming() const;

    struct TFiberParams;
    static int FiberMain(TFiberParams* params) noexcept;
};

using TCommandPtr = std::shared_ptr<TCommand>;

}   // namespace NCloud::NFileStore::NStorage::NFastShard::NClient
