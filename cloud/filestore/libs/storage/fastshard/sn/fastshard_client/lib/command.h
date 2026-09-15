#pragma once

#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/protobuf/util/pb_io.h>

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

    TString InputFile;
    std::unique_ptr<IInputStream> InputStream;

    TString OutputFile;
    std::shared_ptr<IOutputStream> OutputStream;

    NLastGetopt::TOpts Opts;
    std::unique_ptr<NLastGetopt::TOptsParseResultException> ParseResult;

    // Preset by tests; otherwise a TCP client to Host:Port is created
    // inside the fiber right before DoExecute.
    IStorageNodePtr Client;

    // Set by Shutdown; Run stops waiting for the fiber once it is set.
    std::atomic<bool> StopRequested{false};

    // Run gave up on the fiber because of Shutdown; the fiber may still
    // be blocked in I/O, so the command must stay alive until the
    // process exits.
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

    /**
     * Parses argv, then runs DoExecute in a silk fiber and waits for it
     * or for Shutdown, whichever comes first. FiberScheduler must be
     * initialized by the caller.
     *
     * @return - true if the request succeeded.
     * @throws NLastGetopt::TUsageException on invalid options, yexception
     *         on any failure raised by the command body.
     */
    bool Run(int argc, const char* argv[]);

    // Makes Run return false without waiting for the request to finish.
    // Safe to call from a signal handler or another thread.
    void Shutdown();

    bool IsStopped() const
    {
        return Stopped;
    }

    void PrintUsage() const
    {
        Opts.PrintUsage(GetProgramName());
    }

    void SetInputStream(std::unique_ptr<IInputStream> is);
    void SetOutputStream(std::shared_ptr<IOutputStream> os);

protected:
    // Option validation that cannot be expressed via TOpts (e.g. an
    // option required only outside --proto mode). Throws
    // NLastGetopt::TUsageException.
    virtual void CheckOpts() const
    {}

    // Runs inside a silk fiber with Client set.
    virtual bool DoExecute() = 0;

    IInputStream& GetInputStream();
    IOutputStream& GetOutputStream();

    void PrepareHeaders(NCloud::NProto::TDeviceRequestHeaders& headers) const;

    /**
     * Common response handling. In --proto mode prints the whole
     * response as protobuf text; otherwise prints the error, if any, to
     * stderr and leaves the human-readable output to the caller.
     *
     * @return - false if the response carries an error.
     */
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
    struct TFiberParams;
    static int FiberMain(TFiberParams* params) noexcept;
};

using TCommandPtr = std::shared_ptr<TCommand>;

}   // namespace NCloud::NFileStore::NStorage::NFastShard::NClient
