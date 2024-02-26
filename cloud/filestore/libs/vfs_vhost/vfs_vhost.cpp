#include "vfs.h"
#include "vfs_vhost.h"

#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/sglist_iter.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/logger/log.h>

#include <cloud/contrib/vhost/include/vhost/fs.h>
#include <cloud/contrib/vhost/include/vhost/server.h>
#include <cloud/contrib/vhost/include/vhost/types.h>
#include <cloud/contrib/vhost/logging.h>
#include <cloud/contrib/vhost/platform.h>

#include <contrib/libs/linux-headers/linux/fuse.h>
#include <contrib/libs/linux-headers/linux/uio.h>

#include <util/stream/printf.h>
#include <util/system/mutex.h>

#include <string.h>

namespace NCloud::NFileStore::NVFSVhost {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

TLog VhostLog;

ELogPriority GetLogPriority(LogLevel level)
{
    switch (level) {
        case LOG_ERROR: return TLOG_ERR;
        case LOG_WARNING: return TLOG_WARNING;
        case LOG_INFO: return TLOG_INFO;
        case LOG_DEBUG: return TLOG_DEBUG;
    }
}

void vhd_log(LogLevel level, const char* format, ...)
{
    va_list params;
    va_start(params, format);

    ELogPriority priority = GetLogPriority(level);
    if (priority <= VhostLog.FiltrationLevel()) {
        Printf(VhostLog << priority << ": ", format, params);
    }

    va_end(params);
}

////////////////////////////////////////////////////////////////////////////////

class TVfsRequestImpl final
    : public TVfsRequest
{
private:
    vhd_io* VhdIo = nullptr;

public:
    TVfsRequestImpl(vhd_io* vhdIo, void* cookie)
        : VhdIo(vhdIo)
    {
        Cookie = cookie;

        const auto& sglist = vhd_get_fs_io(VhdIo)->sglist;
        Y_DEBUG_ABORT_UNLESS(sglist.nbuffers > 0);

        TSgList in, out;
        SplitRequestBuffers(sglist, in, out);
        Y_DEBUG_ABORT_UNLESS(in.size() + out.size() == sglist.nbuffers);

        In.SetSgList(in);
        Out.SetSgList(out);
    }

    void Complete(EResult result) override
    {
        vhd_complete_bio(VhdIo, GetVhostResult(result));
    }

private:
    void SplitRequestBuffers(const vhd_sglist& sglist, TSgList& in, TSgList& out)
    {
        size_t it = 0, end = 0;
        for (; end < sglist.nbuffers && !sglist.buffers[it].write_only; ++end)
        {}

        in.reserve(end);
        for (; it < end; ++it) {
            in.emplace_back(static_cast<char*>(sglist.buffers[it].base), sglist.buffers[it].len);
        }

        for (; end < sglist.nbuffers && sglist.buffers[end].write_only; ++end)
        {}

        out.reserve(end - it);
        for (; it < end; ++it) {
            out.emplace_back(static_cast<char*>(sglist.buffers[it].base), sglist.buffers[it].len);
        }
    }

    vhd_bdev_io_result GetVhostResult(EResult result)
    {
        switch (result) {
            case SUCCESS:
                return VHD_BDEV_SUCCESS;
            case IOERR:
                return VHD_BDEV_IOERR;
            case CANCELLED:
                return VHD_BDEV_CANCELED;
        }

        Y_ABORT("Unexpected vhost result: %d", (ui32)(result));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TUnregisterCompletion
{
private:
    TPromise<NProto::TError> Result;

public:
    TUnregisterCompletion(TPromise<NProto::TError> result)
        : Result(std::move(result))
    {}

    static void Callback(void* ctx)
    {
        std::unique_ptr<TUnregisterCompletion> completion(
            reinterpret_cast<TUnregisterCompletion*>(ctx));

        completion->OnCompletion();
    }

private:
    void OnCompletion()
    {
        Result.SetValue(NProto::TError());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVfsDevice final
    : public IVfsDevice
{
private:
    vhd_request_queue* const VhdQueue;
    const TString SocketPath;
    void* const Cookie;

    vhd_fsdev_info VhdFsdevInfo;
    vhd_vdev* VhdVdev = nullptr;

    TMutex Lock;

public:
    TVfsDevice(
            vhd_request_queue* vhdQueue,
            TString socketPath,
            void* cookie)
        : VhdQueue(vhdQueue)
        , SocketPath(std::move(socketPath))
        , Cookie(cookie)
    {
        Zero(VhdFsdevInfo);
        VhdFsdevInfo.socket_path = SocketPath.c_str();
        // TODO: pass via config
        VhdFsdevInfo.num_queues = 2;
    }

    ~TVfsDevice()
    {
        Stop();
    }

    bool Start() override
    {
        auto& Log = VhostLog;

        VhdVdev = vhd_register_fs(
            &VhdFsdevInfo,
            VhdQueue,
            Cookie);

        if (!VhdVdev) {
            STORAGE_ERROR("vhd_register_fs has failed for: " << SocketPath);
            return false;
        }

        return true;
    }

    TFuture<NProto::TError> Stop() override
    {
        if (!VhdVdev) {
            return MakeFuture(MakeError(S_ALREADY));
        }

        auto result = NewPromise<NProto::TError>();

        auto& Log = VhostLog;
        STORAGE_INFO("vhd_unregister_fs starting: " << SocketPath);
        result.GetFuture().Apply(
            [socketPath = SocketPath] (const auto& future) {
                auto& Log = VhostLog;
                STORAGE_INFO("vhd_unregister_fs completed: " << socketPath);
                return future;
            });

        auto completion = std::make_unique<TUnregisterCompletion>(result);
        with_lock (Lock) {
            vhd_unregister_fs(
                VhdVdev,
                TUnregisterCompletion::Callback,
                completion.release());
            VhdVdev = nullptr;
        }

        return result.GetFuture();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVhostQueue final
    : public IVfsQueue
{
private:
    TLog Log;
    vhd_request_queue* VhdRequestQueue = nullptr;

public:
    TVhostQueue()
        : Log(VhostLog)
    {
        VhdRequestQueue = vhd_create_request_queue();
    }

    ~TVhostQueue()
    {
        vhd_release_request_queue(VhdRequestQueue);
    }

    int Run() override
    {
        return vhd_run_queue(VhdRequestQueue);
    }

    void Stop() override
    {
        vhd_stop_queue(VhdRequestQueue);
    }

    IVfsDevicePtr CreateDevice(
        TString socketPath,
        void* cookie) override
    {
        return std::make_shared<TVfsDevice>(
            VhdRequestQueue,
            std::move(socketPath),
            cookie);
    }

    TVfsRequestPtr DequeueRequest() override
    {
        vhd_request vhdRequest;

        while (vhd_dequeue_request(VhdRequestQueue, &vhdRequest)) {
            auto vhostRequest = CreateVhostRequest(vhdRequest);
            if (!vhostRequest) {
                vhd_complete_bio(vhdRequest.io, VHD_BDEV_IOERR);
                continue;
            }

            return vhostRequest;
        }

        return nullptr;
    }

private:
    TVfsRequestPtr CreateVhostRequest(const vhd_request& vhdRequest)
    {
        void* cookie = vhd_vdev_get_priv(vhdRequest.vdev);
        return std::make_shared<TVfsRequestImpl>(
            vhdRequest.io,
            cookie);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVhostQueueFactory final
    : public IVfsQueueFactory
{
public:
    TVhostQueueFactory()
    {
        int res = vhd_start_vhost_server(vhd_log);
        Y_ABORT_UNLESS(res == 0, "Error starting vhost server");
    }

    ~TVhostQueueFactory()
    {
        vhd_stop_vhost_server();
    }

    IVfsQueuePtr CreateQueue() override
    {
        return std::make_shared<TVhostQueue>();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IVfsQueueFactoryPtr CreateVhostQueueFactory(ILoggingServicePtr logging)
{
    struct TInitializer
    {
        const IVfsQueueFactoryPtr Factory = std::make_shared<TVhostQueueFactory>();

        TInitializer(ILoggingServicePtr logging)
        {
            VhostLog = logging->CreateLog("NFS_VHOST");
        }
    };

    static const TInitializer initializer(logging);
    return initializer.Factory;
}

}   // namespace NCloud::NFileStore::NVFSVhost
