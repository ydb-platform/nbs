#include "device.h"

#include "address.h"
#include "device_proxy.h"
#include "device_wrapper.h"
#include "env_impl.h"
#include "histogram.h"
#include "spdk.h"
#include "target.h"

#include <util/generic/guid.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NSpdk {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

static constexpr size_t NVME_MAX_BDEVS_PER_RPC = 32;

////////////////////////////////////////////////////////////////////////////////

struct TRegisterNVMeCompletion
{
    TString BaseName;
    TString TransportId;
    TPromise<TVector<TString>> Result;

    const char* Names[NVME_MAX_BDEVS_PER_RPC] = {};
    static const ui32 Count = NVME_MAX_BDEVS_PER_RPC;

    TRegisterNVMeCompletion(
            TString baseName,
            TString transportId,
            TPromise<TVector<TString>> result)
        : BaseName(std::move(baseName))
        , TransportId(std::move(transportId))
        , Result(std::move(result))
    {}

    void OnCompletion(size_t bdev_count, int error)
    {
        if (error) {
            Result.SetException(TStringBuilder()
                << "unable to probe for NVMe devices on " << TransportId.Quote()
                << ": " << strerror(error) << " (" << error << ")");
            return;
        }

        TVector<TString> result;
        result.reserve(bdev_count);

        for (size_t i = 0; i < bdev_count; ++i) {
            result.push_back(Names[i]);
        }

        Result.SetValue(std::move(result));
    }

    static void Callback(void* ctx, size_t bdev_count, int error)
    {
        std::unique_ptr<TRegisterNVMeCompletion> completion(
            reinterpret_cast<TRegisterNVMeCompletion*>(ctx));

        completion->OnCompletion(bdev_count, error);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRegisterSCSICompletion
{
    TString Url;
    TPromise<TString> Result;

    TRegisterSCSICompletion(TString url, TPromise<TString> result)
        : Url(std::move(url))
        , Result(std::move(result))
    {}

    void OnCompletion(spdk_bdev* bdev, int error)
    {
        if (error) {
            Result.SetException(TStringBuilder()
                << "unable to probe for iSCSI device on " << Url.Quote());
        } else {
            Result.SetValue(spdk_bdev_get_name(bdev));
        }
    }

    static void Callback(void* ctx, spdk_bdev* bdev, int error)
    {
        std::unique_ptr<TRegisterSCSICompletion> completion(
            reinterpret_cast<TRegisterSCSICompletion*>(ctx));

        completion->OnCompletion(bdev, error);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TUnregisterCompletion
{
    spdk_bdev* Dev;
    TPromise<void> Result;

    TUnregisterCompletion(spdk_bdev* bdev, TPromise<void> result)
        : Dev(bdev)
        , Result(std::move(result))
    {}

    void OnCompletion(int error)
    {
        if (error) {
            Result.SetException(TStringBuilder()
                << "unable to unregister device " << spdk_bdev_get_name(Dev));
        } else {
            Result.SetValue();
        }
    }

    static void Callback(void* ctx, int error)
    {
        std::unique_ptr<TUnregisterCompletion> completion(
            reinterpret_cast<TUnregisterCompletion*>(ctx));

        completion->OnCompletion(error);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TIOCompletion
{
    spdk_bdev* Dev;
    TPromise<NProto::TError> Result;
    TSgList SglistHolder;

    TIOCompletion(spdk_bdev* bdev, TPromise<NProto::TError> result)
        : Dev(bdev)
        , Result(std::move(result))
    {}

    iovec* HoldSgList(TSgList sglist)
    {
        SglistHolder = std::move(sglist);

        static_assert(sizeof(TBlockDataRef) == sizeof(iovec));
        return reinterpret_cast<iovec*>(SglistHolder.data());
    }

    void OnCompletion(spdk_bdev_io* bdev_io, bool success)
    {
        spdk_bdev_free_io(bdev_io);

        NProto::TError error;
        if (!success) {
            error.SetCode(E_IO);
            error.SetMessage(TStringBuilder()
                << "unable to read/write to device " << spdk_bdev_get_name(Dev));
        }

        Result.SetValue(std::move(error));
    }

    static void Callback(spdk_bdev_io* bdev_io, bool success, void* ctx)
    {
        std::unique_ptr<TIOCompletion> completion(
            reinterpret_cast<TIOCompletion*>(ctx));

        completion->OnCompletion(bdev_io, success);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TQosCompletion
{
    spdk_bdev* Dev;
    TPromise<void> Result;

    TQosCompletion(
            spdk_bdev* bdev,
            TPromise<void> result)
        : Dev(bdev)
        , Result(std::move(result))
    {}

    void OnCompletion(int status)
    {
        if (status) {
            Result.SetException(TStringBuilder()
                << "unable to set QoS limits to device "
                << spdk_bdev_get_name(Dev));
        } else {
            Result.SetValue();
        }
    }

    static void Callback(void* cb_arg, int status)
    {
        std::unique_ptr<TQosCompletion> completion(
            reinterpret_cast<TQosCompletion*>(cb_arg));

        completion->OnCompletion(status);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TIoStatsCompletion
{
    spdk_bdev* Dev;
    TPromise<TDeviceIoStats> Result;
    spdk_bdev_io_stat NativeStats = {};

    TIoStatsCompletion(
            spdk_bdev* bdev,
            TPromise<TDeviceIoStats> result)
        : Dev(bdev)
        , Result(std::move(result))
    {}

    void OnCompletion(int status)
    {
        if (status) {
            Result.SetException(TStringBuilder()
                << "unable to get IO stats for device "
                << spdk_bdev_get_name(Dev));
        } else {
            Result.SetValue(TDeviceIoStats {
                .BytesRead = NativeStats.bytes_read,
                .NumReadOps = NativeStats.num_read_ops,
                .BytesWritten = NativeStats.bytes_written,
                .NumWriteOps = NativeStats.num_write_ops
            });
        }
    }

    static void Callback(
        spdk_bdev* bdev,
        spdk_bdev_io_stat* stat,
        void* cb_arg,
        int rc)
    {
        Y_UNUSED(bdev);
        Y_UNUSED(stat);

        std::unique_ptr<TIoStatsCompletion> completion(
            reinterpret_cast<TIoStatsCompletion*>(cb_arg));

        completion->OnCompletion(rc);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TEnableHistogramCompletion
{
    spdk_bdev* Dev;
    TPromise<void> Result;

    TEnableHistogramCompletion(
            spdk_bdev* bdev,
            TPromise<void> result)
        : Dev(bdev)
        , Result(std::move(result))
    {}

    void OnCompletion(int status)
    {
        if (status) {
            Result.SetException(TStringBuilder()
                << "unable to change histogram collecting state for device "
                << spdk_bdev_get_name(Dev));
        } else {
            Result.SetValue();
        }
    }

    static void Callback(void* cb_arg, int status)
    {
        std::unique_ptr<TEnableHistogramCompletion> completion(
            reinterpret_cast<TEnableHistogramCompletion*>(cb_arg));

        completion->OnCompletion(status);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TGetHistogramCompletion
{
    spdk_bdev* Dev;
    TPromise<TVector<TBucketInfo>> Result;
    THistogramPtr Histogram;

    TGetHistogramCompletion(
            spdk_bdev* bdev,
            TPromise<TVector<TBucketInfo>> result)
        : Dev(bdev)
        , Result(std::move(result))
        , Histogram(CreateHistogram())
    {}

    void OnCompletion(int status)
    {
        if (status) {
            Result.SetException(TStringBuilder()
                << "unable to get histogram data for device "
                << spdk_bdev_get_name(Dev));
            return;
        }

        Result.SetValue(CollectBuckets(*Histogram, spdk_get_ticks_hz()));
    }

    static void Callback(
        void* ctx,
        int status,
        spdk_histogram_data* histogram)
    {
        Y_UNUSED(histogram);

        std::unique_ptr<TGetHistogramCompletion> completion(
            reinterpret_cast<TGetHistogramCompletion*>(ctx));

        completion->OnCompletion(status);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TCtrlrFormatCompletion
{
    spdk_bdev* Dev;
    TPromise<NProto::TError> Result;

    TCtrlrFormatCompletion(
            spdk_bdev* bdev,
            TPromise<NProto::TError> result)
        : Dev(bdev)
        , Result(std::move(result))
    {}

    void OnCompletion(const spdk_nvme_cpl* cpl)
    {
        if (spdk_nvme_cpl_is_error(cpl)) {
            Result.SetException(TStringBuilder()
                << "unable to format device " << spdk_bdev_get_name(Dev));
            return;
        }

        Result.SetValue(NProto::TError());
    }

    static void Callback(
        void* arg,
        const spdk_nvme_cpl* cpl)
    {
        std::unique_ptr<TCtrlrFormatCompletion> completion(
            static_cast<TCtrlrFormatCompletion*>(arg));

        completion->OnCompletion(cpl);
    }
};

nvme_bdev_ns* NvmeNamespaceFromBdev(spdk_bdev* bdev)
{
    return SPDK_CONTAINEROF(bdev, nvme_bdev, disk)->nvme_ns;
}

void EventCallback(
    enum spdk_bdev_event_type type,
    spdk_bdev *bdev,
    void* ctx)
{
    Y_UNUSED(type);
    Y_UNUSED(bdev);
    Y_UNUSED(ctx);
}

spdk_nvme_transport_id ParseTransportId(const TString& transportId)
{
    spdk_nvme_transport_id trid;
    Zero(trid);

    int error = spdk_nvme_transport_id_parse(&trid, transportId.c_str());
    if (error) {
        ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
            << "unable to parse NVMe transport ID " << transportId.Quote();
    }

    if (!spdk_nvme_transport_available(trid.trtype)) {
        ythrow TServiceError(E_NOT_IMPLEMENTED)
            << "unable to parse NVMe transport ID " << transportId.Quote()
            << " - unsupported transport type "
            << spdk_nvme_transport_id_trtype_str(trid.trtype);
    }

    return trid;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TSpdkDevice final
    : public ISpdkDevice
    , public std::enable_shared_from_this<TSpdkDevice>
{
private:
    const TSpdkEnvPtr Env;
    spdk_bdev* const Dev;
    spdk_bdev_desc* const Desc;

    TVector<spdk_io_channel*> Channels;
    TFuture<void> StopResult;

public:
    TSpdkDevice(TSpdkEnvPtr env, spdk_bdev* bdev, spdk_bdev_desc* desc)
        : Env(std::move(env))
        , Dev(bdev)
        , Desc(desc)
    {
        Channels.resize(spdk_env_get_core_count());
    }

    ~TSpdkDevice()
    {
        Y_VERIFY_DEBUG(StopResult.HasValue());
    }

    void Start() override;
    void Stop() override;

    TFuture<void> StartAsync() override;
    TFuture<void> StopAsync() override;

    TFuture<NProto::TError> Read(
        void* buf,
        ui64 fileOffset,
        ui32 bytesCount) override;

    TFuture<NProto::TError> Read(
        TSgList sglist,
        ui64 fileOffset,
        ui32 bytesCount) override;

    TFuture<NProto::TError> Write(
        void* buf,
        ui64 fileOffset,
        ui32 bytesCount) override;

    TFuture<NProto::TError> Write(
        TSgList sglist,
        ui64 fileOffset,
        ui32 bytesCount) override;

    TFuture<NProto::TError> WriteZeroes(
        ui64 fileOffset,
        ui32 bytesCount) override;

    TFuture<NProto::TError> Erase(
        NProto::EDeviceEraseMethod method) override;

private:
    spdk_io_channel* GetChannel();
    TFuture<void> ReleaseChannels();

    TFuture<NProto::TError> ZeroFill();
    TFuture<NProto::TError> SecureErase(spdk_nvme_secure_erase_setting ses);
};

////////////////////////////////////////////////////////////////////////////////

void TSpdkDevice::Start()
{
    StartAsync().GetValue(StartTimeout);
}

void TSpdkDevice::Stop()
{
    StopAsync().GetValue(StartTimeout);
}

TFuture<void> TSpdkDevice::StartAsync()
{
    return MakeFuture();
}

TFuture<void> TSpdkDevice::StopAsync()
{
    if (!StopResult.HasValue()) {
        auto device = shared_from_this();
        StopResult = ReleaseChannels().Apply([=] (const auto&) {
            return Env->Execute([=] {
                spdk_bdev_close(device->Desc);
            });
        });
    }
    return StopResult;
}

spdk_io_channel* TSpdkDevice::GetChannel()
{
    int index = rte_lcore_index(-1);
    Y_VERIFY(index >= 0);

    auto* channel = Channels[index];
    if (Y_UNLIKELY(!channel)) {
        channel = spdk_bdev_get_io_channel(Desc);
        if (!channel) {
            ythrow TServiceError(E_FAIL)
                << "unable to get I/O channel for device "
                << spdk_bdev_get_name(Dev);
        }
        Channels[index] = channel;
    }
    return channel;
}

TFuture<void> TSpdkDevice::ReleaseChannels()
{
    TVector<TFuture<void>> futures;

    for (size_t i = 0; i < Channels.size(); ++i) {
        auto future = Env->GetTaskQueue(i)->Execute([=] {
            int index = rte_lcore_index(-1);
            Y_VERIFY(index >= 0);

            auto* channel = Channels[index];
            if (channel) {
                spdk_put_io_channel(channel);
                Channels[index] = nullptr;
            }
        });
        futures.push_back(std::move(future));
    }

    return WaitAll(futures);
}

TFuture<NProto::TError> TSpdkDevice::Read(
    void* buf,
    ui64 fileOffset,
    ui32 bytesCount)
{
    int index = Env->PickCore();
    return Env->GetTaskQueue(index)->Execute([=] {
        auto result = NewPromise<NProto::TError>();

        auto completion = std::make_unique<TIOCompletion>(Dev, result);
        int error = spdk_bdev_read(
            Desc, GetChannel(), buf, fileOffset, bytesCount,
            TIOCompletion::Callback, completion.get());

        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to read from device " << spdk_bdev_get_name(Dev);
        }

        completion.release();   // will be deleted in callback
        return result.GetFuture();
    });
}

TFuture<NProto::TError> TSpdkDevice::Read(
    TSgList sglist,
    ui64 fileOffset,
    ui32 bytesCount)
{
    int index = Env->PickCore();
    return Env->GetTaskQueue(index)->Execute([=] () mutable {
        auto result = NewPromise<NProto::TError>();

        auto completion = std::make_unique<TIOCompletion>(Dev, result);
        auto iovSize = sglist.size();
        auto* iovData = completion->HoldSgList(std::move(sglist));

        int error = spdk_bdev_readv(
            Desc, GetChannel(), iovData, iovSize, fileOffset, bytesCount,
            TIOCompletion::Callback, completion.get());

        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to read from device " << spdk_bdev_get_name(Dev);
        }

        completion.release();   // will be deleted in callback
        return result.GetFuture();
    });
}

TFuture<NProto::TError> TSpdkDevice::Write(
    void* buf,
    ui64 fileOffset,
    ui32 bytesCount)
{
    int index = Env->PickCore();
    return Env->GetTaskQueue(index)->Execute([=] {
        auto result = NewPromise<NProto::TError>();

        auto completion = std::make_unique<TIOCompletion>(Dev, result);
        int error = spdk_bdev_write(
            Desc, GetChannel(), buf, fileOffset, bytesCount,
            TIOCompletion::Callback, completion.get());

        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to write to device " << spdk_bdev_get_name(Dev);
        }

        completion.release();   // will be deleted in callback
        return result.GetFuture();
    });
}

TFuture<NProto::TError> TSpdkDevice::Write(
    TSgList sglist,
    ui64 fileOffset,
    ui32 bytesCount)
{
    int index = Env->PickCore();
    return Env->GetTaskQueue(index)->Execute([=] () mutable {
        auto result = NewPromise<NProto::TError>();

        auto completion = std::make_unique<TIOCompletion>(Dev, result);
        auto iovSize = sglist.size();
        auto* iovData = completion->HoldSgList(std::move(sglist));

        int error = spdk_bdev_writev(
            Desc, GetChannel(), iovData, iovSize, fileOffset, bytesCount,
            TIOCompletion::Callback, completion.get());

        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to write to device " << spdk_bdev_get_name(Dev);
        }

        completion.release();   // will be deleted in callback
        return result.GetFuture();
    });
}

TFuture<NProto::TError> TSpdkDevice::WriteZeroes(
    ui64 fileOffset,
    ui32 bytesCount)
{
    int index = Env->PickCore();
    return Env->GetTaskQueue(index)->Execute([=] {
        auto result = NewPromise<NProto::TError>();

        auto completion = std::make_unique<TIOCompletion>(Dev, result);
        int error = spdk_bdev_write_zeroes(
            Desc, GetChannel(), fileOffset, bytesCount,
            TIOCompletion::Callback, completion.get());

        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to write to device " << spdk_bdev_get_name(Dev);
        }

        completion.release();   // will be deleted in callback
        return result.GetFuture();
    });
}

TFuture<NProto::TError> TSpdkDevice::Erase(NProto::EDeviceEraseMethod method)
{
    switch (method) {
    case NProto::DEVICE_ERASE_METHOD_ZERO_FILL:
        return ZeroFill();

    case NProto::DEVICE_ERASE_METHOD_USER_DATA_ERASE:
        return SecureErase(SPDK_NVME_FMT_NVM_SES_USER_DATA_ERASE);

    case NProto::DEVICE_ERASE_METHOD_CRYPTO_ERASE:
        return SecureErase(SPDK_NVME_FMT_NVM_SES_CRYPTO_ERASE);

    case NProto::DEVICE_ERASE_METHOD_NONE:
        return {};
    }
}

TFuture<NProto::TError> TSpdkDevice::ZeroFill()
{
    int index = Env->PickCore();

    return Env->GetTaskQueue(index)->Execute([=] {
        auto result = NewPromise<NProto::TError>();

        auto completion = std::make_unique<TIOCompletion>(Dev, result);
        int error = spdk_bdev_write_zeroes_blocks(
            Desc,
            GetChannel(),
            0,  // offset_blocks
            spdk_bdev_get_num_blocks(Dev),
            TIOCompletion::Callback,
            completion.get());

        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to zero device " << spdk_bdev_get_name(Dev);
        }

        completion.release();   // will be deleted in callback
        return result.GetFuture();
    });
}

TFuture<NProto::TError> TSpdkDevice::SecureErase(
    spdk_nvme_secure_erase_setting ses)
{
    int index = Env->PickCore();

    return Env->GetTaskQueue(index)->Execute([=] {
        auto name = TString(spdk_bdev_get_name(Dev));

        spdk_nvme_ctrlr* ctrlr = bdev_nvme_get_ctrlr(Dev);
        if (!ctrlr) {
            ythrow TServiceError(E_FAIL)
                << "unable to get controller for device " << name.Quote();
        }

        nvme_bdev_ns* ns = NvmeNamespaceFromBdev(Dev);
        if (!ns) {
            ythrow TServiceError(E_FAIL)
                << "unable to get namespace for device " << name.Quote();
        }

        const spdk_nvme_ctrlr_data* cdata = spdk_nvme_ctrlr_get_data(ctrlr);
        Y_ENSURE(cdata);

        if (ses == SPDK_NVME_FMT_NVM_SES_CRYPTO_ERASE &&
            !cdata->fna.crypto_erase_supported)
        {
            ythrow TServiceError(E_FAIL)
                << "cryptographic erase is not supported for device "
                << name.Quote();
        }

        uint32_t nsid = spdk_nvme_ns_get_id(ns->ns);
        Y_ENSURE(nsid);

        const spdk_nvme_ns_data* ndata = spdk_nvme_ns_get_data(ns->ns);
        Y_ENSURE(ndata);
        Y_ENSURE(ndata->lbaf[ndata->flbas.format].ms == 0, "unexpected metadata");

        spdk_nvme_format format = {
            .lbaf = ndata->flbas.format,
            .ses = ses,
        };
        auto result = NewPromise<NProto::TError>();
        auto completion = std::make_unique<TCtrlrFormatCompletion>(Dev, result);

        int res = nvme_ctrlr_cmd_format(
            ctrlr,
            nsid,
            &format,
            TCtrlrFormatCompletion::Callback,
            completion.get());

        if (res) {
            ythrow TServiceError(E_FAIL)
                << "unable to format device " << name.Quote() << ": "
                << res;
        }

        completion.release();   // will be deleted in callback
        return result.GetFuture();
    });
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TString> TSpdkEnv::RegisterNullDevice(
    const TString& name,
    ui64 blocksCount,
    ui32 blockSize)
{
    return Execute([=] {
        spdk_null_bdev_opts opts = {
            .name = name.c_str(),
            .uuid = nullptr,
            .num_blocks = blocksCount,
            .block_size = blockSize,
        };

        spdk_bdev* bdev = nullptr;
        int error = bdev_null_create(&bdev, &opts);
        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to create null device " << name.Quote();
        }

        return name;
    });
}

TFuture<TString> TSpdkEnv::RegisterMemoryDevice(
    const TString& name,
    ui64 blocksCount,
    ui32 blockSize)
{
    return Execute([=] {
        spdk_bdev* bdev = nullptr;
        int error = create_malloc_disk(
            &bdev,
            name.c_str(),
            nullptr,    // uuid
            blocksCount,
            blockSize);

        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to create memory device " << name.Quote();
        }

        return name;
    });
}

TFuture<TString> TSpdkEnv::RegisterFileDevice(
    const TString& name,
    const TString& path,
    ui32 blockSize)
{
    return Execute([=] {
        int error = create_aio_bdev(
            name.c_str(),
            path.c_str(),
            blockSize);

        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to create file device " << path.Quote();
        }

        return name;
    });
}

TFuture<TString> TSpdkEnv::RegisterSCSIDevice(
    const TString& name,
    const TString& targetUrl,
    const TString& initiatorIqn)
{
    return Execute([=] {
        auto result = NewPromise<TString>();

        auto completion = std::make_unique<TRegisterSCSICompletion>(targetUrl, result);
        int error = create_iscsi_disk(
            name.c_str(),
            targetUrl.c_str(),
            initiatorIqn.c_str(),
            TRegisterSCSICompletion::Callback,
            completion.get());

        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to probe for iSCSI device on " << targetUrl.Quote();
        }

        completion.release();   // will be deleted in callback
        return result.GetFuture();
    });
}

TFuture<TVector<TString>> TSpdkEnv::CreateNvmeController(
    const TString& baseName,
    const TString& transportId)
{
    spdk_nvme_transport_id trid = ParseTransportId(transportId);

    for (auto& controller: NvmeControllers) {
        if (spdk_nvme_transport_id_compare(&controller.TransportId, &trid) == 0) {
            return controller.Devices;
        }
    }

    auto result = NewPromise<TVector<TString>>();

    // TODO
    spdk_nvme_host_id hostid = {};

    auto completion = std::make_unique<TRegisterNVMeCompletion>(
        baseName,
        transportId,
        result);

    int error = bdev_nvme_create(
        &trid,
        &hostid,
        completion->BaseName.c_str(),
        completion->Names,
        completion->Count,
        nullptr,    // hostnqn
        0,          // prchk_flags
        TRegisterNVMeCompletion::Callback,
        completion.get(),
        nullptr);   // nvme_ctrlr_opts

    if (error) {
        ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
            << "unable to probe for NVMe devices on " << transportId.Quote()
            << ": " << strerror(error) << " (" << error << ")";
    }

    completion.release();   // will be deleted in the callback

    return NvmeControllers.emplace_back(trid, result.GetFuture()).Devices;
}

TFuture<TVector<TString>> TSpdkEnv::RegisterNVMeDevices(
    const TString& baseName,
    const TString& transportId)
{
    return Execute([=] {
        auto result = NewPromise<TVector<TString>>();
        auto devices = CreateNvmeController(baseName, transportId);

        devices.Subscribe([=] (const auto& future) mutable {
            auto names = future.GetValue();
            auto nsid = GetNSIDFromTransportId(transportId);

            if (0 < nsid && nsid <= names.size()) {
                result.SetValue(TVector<TString>{names[nsid - 1]});
            } else {
                result.SetValue(names);
            }
        });

        return result.GetFuture();
    });
}

TFuture<void> TSpdkEnv::UnregisterDevice(const TString& name)
{
    return Execute([=] {
        auto* bdev = spdk_bdev_get_by_name(name.c_str());
        if (!bdev) {
            ythrow TServiceError(E_ARGUMENT)
                << "unable to find device " << name.Quote();
        }

        auto result = NewPromise();

        auto completion = std::make_unique<TUnregisterCompletion>(bdev, result);
        spdk_bdev_unregister(bdev, TUnregisterCompletion::Callback, completion.get());

        completion.release();   // will be deleted in callback
        return result.GetFuture();
    });
}

TFuture<ISpdkDevicePtr> TSpdkEnv::OpenDevice(const TString& name, bool write)
{
    return Execute([=] {
        auto* bdev = spdk_bdev_get_by_name(name.c_str());
        if (!bdev) {
            ythrow TServiceError(E_ARGUMENT)
                << "unable to find device " << name.Quote();
        }

        return OpenDevice(bdev, write);
    });
}

ISpdkDevicePtr TSpdkEnv::OpenDevice(spdk_bdev* bdev, bool write)
{
    spdk_bdev_desc* desc = nullptr;
    const char* name = spdk_bdev_get_name(bdev);

    int error = spdk_bdev_open_ext(name, write, EventCallback, nullptr, &desc);
    if (error) {
        ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
            << "unable to open device " << name;
    }

    return std::make_shared<TSpdkDevice>(shared_from_this(), bdev, desc);
}

TFuture<TDeviceStats> TSpdkEnv::QueryDeviceStats(const TString& name)
{
    return Execute([=] {
        auto* bdev = spdk_bdev_get_by_name(name.c_str());
        if (!bdev) {
            ythrow TServiceError(E_ARGUMENT)
                << "unable to find device " << name.Quote();
        }

        TDeviceStats stats;
        memcpy(&stats.DeviceUUID, spdk_bdev_get_uuid(bdev), sizeof(TGUID));
        stats.BlockSize = spdk_bdev_get_block_size(bdev);
        stats.BlocksCount = spdk_bdev_get_num_blocks(bdev);

        return stats;
    });
}

TFuture<TString> TSpdkEnv::RegisterDeviceProxy(
    const TString& deviceName,
    const TString& proxyName)
{
    return Execute([=] {
        auto* bdev = spdk_bdev_get_by_name(deviceName.c_str());
        if (!bdev) {
            ythrow TServiceError(E_ARGUMENT)
                << "unable to find device " << deviceName.Quote();
        }

        int error = NSpdk::RegisterDeviceProxy(bdev, proxyName);
        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to register proxy device " << proxyName.Quote();
        }

        return proxyName;
    });
}

TFuture<TString> TSpdkEnv::RegisterDeviceWrapper(
    ISpdkDevicePtr device,
    const TString& wrapperName,
    ui64 blocksCount,
    ui32 blockSize)
{
    return Execute([=] {
        int error = NSpdk::RegisterDeviceWrapper(
            device,
            wrapperName,
            blocksCount,
            blockSize);

        if (error) {
            ythrow TServiceError(MAKE_SYSTEM_ERROR(error))
                << "unable to register wrapper device " << wrapperName.Quote();
        }

        return wrapperName;
    });
}

TFuture<void> TSpdkEnv::SetRateLimits(
    const TString& deviceName,
    TDeviceRateLimits rateLimits)
{
    return Execute([=] {
        auto* bdev = spdk_bdev_get_by_name(deviceName.c_str());
        if (!bdev) {
            ythrow TServiceError(E_ARGUMENT)
                << "unable to find device " << deviceName.Quote();
        }

        auto result = NewPromise();

        ui64 limits[] = {
            rateLimits.IopsLimit,
            rateLimits.BandwidthLimit,
            rateLimits.ReadBandwidthLimit,
            rateLimits.WriteBandwidthLimit
        };

        auto completion = std::make_unique<TQosCompletion>(bdev, result);
        spdk_bdev_set_qos_rate_limits(
            bdev, limits, TQosCompletion::Callback, completion.get());

        completion.release();   // will be deleted in callback
        return result.GetFuture();
    });
}

TFuture<TDeviceRateLimits> TSpdkEnv::GetRateLimits(const TString& deviceName)
{
    return Execute([=] {
        auto* bdev = spdk_bdev_get_by_name(deviceName.c_str());
        if (!bdev) {
            ythrow TServiceError(E_ARGUMENT)
                << "unable to find device " << deviceName.Quote();
        }

        uint64_t limits[SPDK_BDEV_QOS_NUM_RATE_LIMIT_TYPES] = {};
        spdk_bdev_get_qos_rate_limits(bdev, limits);

        return TDeviceRateLimits {
            .IopsLimit = limits[0],
            .BandwidthLimit = limits[1],
            .ReadBandwidthLimit = limits[2],
            .WriteBandwidthLimit = limits[3]
        };
    });
}

TFuture<TDeviceIoStats> TSpdkEnv::GetDeviceIoStats(const TString& deviceName)
{
    return Execute([=] {
        auto* bdev = spdk_bdev_get_by_name(deviceName.c_str());
        if (!bdev) {
            ythrow TServiceError(E_ARGUMENT)
                << "unable to find device " << deviceName.Quote();
        }

        auto result = NewPromise<TDeviceIoStats>();

        auto completion = std::make_unique<TIoStatsCompletion>(bdev, result);
        spdk_bdev_get_device_stat(
            bdev,
            &completion->NativeStats,
            TIoStatsCompletion::Callback,
            completion.get());

        completion.release();   // will be deleted in callback
        return result.GetFuture();
    });
}

TFuture<void> TSpdkEnv::EnableHistogram(const TString& deviceName, bool enable)
{
    return Execute([=] {
        auto* bdev = spdk_bdev_get_by_name(deviceName.c_str());
        if (!bdev) {
            ythrow TServiceError(E_ARGUMENT)
                << "unable to find device " << deviceName.Quote();
        }

        auto result = NewPromise();

        auto completion = std::make_unique<TEnableHistogramCompletion>(bdev, result);
        spdk_bdev_histogram_enable(
            bdev,
            TEnableHistogramCompletion::Callback,
            completion.get(),
            enable);

        completion.release();   // will be deleted in callback
        return result.GetFuture();
    });
}

TFuture<TVector<TBucketInfo>> TSpdkEnv::GetHistogramBuckets(
    const TString& deviceName)
{
    return Execute([=] {
        auto* bdev = spdk_bdev_get_by_name(deviceName.c_str());
        if (!bdev) {
            ythrow TServiceError(E_ARGUMENT)
                << "unable to find device " << deviceName.Quote();
        }

        auto result = NewPromise<TVector<TBucketInfo>>();

        auto completion = std::make_unique<TGetHistogramCompletion>(
            bdev, result);

        spdk_bdev_histogram_get(
            bdev,
            completion->Histogram.get(),
            TGetHistogramCompletion::Callback,
            completion.get());

        completion.release();   // will be deleted in callback
        return result.GetFuture();
    });
}

}   // namespace NCloud::NBlockStore::NSpdk
