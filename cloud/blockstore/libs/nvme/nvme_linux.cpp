#include "nvme_linux.h"

#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>

#include <contrib/libs/systemd/src/systemd/sd-device.h>

#include <util/folder/path.h>
#include <util/generic/yexception.h>
#include <util/string/printf.h>
#include <util/system/file.h>

#include <linux/fs.h>
#include <linux/hdreg.h>
#include <linux/nvme_ioctl.h>
#include <sys/ioctl.h>
#include <sys/stat.h>

#include <cerrno>

namespace NCloud::NBlockStore::NNvme {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

void Test(const TString& path) {
    Cerr << "path = " << path << Endl;

    const TFsPath fspath{path};
    if (!fspath.Exists()) {
        Cerr << "COULD NOT READ THE PATH" << Endl;
        Cerr << "--------------------------------------------" << Endl;
        return;
    }

    const TFsPath devPath = fspath.ReadLink();
    // devPath.RelativePath(fspath);

    Cerr << "devPath = " << devPath << Endl;
    // if (!devPath.Exists()) {
    //     Cerr << "COULD NOT READ THE LINK" << Endl;
    //     Cerr << "--------------------------------------------" << Endl;
    //     return;
    // }

    Cerr << "Inserting name = " << devPath.Basename() << Endl;

    sd_device* device = nullptr;
    int res = sd_device_new_from_subsystem_sysname(&device, "block", devPath.Basename().c_str());
    Cerr << "sd_device_new_from_subsystem_sysname res = " << res << Endl;
    // Cerr << "device = " << device << Endl;

    if (!device) {
        int err = errno;
        Cerr << "NO DEVICE! errno = " << err << " (" << ::strerror(err) << ")" << Endl;
        Cerr << "--------------------------------------------" << Endl;
        return;
    }

    const char* value = nullptr;
    res = sd_device_get_property_value(device, "ID_SERIAL_SHORT", &value);
    int err = errno;
    Cerr << "sd_device_get_property_value res = " << res << "; errno = " << err << " (" << ::strerror(err) << ")" << Endl;
    if (value) {
        Cerr << "sd_device_get_property_value value = " << TString(value)
             << Endl;
    }
    Cerr << "--------------------------------------------" << Endl;
}

nvme_ctrlr_data NVMeIdentifyCtrl(TFileHandle& device)
{
    nvme_ctrlr_data ctrl = {};

    nvme_admin_cmd cmd = {
        .opcode = NVME_OPC_IDENTIFY,
        .addr = static_cast<ui64>(reinterpret_cast<uintptr_t>(&ctrl)),
        .data_len = sizeof(ctrl),
        .cdw10 = NVME_IDENTIFY_CTRLR
    };

    int err = ioctl(device, NVME_IOCTL_ADMIN_CMD, &cmd);

    if (err) {
        int err = errno;
        ythrow TServiceError(MAKE_SYSTEM_ERROR(err))
            << "NVMeIdentifyCtrl failed: " << strerror(err);
    }

    return ctrl;
}

nvme_ns_data NVMeIdentifyNs(TFileHandle& device, ui32 nsId)
{
    nvme_ns_data ns = {};

    nvme_admin_cmd cmd = {
        .opcode = NVME_OPC_IDENTIFY,
        .nsid = nsId,
        .addr = static_cast<ui64>(reinterpret_cast<uintptr_t>(&ns)),
        .data_len = sizeof(ns),
        .cdw10 = NVME_IDENTIFY_NS
    };

    int err = ioctl(device, NVME_IOCTL_ADMIN_CMD, &cmd);

    if (err) {
        int err = errno;
        ythrow TServiceError(MAKE_SYSTEM_ERROR(err))
            << "NVMeIdentifyNs failed: " << strerror(err);
    }

    return ns;
}

void NVMeFormatImpl(
    TFileHandle& device,
    ui32 nsId,
    nvme_format format,
    TDuration timeout)
{
    nvme_admin_cmd cmd = {
        .opcode = NVME_OPC_FORMAT_NVM,
        .nsid = nsId,
        .timeout_ms = static_cast<ui32>(timeout.MilliSeconds())
    };

    memcpy(&cmd.cdw10, &format, sizeof(ui32));

    int err = ioctl(device, NVME_IOCTL_ADMIN_CMD, &cmd);

    if (err) {
        int err = errno;
        ythrow TServiceError(MAKE_SYSTEM_ERROR(err))
            << "NVMeFormatImpl failed: " << strerror(err);
    }
}

bool IsBlockOrCharDevice(TFileHandle& device)
{
    struct stat deviceStat = {};

    if (fstat(device, &deviceStat) < 0) {
        int err = errno;
        ythrow TServiceError(MAKE_SYSTEM_ERROR(err))
            << "fstat error: " << strerror(err);
    }

    return S_ISCHR(deviceStat.st_mode) || S_ISBLK(deviceStat.st_mode);
}

hd_driveid HDIdentity(TFileHandle& device)
{
    hd_driveid hd {};
    int err = ioctl(device, HDIO_GET_IDENTITY, &hd);

    if (err) {
        int err = errno;
        ythrow TServiceError(MAKE_SYSTEM_ERROR(err))
            << "HDIdentity failed: " << strerror(err);
    }

    return hd;
}

TResultOrError<bool> IsRotational(TFileHandle& device)
{
    unsigned short val = 0;
    int err = ioctl(device, BLKROTATIONAL, &val);
    if (err) {
        int err = errno;
        return MakeError(MAKE_SYSTEM_ERROR(err), strerror(err));
    }

    return val != 0;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TNvmeManager::TNvmeManager(ITaskQueuePtr executor, TDuration timeout)
    : Executor(executor)
    , Timeout(timeout)
{}

TNvmeManager::~TNvmeManager() = default;

void TNvmeManager::FormatImpl(
    const TString& path,
    nvme_secure_erase_setting ses)
{
    TFileHandle device(path, OpenExisting | RdOnly);

    Y_ENSURE(IsBlockOrCharDevice(device), "expected block or character device");

    nvme_ctrlr_data ctrl = NVMeIdentifyCtrl(device);

    Y_ENSURE(ctrl.fna.format_all_ns == 0, "can't format single namespace");
    Y_ENSURE(ctrl.fna.erase_all_ns == 0, "can't erase single namespace");
    Y_ENSURE(
        ses != NVME_FMT_NVM_SES_CRYPTO_ERASE ||
            ctrl.fna.crypto_erase_supported == 1,
        "cryptographic erase is not supported");

    const int nsId = ioctl(device, NVME_IOCTL_ID);

    Y_ENSURE(nsId > 0, "unexpected namespace id");

    nvme_ns_data ns = NVMeIdentifyNs(device, static_cast<ui32>(nsId));

    Y_ENSURE(ns.lbaf[ns.flbas.format].ms == 0, "unexpected metadata");

    nvme_format format{.lbaf = ns.flbas.format, .ses = ses};

    NVMeFormatImpl(device, nsId, format, Timeout);
}

void TNvmeManager::DeallocateImpl(
    const TString& path,
    ui64 offsetBytes,
    ui64 sizeBytes)
{
    TFileHandle device(path, OpenExisting | RdWr);
    Y_ENSURE(IsBlockOrCharDevice(device), "expected block or character device");

    ui64 devSizeBytes = 0;
    int err = ioctl(device, BLKGETSIZE64, &devSizeBytes);
    if (err) {
        err = errno;
        ythrow TServiceError(MAKE_SYSTEM_ERROR(err))
            << "NVMeDeallocateImpl failed to read device size: "
            << strerror(err);
    }

    Y_ENSURE(
        offsetBytes + sizeBytes <= devSizeBytes,
        "invalid deallocate range: "
        "offsetBytes="
            << offsetBytes << ", sizeBytes=" << sizeBytes
            << ", devSizeBytes=" << devSizeBytes);

    ui64 range[2] = {offsetBytes, sizeBytes};
    err = ioctl(device, BLKDISCARD, range);
    if (err) {
        err = errno;
        ythrow TServiceError(MAKE_SYSTEM_ERROR(err))
            << "NVMeDeallocateImpl failed to deallocate: " << strerror(err);
    }
}

TFuture<NProto::TError> TNvmeManager::Format(
    const TString& path,
    nvme_secure_erase_setting ses)
{
    return Executor->Execute(
        [=, this]
        {
            try {
                FormatImpl(path, ses);
                return NProto::TError();
            } catch (...) {
                return MakeError(E_FAIL, CurrentExceptionMessage());
            }
        });
}

TFuture<NProto::TError>
TNvmeManager::Deallocate(const TString& path, ui64 offsetBytes, ui64 sizeBytes)
{
    return Executor->Execute(
        [=, this]
        {
            try {
                DeallocateImpl(path, offsetBytes, sizeBytes);
                return NProto::TError();
            } catch (const TServiceError& e) {
                return MakeError(e.GetCode(), TString(e.GetMessage()));
            } catch (...) {
                return MakeError(E_FAIL, CurrentExceptionMessage());
            }
        });
}

TResultOrError<TString> TNvmeManager::GetSerialNumber(const TString& path)
{
    return SafeExecute<TResultOrError<TString>>(
        [&]
        {
            Test(path);

            TFileHandle device(path, OpenExisting | RdOnly);

            auto str = [](auto& arr)
            {
                auto* sn = std::bit_cast<const char*>(&arr[0]);
                auto end = std::find(sn, sn + sizeof(arr), '\0');

                return TString(sn, end);
            };

            auto [isRot, error] = IsRotational(device);

            if (!HasError(error) && isRot) {
                auto hd = HDIdentity(device);
                return str(hd.serial_no);
            }

            auto ctrl = NVMeIdentifyCtrl(device);

            return str(ctrl.sn);
        });
}

TResultOrError<bool> TNvmeManager::IsSsd(const TString& path)
{
    return SafeExecute<TResultOrError<bool>>(
        [&]
        {
            TFileHandle device(path, OpenExisting | RdOnly);

            auto [isRot, error] = IsRotational(device);
            if (HasError(error)) {
                ythrow TServiceError(error.GetCode())
                    << "NVMeIsSsd failed: " << error.GetMessage();
            }

            return TResultOrError{!isRot};
        });
}

}   // namespace NCloud::NBlockStore::NNvme
