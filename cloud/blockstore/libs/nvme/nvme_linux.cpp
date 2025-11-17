#include "nvme.h"

#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>

#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/string/strip.h>
#include <util/system/file.h>

#include <linux/fs.h>
#include <linux/hdreg.h>
#include <linux/nvme_ioctl.h>
#include <sys/ioctl.h>
#include <sys/stat.h>

#include <cerrno>
#include <charconv>
#include <filesystem>
#include <regex>

namespace NCloud::NBlockStore::NNvme {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString NVMeDriverName = "nvme";
const TString VFIODriverName = "vfio-pci";

////////////////////////////////////////////////////////////////////////////////

TString ReadFile(const std::filesystem::path& path)
{
    return TFileInput(path).ReadAll();
}

ui16 ReadFileHex(const std::filesystem::path& path)
{
    const auto s = Strip(ReadFile(path));

    TStringBuf buf{s};
    buf.SkipPrefix("0x");

    ui16 value = 0;
    const auto r =
        std::from_chars(buf.data(), buf.data() + buf.size(), value, 16);
    if (r.ec == std::errc{}) {
        return value;
    }

    ythrow TServiceError(E_ARGUMENT)
        << "can't parse a hex value from the string " << s.Quote();
}

////////////////////////////////////////////////////////////////////////////////

nvme_ctrlr_data NVMeIdentifyCtrl(TFile& device)
{
    nvme_ctrlr_data ctrl = {};

    nvme_admin_cmd cmd = {
        .opcode = NVME_OPC_IDENTIFY,
        .addr = static_cast<ui64>(reinterpret_cast<uintptr_t>(&ctrl)),
        .data_len = sizeof(ctrl),
        .cdw10 = NVME_IDENTIFY_CTRLR
    };

    int err = ioctl(device.GetHandle(), NVME_IOCTL_ADMIN_CMD, &cmd);

    if (err) {
        int err = errno;
        ythrow TServiceError(MAKE_SYSTEM_ERROR(err))
            << "NVMeIdentifyCtrl failed: " << strerror(err);
    }

    return ctrl;
}

nvme_ns_data NVMeIdentifyNs(TFile& device, ui32 nsId)
{
    nvme_ns_data ns = {};

    nvme_admin_cmd cmd = {
        .opcode = NVME_OPC_IDENTIFY,
        .nsid = nsId,
        .addr = static_cast<ui64>(reinterpret_cast<uintptr_t>(&ns)),
        .data_len = sizeof(ns),
        .cdw10 = NVME_IDENTIFY_NS
    };

    int err = ioctl(device.GetHandle(), NVME_IOCTL_ADMIN_CMD, &cmd);

    if (err) {
        int err = errno;
        ythrow TServiceError(MAKE_SYSTEM_ERROR(err))
            << "NVMeIdentifyNs failed: " << strerror(err);
    }

    return ns;
}

void NVMeFormatImpl(
    TFile& device,
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

    int err = ioctl(device.GetHandle(), NVME_IOCTL_ADMIN_CMD, &cmd);

    if (err) {
        int err = errno;
        ythrow TServiceError(MAKE_SYSTEM_ERROR(err))
            << "NVMeFormatImpl failed: " << strerror(err);
    }
}

bool IsBlockOrCharDevice(TFile& device)
{
    struct stat deviceStat = {};

    if (fstat(device.GetHandle(), &deviceStat) < 0) {
        int err = errno;
        ythrow TServiceError(MAKE_SYSTEM_ERROR(err))
            << "fstat error: " << strerror(err);
    }

    return S_ISCHR(deviceStat.st_mode) || S_ISBLK(deviceStat.st_mode);
}

hd_driveid HDIdentity(TFile& device)
{
    hd_driveid hd {};
    int err = ioctl(device.GetHandle(), HDIO_GET_IDENTITY, &hd);

    if (err) {
        int err = errno;
        ythrow TServiceError(MAKE_SYSTEM_ERROR(err))
            << "HDIdentity failed: " << strerror(err);
    }

    return hd;
}

TResultOrError<bool> IsRotational(TFile& device)
{
    unsigned short val = 0;
    int err = ioctl(device.GetHandle(), BLKROTATIONAL, &val);
    if (err) {
        int err = errno;
        return MakeError(MAKE_SYSTEM_ERROR(err), strerror(err));
    }

    return val != 0;
}

template <typename T, size_t N>
TString ToString(T (&arr)[N])
{
    auto* it = std::bit_cast<const char*>(&arr[0]);
    auto end = std::find(it, it + sizeof(arr), '\0');

    return TString(it, end);
}

////////////////////////////////////////////////////////////////////////////////

class TNvmeManager final
    : public INvmeManager
{
private:
    ITaskQueuePtr Executor;
    TDuration Timeout;  // admin command timeout

    void FormatImpl(
        const TString& path,
        nvme_secure_erase_setting ses)
    {
        TFile device(path, OpenExisting | RdOnly);

        Y_ENSURE(IsBlockOrCharDevice(device), "expected block or character device");

        nvme_ctrlr_data ctrl = NVMeIdentifyCtrl(device);

        Y_ENSURE(ctrl.fna.format_all_ns == 0, "can't format single namespace");
        Y_ENSURE(ctrl.fna.erase_all_ns == 0, "can't erase single namespace");
        Y_ENSURE(
            ses != NVME_FMT_NVM_SES_CRYPTO_ERASE || ctrl.fna.crypto_erase_supported == 1,
            "cryptographic erase is not supported");

        const int nsId = ioctl(device.GetHandle(), NVME_IOCTL_ID);

        Y_ENSURE(nsId > 0, "unexpected namespace id");

        nvme_ns_data ns = NVMeIdentifyNs(device, static_cast<ui32>(nsId));

        Y_ENSURE(ns.lbaf[ns.flbas.format].ms == 0, "unexpected metadata");

        nvme_format format {
            .lbaf = ns.flbas.format,
            .ses = ses
        };

        NVMeFormatImpl(device, nsId, format, Timeout);
    }

    void DeallocateImpl(const TString& path, ui64 offsetBytes, ui64 sizeBytes)
    {
        TFile device(path, OpenExisting | RdWr);
        Y_ENSURE(IsBlockOrCharDevice(device), "expected block or character device");

        ui64 devSizeBytes = 0;
        int err = ioctl(device.GetHandle(), BLKGETSIZE64, &devSizeBytes);
        if (err) {
            err = errno;
            ythrow TServiceError(MAKE_SYSTEM_ERROR(err))
                << "NVMeDeallocateImpl failed to read device size: "
                << strerror(err);
        }

        Y_ENSURE(offsetBytes + sizeBytes <= devSizeBytes,
            "invalid deallocate range: "
            "offsetBytes=" << offsetBytes <<
            ", sizeBytes=" << sizeBytes <<
            ", devSizeBytes=" << devSizeBytes);

        ui64 range[2] = { offsetBytes, sizeBytes };
        err = ioctl(device.GetHandle(), BLKDISCARD, range);
        if (err) {
            err = errno;
            ythrow TServiceError(MAKE_SYSTEM_ERROR(err))
                << "NVMeDeallocateImpl failed to deallocate: "
                << strerror(err);
        }
    }

public:
    TNvmeManager(ITaskQueuePtr executor, TDuration timeout)
        : Executor(std::move(executor))
        , Timeout(timeout)
    {}

    TFuture<NProto::TError> Format(
        const TString& path,
        nvme_secure_erase_setting ses) override
    {
        return Executor->Execute([=, this] {
            try {
                FormatImpl(path, ses);
                return NProto::TError();
            } catch (...) {
                return MakeError(E_FAIL, CurrentExceptionMessage());
            }
        });
    }

    TFuture<NProto::TError> Deallocate(
        const TString& path,
        ui64 offsetBytes,
        ui64 sizeBytes) override
    {
        return Executor->Execute([this, path, offsetBytes, sizeBytes] {
            try {
                DeallocateImpl(path, offsetBytes, sizeBytes);
                return NProto::TError();
            } catch (const TServiceError &e) {
                return MakeError(e.GetCode(), TString(e.GetMessage()));
            } catch (...) {
                return MakeError(E_FAIL, CurrentExceptionMessage());
            }
        });
    }

    TResultOrError<TString> GetSerialNumber(const TString& path) override
    {
        return SafeExecute<TResultOrError<TString>>(
            [&]
            {
                TFile device(path, OpenExisting | RdOnly);

                auto [isRot, error] = IsRotational(device);

                if (!HasError(error) && isRot) {
                    auto hd = HDIdentity(device);
                    return ToString(hd.serial_no);
                }

                auto ctrl = NVMeIdentifyCtrl(device);

                return ToString(ctrl.sn);
            });
    }

    TResultOrError<bool> IsSsd(const TString& path) override
    {
        return SafeExecute<TResultOrError<bool>>([&] {
            TFile device(path, OpenExisting | RdOnly);

            auto [isRot, error] = IsRotational(device);
            if (HasError(error)) {
                ythrow TServiceError(error.GetCode())
                    << "NVMeIsSsd failed: " << error.GetMessage();
            }

            return TResultOrError { !isRot };
        });
    }

    TResultOrError<TVector<TControllerData>> ListControllers() final
    {
        namespace NFs = std::filesystem;

        return SafeExecute<TResultOrError<TVector<TControllerData>>>(
            [&]
            {
                TVector<TControllerData> result;

                for (const auto& e:
                     NFs::directory_iterator{"/sys/class/nvme"}) {
                    const NFs::path devicePath = "/dev" / e.path().filename();

                    TFile device(devicePath, OpenExisting | RdOnly);

                    nvme_ctrlr_data data = NVMeIdentifyCtrl(device);
                    Y_ABORT_IF(
                        data.tnvmcap[1] > 0,
                        "unexpected NVMe device capacity");

                    result.push_back({
                        .DevicePath = devicePath.string(),
                        .SerialNumber = ToString(data.sn),
                        .ModelNumber = ToString(data.mn),
                        .Capacity = data.tnvmcap[0],
                    });
                }
                return result;
            });
    }

    TResultOrError<TPCIDeviceInfo> GetPCIDeviceInfo(const TString& devicePath) final
    {
        namespace NFs = std::filesystem;

        return SafeExecute<TResultOrError<TPCIDeviceInfo>>(
            [&]
            {
                const auto filename = NFs::path{devicePath.c_str()}.filename();
                TString address =
                    Strip(ReadFile("/sys/class/nvme" / filename / "address"));
                Y_ENSURE(!address.empty());

                const auto base = NFs::path{"/sys/bus/pci/devices"} /
                                  std::string_view{address};

                std::optional<ui32> iommuGroup;
                if (NFs::exists(base / "iommu_group")) {
                    iommuGroup = std::stoi(
                        NFs::read_symlink(base / "iommu_group")
                            .filename()
                            .string());
                }

                return TPCIDeviceInfo{
                    .VendorId = ReadFileHex(base / "vendor"),
                    .DeviceId = ReadFileHex(base / "device"),
                    .Address = std::move(address),
                    .IOMMUGroup = iommuGroup,
                };
            });
    }

    TResultOrError<TString> GetDriverName(const TPCIDeviceInfo& pci) final
    {
        namespace NFs = std::filesystem;

        return SafeExecute<TResultOrError<TString>>(
            [&]
            {
                const auto address = NFs::path{"/sys/bus/pci/devices"} /
                                  std::string_view{pci.Address};

                if (!NFs::exists(address)) {
                    return TString{};
                }

                if (pci.VendorId != ReadFileHex(address / "vendor") ||
                    pci.DeviceId != ReadFileHex(address / "device"))
                {
                    return TString{};
                }

                return TString{
                    NFs::read_symlink(address / "driver").filename().string()};
            });
    }

    NProto::TError BindToVFIO(const TPCIDeviceInfo& pci) final
    {
        if (!pci) {
            return MakeError(E_ARGUMENT, "empty PCI address");
        }

        // Allow vfio driver to handle devices with vendorId:deviceId
        auto error = SafeExecute<NProto::TError>(
            [&]
            {
                TFileOutput out("/sys/bus/pci/drivers/vfio-pci/new_id");
                out << Hex(pci.VendorId, {}) << " " << Hex(pci.DeviceId, {});

                return NProto::TError();
            });

        if (HasError(error)) {
            return error;
        }

        return RebindDriver(pci, NVMeDriverName, VFIODriverName);
    }

    NProto::TError BindToNVME(const TPCIDeviceInfo& pci) final
    {
        if (!pci) {
            return MakeError(E_ARGUMENT, "empty PCI address");
        }

        return RebindDriver(pci, VFIODriverName, NVMeDriverName);
    }

    auto RebindDriver(
        const TPCIDeviceInfo& pci,
        const TString& from,
        const TString& to) -> NProto::TError
    {
        auto [currentDriver, error] = GetDriverName(pci);
        if (HasError(error)) {
            return error;
        }

        if (to == currentDriver) {
            return MakeError(S_ALREADY);
        }

        if (from != currentDriver) {
            return MakeError(
                E_PRECONDITION_FAILED,
                TStringBuilder() << "unexpected driver: " << currentDriver);
        }

        return SafeExecute<NProto::TError>(
            [&]
            {
                const TString unbind =
                    "/sys/bus/pci/drivers/" + from + "/unbind";
                const TString bind = "/sys/bus/pci/drivers/" + to + "/bind";

                TFileOutput(unbind).Write(pci.Address);
                TFileOutput(bind).Write(pci.Address);

                return NProto::TError{};
            });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

INvmeManagerPtr CreateNvmeManager(TDuration timeout)
{
    return std::make_shared<TNvmeManager>(
        CreateLongRunningTaskExecutor("SecureErase"),
        timeout);
}

}   // namespace NCloud::NBlockStore::NNvme
