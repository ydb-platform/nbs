#include "nvme.h"

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/system/file.h>

#include <linux/fs.h>
#include <linux/hdreg.h>
#include <linux/nvme_ioctl.h>
#include <sys/ioctl.h>
#include <sys/stat.h>

#include <cerrno>
#include <span>

namespace NCloud::NBlockStore::NNvme {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

void InvokeAdminCmd(TFileHandle& file, nvme_admin_cmd& cmd, const char* source)
{
    if (!ioctl(file, NVME_IOCTL_ADMIN_CMD, &cmd)) {
        return;
    }

    int err = errno;
    STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
        << "Failed to " << source
        << " (opcode: " << static_cast<int>(cmd.opcode) << " nsid: " << cmd.nsid
        << " cdw10: " << cmd.cdw10 << "): " << strerror(err);
}

auto ListAllocatedNamespaces(TFileHandle& file) -> TVector<ui32>
{
    ui32 namespaceIDs[1024]{};

    nvme_admin_cmd cmd = {
        .opcode = NVME_OPC_IDENTIFY,
        .addr = std::bit_cast<ui64>(&namespaceIDs[0]),
        .data_len = sizeof(namespaceIDs),
        .cdw10 = NVME_IDENTIFY_ALLOCATED_NS_LIST,
    };

    InvokeAdminCmd(file, cmd, "identify allocated ns list");

    return {std::begin(namespaceIDs), std::ranges::find(namespaceIDs, 0U)};
}

ui32 CreateNamespace(TFileHandle& file, ui64 totalBlocks, ui8 lbaFormatIndex)
{
    nvme_ns_data data{
        .nsze = totalBlocks,
        .ncap = totalBlocks,
        .flbas =
            {
                .format = lbaFormatIndex,
            },
    };

    nvme_admin_cmd cmd = {
        .opcode = NVME_OPC_NS_MANAGEMENT,
        .addr = std::bit_cast<ui64>(&data),
        .data_len = sizeof(data),
        .cdw10 = 0,   // create
    };

    InvokeAdminCmd(file, cmd, "create ns");

    return cmd.result;
}

void DeleteNamespace(TFileHandle& file, ui32 nsid)
{
    nvme_admin_cmd cmd = {
        .opcode = NVME_OPC_NS_MANAGEMENT,
        .nsid = nsid,
        .addr = 0,
        .data_len = 0,
        .cdw10 = 1,   // delete
    };
    InvokeAdminCmd(file, cmd, "delete ns");
}

void AttachNamespace(TFileHandle& file, ui32 nsid, ui16 ctrlId)
{
    nvme_ctrlr_list ctrlList{.num = 1, .identifiers = {ctrlId}};

    nvme_admin_cmd cmd = {
        .opcode = NVME_OPC_NS_ATTACHMENT,
        .nsid = nsid,
        .addr = std::bit_cast<ui64>(&ctrlList),
        .data_len = sizeof(ctrlList),
        .cdw10 = 0,   // attach
    };
    InvokeAdminCmd(file, cmd, "attach ns");
}

void DetachNamespaceFromAll(TFileHandle& file, ui32 nsid)
{
    nvme_ctrlr_list data{};

    nvme_admin_cmd cmd{
        .opcode = NVME_OPC_IDENTIFY,
        .nsid = nsid,
        .addr = std::bit_cast<ui64>(&data),
        .data_len = sizeof(data),
        .cdw10 = NVME_IDENTIFY_NS_ATTACHED_CTRLR_LIST,
    };

    InvokeAdminCmd(file, cmd, "identify ns attached ctrlr list");

    if (data.num == 0) {
        return;   // not attached to anything
    }

    // detach all: send the same buffer as-is
    cmd = {
        .opcode = NVME_OPC_NS_ATTACHMENT,
        .nsid = nsid,
        .addr = std::bit_cast<ui64>(&data),
        .data_len = sizeof(data),
        .cdw10 = 1,   // detach
    };
    InvokeAdminCmd(file, cmd, "detach ns from all ctrls");
}

nvme_ns_data IdentifyAllocatedNs(TFileHandle& device, ui32 nsid)
{
    nvme_ns_data ns{};
    nvme_admin_cmd cmd = {
        .opcode = NVME_OPC_IDENTIFY,
        .nsid = nsid,
        .addr = std::bit_cast<ui64>(&ns),
        .data_len = sizeof(ns),
        .cdw10 = NVME_IDENTIFY_NS_ALLOCATED,
    };

    InvokeAdminCmd(device, cmd, "identify allocated ns");

    return ns;
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
        STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
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
        STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
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
        STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
            << "NVMeFormatImpl failed: " << strerror(err);
    }
}

bool IsBlockOrCharDevice(TFileHandle& device)
{
    struct stat deviceStat = {};

    if (fstat(device, &deviceStat) < 0) {
        int err = errno;
        STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
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
        STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
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

ui32 GetSanitizeAction(TFileHandle& device)
{
    nvme_ctrlr_data ctrl = NVMeIdentifyCtrl(device);
    if (ctrl.sanicap.bits.crypto_erase) {
        return NVME_SANITIZE_ACT_CRYPTO_ERASE;
    }

    if (ctrl.sanicap.bits.block_erase) {
        return NVME_SANITIZE_ACT_BLOCK_ERASE;
    }

    STORAGE_THROW_SERVICE_ERROR(E_ARGUMENT)
        << "Device doesn't support Crypto Erase or Block Erase sanitize "
           "actions";
}

////////////////////////////////////////////////////////////////////////////////

class TNvmeManager final
    : public INvmeManager
{
private:
    const ILoggingServicePtr Logging;
    TLog Log;

    ITaskQueuePtr Executor;
    TDuration Timeout;  // admin command timeout

    void FormatImpl(
        const TString& path,
        nvme_secure_erase_setting ses)
    {
        TFileHandle device(path, OpenExisting | RdOnly);

        Y_ENSURE(IsBlockOrCharDevice(device), "expected block or character device");

        nvme_ctrlr_data ctrl = NVMeIdentifyCtrl(device);

        Y_ENSURE(ctrl.fna.format_all_ns == 0, "can't format single namespace");
        Y_ENSURE(ctrl.fna.erase_all_ns == 0, "can't erase single namespace");
        Y_ENSURE(
            ses != NVME_FMT_NVM_SES_CRYPTO_ERASE || ctrl.fna.crypto_erase_supported == 1,
            "cryptographic erase is not supported");

        const int nsId = ioctl(device, NVME_IOCTL_ID);

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
        TFileHandle device(path, OpenExisting | RdWr);
        Y_ENSURE(IsBlockOrCharDevice(device), "expected block or character device");

        ui64 devSizeBytes = 0;
        int err = ioctl(device, BLKGETSIZE64, &devSizeBytes);
        if (err) {
            err = errno;
            STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
                << "NVMeDeallocateImpl failed to read device size: "
                << strerror(err);
        }

        Y_ENSURE(offsetBytes + sizeBytes <= devSizeBytes,
            "invalid deallocate range: "
            "offsetBytes=" << offsetBytes <<
            ", sizeBytes=" << sizeBytes <<
            ", devSizeBytes=" << devSizeBytes);

        ui64 range[2] = { offsetBytes, sizeBytes };
        err = ioctl(device, BLKDISCARD, range);
        if (err) {
            err = errno;
            STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
                << "NVMeDeallocateImpl failed to deallocate: "
                << strerror(err);
        }
    }

public:
    TNvmeManager(ILoggingServicePtr logging, ITaskQueuePtr executor, TDuration timeout)
        : Logging(std::move(logging))
        , Executor(executor)
        , Timeout(timeout)
    {}

    void Start() final
    {
        Log = Logging->CreateLog("BLOCKSTORE_NVME");
    }

    void Stop() final
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
        return Executor->Execute([=, this] {
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
        return SafeExecute<TResultOrError<TString>>([&] {
            TFileHandle device(path, OpenExisting | RdOnly);

            auto str = [] (auto& arr) {
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

    TResultOrError<bool> IsSsd(const TString& path) override
    {
        return SafeExecute<TResultOrError<bool>>([&] {
            TFileHandle device(path, OpenExisting | RdOnly);

            auto [isRot, error] = IsRotational(device);
            if (HasError(error)) {
                STORAGE_THROW_SERVICE_ERROR(error.GetCode())
                    << "NVMeIsSsd failed: " << error.GetMessage();
            }

            return TResultOrError { !isRot };
        });
    }

    NProto::TError Sanitize(const TString& ctrlPath) override
    {
        return SafeExecute<NProto::TError>(
            [&]
            {
                TFileHandle device(ctrlPath, OpenExisting | RdWr);
                if (!device.IsOpen()) {
                    int err = errno;
                    STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
                        << "Failed to open file " << ctrlPath.Quote();
                }

                nvme_admin_cmd cmd{
                    .opcode = NVME_OPC_SANITIZE,
                    .cdw10 = GetSanitizeAction(device),
                };

                if (ioctl(device, NVME_IOCTL_ADMIN_CMD, &cmd)) {
                    int err = errno;
                    STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
                        << "Sanitize failed: " << strerror(err);
                }

                return MakeError(S_OK);
            });
    }

    TResultOrError<TSanitizeStatus> GetSanitizeStatus(
        const TString& ctrlPath) override
    {
        return SafeExecute<TResultOrError<TSanitizeStatus>>(
            [&]() -> TResultOrError<TSanitizeStatus>
            {
                TFileHandle device(ctrlPath, OpenExisting | RdWr);
                if (!device.IsOpen()) {
                    int err = errno;
                    STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
                        << "Failed to open file " << ctrlPath.Quote();
                }

                char buffer[4]{};

                const ui32 numd = (sizeof(buffer) / 4) - 1;

                nvme_admin_cmd cmd{
                    .opcode = NVME_OPC_GET_LOG_PAGE,
                    .addr = std::bit_cast<ui64>(&buffer[0]),
                    .data_len = sizeof(buffer),
                    .cdw10 = 0x81   // Log Page ID: Sanitize Status
                             | (numd << 16),
                };

                if (ioctl(device, NVME_IOCTL_ADMIN_CMD, &cmd)) {
                    int err = errno;
                    STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
                        << "Failed to get Sanitize status: " << strerror(err);
                }

                ui16 sprog = 0;
                ui16 sstat = 0;

                std::memcpy(&sprog, &buffer[0], 2);
                std::memcpy(&sstat, &buffer[2], 2);

                NProto::TError status;
                switch (sstat & NVME_SANITIZE_SSTAT_STATUS_MASK) {
                    case NVME_SANITIZE_SSTAT_COMPLETED:
                        status = MakeError(S_OK);
                        break;
                    case NVME_SANITIZE_SSTAT_IN_PROGRESS:
                        status = MakeError(
                            E_TRY_AGAIN,
                            "Sanitize operation in progress");
                        break;
                    case NVME_SANITIZE_SSTAT_FAILED:
                        status = MakeError(E_FAIL, "Sanitize operation failed");
                        break;
                    default:
                        status = MakeError(
                            E_FAIL,
                            TStringBuilder() << "Unexpected status: " << sstat);
                        break;
                }

                return TSanitizeStatus{
                    .Status = status,
                    .Progress = (sprog * 100.0) / 65535.0,
                };
            });
    }

    NProto::TError ResetToSingleNamespace(const TString& ctrlPath) final
    {
        return SafeExecute<NProto::TError>(
            [&]
            {
                TFileHandle device(ctrlPath, OpenExisting | RdWr);
                if (!device.IsOpen()) {
                    int err = errno;
                    STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
                        << "Failed to open file " << ctrlPath.Quote();
                }

                nvme_ctrlr_data ctrl = NVMeIdentifyCtrl(device);

                STORAGE_DEBUG(
                    "Current NVMe capacity: unallocated="
                    << ctrl.unvmcap[0] << " bytes total=" << ctrl.tnvmcap[0]
                    << " bytes");

                if (!ctrl.oacs.ns_manage) {
                    STORAGE_THROW_SERVICE_ERROR(E_ARGUMENT)
                        << "NVMe doesn't support namespace management";
                }

                if (!ctrl.tnvmcap[0]) {
                    STORAGE_THROW_SERVICE_ERROR(E_INVALID_STATE)
                        << "NVMe total capacity (tnvmcap) is empty";
                }

                // detach & delete namespaces

                while (auto nsIds = ListAllocatedNamespaces(device)) {
                    for (ui32 nsid: nsIds) {
                        STORAGE_DEBUG("Detach ns: " << nsid << "...");
                        DetachNamespaceFromAll(device, nsid);

                        STORAGE_DEBUG("Delete ns: " << nsid << "...");
                        DeleteNamespace(device, nsid);
                    }
                }

                // Re-read controller for updated unallocated capacity
                ctrl = NVMeIdentifyCtrl(device);

                STORAGE_DEBUG(
                    "NVMe capacity after deleting namespaces: unallocated="
                    << ctrl.unvmcap[0] << " bytes total=" << ctrl.tnvmcap[0]
                    << " bytes");

                const auto [lbaFormatIndex, blockSize] = PickLbaFormat(device);

                const ui64 unvmcap = ctrl.unvmcap[0];
                const ui64 tnvmcap = ctrl.tnvmcap[0];
                const ui64 capacity = unvmcap ? unvmcap : tnvmcap;
                const ui64 totalBlocks = capacity / blockSize;

                STORAGE_DEBUG(
                    "Create a namespace with "
                    << FormatByteSize(capacity) << " and LBA format #"
                    << static_cast<int>(lbaFormatIndex) << " (" << blockSize
                    << " B)");

                const ui32 nsid =
                    CreateNamespace(device, totalBlocks, lbaFormatIndex);

                AttachNamespace(device, nsid, ctrl.cntlid);

                return MakeError(S_OK);
            });
    }

    // returns (LBA format index, block size)
    auto PickLbaFormat(TFileHandle& device) -> std::pair<ui8, ui32>
    {
        STORAGE_DEBUG("Create a temporary namespace to query actual formats");

        // Create a temporary namespace to query actual formats

        const ui32 totalBlocks = 4096;
        const ui32 nsid = CreateNamespace(
            device,
            totalBlocks,
            0);   // lbaFormatIndex

        STORAGE_DEBUG("Query formats");
        auto ns = IdentifyAllocatedNs(device, nsid);

        STORAGE_DEBUG("Delete the temporary namespace");
        DeleteNamespace(device, nsid);

        std::span formats(ns.lbaf, ns.nlbaf + 1);

        // Pick the most performant format without metadata
        auto cmp = [](const auto& lhs, const auto& rhs)
        {
            return std::tie(lhs.ms, lhs.rp) < std::tie(rhs.ms, rhs.rp);
        };

        auto it = std::ranges::min_element(formats, cmp);

        return {std::distance(formats.begin(), it), 1U << it->lbads};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

INvmeManagerPtr CreateNvmeManager(ILoggingServicePtr logging, TDuration timeout)
{
    return std::make_shared<TNvmeManager>(
        std::move(logging),
        CreateLongRunningTaskExecutor("SecureErase"),
        timeout);
}

}   // namespace NCloud::NBlockStore::NNvme
