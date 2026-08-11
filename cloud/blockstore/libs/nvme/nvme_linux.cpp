#include "nvme.h"

#include "utils.h"

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/stream/format.h>
#include <util/string/builder.h>
#include <util/string/join.h>
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

TString ToString(const TVector<ui8>& opcodes)
{
    TStringBuilder out;

    out << "[ ";
    for (ui8 opcode: opcodes) {
        out << Hex(opcode, HF_ADDX) << " ";
    }
    out << "]";

    return out;
}

void InvokeAdminCmd(TFileHandle& file, nvme_admin_cmd& cmd, TStringBuf source)
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

auto ListAllocatedNamespaces(TFileHandle& file, TDuration timeout)
    -> TVector<ui32>
{
    ui32 namespaceIDs[1024]{};

    nvme_admin_cmd cmd = {
        .opcode = NVME_OPC_IDENTIFY,
        .addr = std::bit_cast<ui64>(&namespaceIDs[0]),
        .data_len = sizeof(namespaceIDs),
        .cdw10 = NVME_IDENTIFY_ALLOCATED_NS_LIST,
        .timeout_ms = static_cast<ui32>(timeout.MilliSeconds())};

    InvokeAdminCmd(file, cmd, "identify allocated ns list");

    return {std::begin(namespaceIDs), std::ranges::find(namespaceIDs, 0U)};
}

ui32 CreateNamespace(
    TFileHandle& file,
    ui64 totalBlocks,
    ui8 lbaFormatIndex,
    TDuration timeout)
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
        .timeout_ms = static_cast<ui32>(timeout.MilliSeconds())};

    InvokeAdminCmd(file, cmd, "create ns");

    return cmd.result;
}

void DeleteNamespace(TFileHandle& file, ui32 nsid, TDuration timeout)
{
    nvme_admin_cmd cmd = {
        .opcode = NVME_OPC_NS_MANAGEMENT,
        .nsid = nsid,
        .addr = 0,
        .data_len = 0,
        .cdw10 = 1,   // delete
        .timeout_ms = static_cast<ui32>(timeout.MilliSeconds())};
    InvokeAdminCmd(file, cmd, "delete ns");
}

void AttachNamespace(
    TFileHandle& file,
    ui32 nsid,
    ui16 ctrlId,
    TDuration timeout)
{
    nvme_ctrlr_list ctrlList{.num = 1, .identifiers = {ctrlId}};

    nvme_admin_cmd cmd = {
        .opcode = NVME_OPC_NS_ATTACHMENT,
        .nsid = nsid,
        .addr = std::bit_cast<ui64>(&ctrlList),
        .data_len = sizeof(ctrlList),
        .cdw10 = 0,   // attach
        .timeout_ms = static_cast<ui32>(timeout.MilliSeconds())};
    InvokeAdminCmd(file, cmd, "attach ns");
}

void DetachNamespaceFromAll(TFileHandle& file, ui32 nsid, TDuration timeout)
{
    nvme_ctrlr_list data{};

    nvme_admin_cmd cmd{
        .opcode = NVME_OPC_IDENTIFY,
        .nsid = nsid,
        .addr = std::bit_cast<ui64>(&data),
        .data_len = sizeof(data),
        .cdw10 = NVME_IDENTIFY_NS_ATTACHED_CTRLR_LIST,
        .timeout_ms = static_cast<ui32>(timeout.MilliSeconds())};

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
        .timeout_ms = static_cast<ui32>(timeout.MilliSeconds())};
    InvokeAdminCmd(file, cmd, "detach ns from all ctrls");
}

nvme_ns_data
IdentifyAllocatedNs(TFileHandle& device, ui32 nsid, TDuration timeout)
{
    nvme_ns_data ns{};
    nvme_admin_cmd cmd = {
        .opcode = NVME_OPC_IDENTIFY,
        .nsid = nsid,
        .addr = std::bit_cast<ui64>(&ns),
        .data_len = sizeof(ns),
        .cdw10 = NVME_IDENTIFY_NS_ALLOCATED,
        .timeout_ms = static_cast<ui32>(timeout.MilliSeconds())};

    InvokeAdminCmd(device, cmd, "identify allocated ns");

    return ns;
}

nvme_ctrlr_data NVMeIdentifyCtrl(TFileHandle& device, TDuration timeout)
{
    nvme_ctrlr_data ctrl = {};

    nvme_admin_cmd cmd = {
        .opcode = NVME_OPC_IDENTIFY,
        .addr = static_cast<ui64>(reinterpret_cast<uintptr_t>(&ctrl)),
        .data_len = sizeof(ctrl),
        .cdw10 = NVME_IDENTIFY_CTRLR,
        .timeout_ms = static_cast<ui32>(timeout.MilliSeconds())};

    int err = ioctl(device, NVME_IOCTL_ADMIN_CMD, &cmd);

    if (err) {
        int err = errno;
        STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
            << "NVMeIdentifyCtrl failed: " << strerror(err);
    }

    return ctrl;
}

nvme_ns_data NVMeIdentifyNs(TFileHandle& device, ui32 nsId, TDuration timeout)
{
    nvme_ns_data ns = {};

    nvme_admin_cmd cmd = {
        .opcode = NVME_OPC_IDENTIFY,
        .nsid = nsId,
        .addr = static_cast<ui64>(reinterpret_cast<uintptr_t>(&ns)),
        .data_len = sizeof(ns),
        .cdw10 = NVME_IDENTIFY_NS,
        .timeout_ms = static_cast<ui32>(timeout.MilliSeconds())};

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
        .timeout_ms = static_cast<ui32>(timeout.MilliSeconds())};

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
    hd_driveid hd{};
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

ui32 GetSanitizeAction(TFileHandle& device, TDuration timeout)
{
    nvme_ctrlr_data ctrl = NVMeIdentifyCtrl(device, timeout);
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

TFileHandle OpenCtrl(const TString& ctrlPath)
{
    TFileHandle device(ctrlPath, OpenExisting | RdWr);
    if (!device.IsOpen()) {
        int err = errno;
        STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
            << "Failed to open file " << ctrlPath.Quote();
    }

    return device;
}

////////////////////////////////////////////////////////////////////////////////

class TNvmeManager final: public INvmeManager
{
private:
    const ILoggingServicePtr Logging;
    TLog Log;

    ITaskQueuePtr Executor;
    TDuration SecureEraseTimeout;
    TDuration AdminCmdTimeout;

    void FormatImpl(const TString& path, nvme_secure_erase_setting ses)
    {
        TFileHandle device(path, OpenExisting | RdOnly);

        Y_ENSURE(
            IsBlockOrCharDevice(device),
            "expected block or character device");

        nvme_ctrlr_data ctrl = NVMeIdentifyCtrl(device, AdminCmdTimeout);

        Y_ENSURE(ctrl.fna.format_all_ns == 0, "can't format single namespace");
        Y_ENSURE(ctrl.fna.erase_all_ns == 0, "can't erase single namespace");
        Y_ENSURE(
            ses != NVME_FMT_NVM_SES_CRYPTO_ERASE ||
                ctrl.fna.crypto_erase_supported == 1,
            "cryptographic erase is not supported");

        const int nsId = ioctl(device, NVME_IOCTL_ID);

        Y_ENSURE(nsId > 0, "unexpected namespace id");

        nvme_ns_data ns =
            NVMeIdentifyNs(device, static_cast<ui32>(nsId), AdminCmdTimeout);

        Y_ENSURE(ns.lbaf[ns.flbas.format].ms == 0, "unexpected metadata");

        nvme_format format{.lbaf = ns.flbas.format, .ses = ses};

        NVMeFormatImpl(device, nsId, format, SecureEraseTimeout);
    }

    void DeallocateImpl(const TString& path, ui64 offsetBytes, ui64 sizeBytes)
    {
        TFileHandle device(path, OpenExisting | RdWr);
        Y_ENSURE(
            IsBlockOrCharDevice(device),
            "expected block or character device");

        ui64 devSizeBytes = 0;
        int err = ioctl(device, BLKGETSIZE64, &devSizeBytes);
        if (err) {
            err = errno;
            STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
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
            STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
                << "NVMeDeallocateImpl failed to deallocate: " << strerror(err);
        }
    }

public:
    TNvmeManager(
        ILoggingServicePtr logging,
        ITaskQueuePtr executor,
        TDuration secureEraseTimeout,
        TDuration adminCmdTimeout)
        : Logging(std::move(logging))
        , Executor(executor)
        , SecureEraseTimeout(secureEraseTimeout)
        , AdminCmdTimeout(adminCmdTimeout)
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
    Deallocate(const TString& path, ui64 offsetBytes, ui64 sizeBytes) override
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

    TResultOrError<TString> GetSerialNumber(const TString& path) override
    {
        return SafeExecute<TResultOrError<TString>>(
            [&]
            {
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

                auto ctrl = NVMeIdentifyCtrl(device, AdminCmdTimeout);

                return str(ctrl.sn);
            });
    }

    TResultOrError<TString> GetDeviceModel(const TString& path) override
    {
        return SafeExecute<TResultOrError<TString>>(
            [&]
            {
                TFileHandle device(path, OpenExisting | RdOnly);

                auto str = [](auto& arr)
                {
                    auto* model = std::bit_cast<const char*>(&arr[0]);
                    auto end = std::find(model, model + sizeof(arr), '\0');

                    return TString(model, end);
                };

                auto [isRot, error] = IsRotational(device);

                if (!HasError(error) && isRot) {
                    auto hd = HDIdentity(device);
                    return str(hd.model);
                }

                auto ctrl = NVMeIdentifyCtrl(device, AdminCmdTimeout);

                return str(ctrl.mn);
            });
    }

    TResultOrError<bool> IsSsd(const TString& path) override
    {
        return SafeExecute<TResultOrError<bool>>(
            [&]
            {
                TFileHandle device(path, OpenExisting | RdOnly);

                auto [isRot, error] = IsRotational(device);
                if (HasError(error)) {
                    STORAGE_THROW_SERVICE_ERROR(error.GetCode())
                        << "NVMeIsSsd failed: " << error.GetMessage();
                }

                return TResultOrError{!isRot};
            });
    }

    NProto::TError StartSanitize(const TString& ctrlPath) override
    {
        return SafeExecute<NProto::TError>(
            [&]
            {
                TFileHandle device = OpenCtrl(ctrlPath);

                nvme_admin_cmd cmd{
                    .opcode = NVME_OPC_SANITIZE,
                    .cdw10 = GetSanitizeAction(device, AdminCmdTimeout),
                    .timeout_ms =
                        static_cast<ui32>(AdminCmdTimeout.MilliSeconds())};

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
                TFileHandle device = OpenCtrl(ctrlPath);

                char buffer[4]{};

                const ui32 numd = (sizeof(buffer) / 4) - 1;

                nvme_admin_cmd cmd{
                    .opcode = NVME_OPC_GET_LOG_PAGE,
                    .addr = std::bit_cast<ui64>(&buffer[0]),
                    .data_len = sizeof(buffer),
                    .cdw10 = NVME_LOG_LID_SANITIZE | (numd << 16),
                    .timeout_ms =
                        static_cast<ui32>(AdminCmdTimeout.MilliSeconds())};

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
                TFileHandle device = OpenCtrl(ctrlPath);

                nvme_ctrlr_data ctrl =
                    NVMeIdentifyCtrl(device, AdminCmdTimeout);

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

                while (auto nsIds =
                           ListAllocatedNamespaces(device, AdminCmdTimeout))
                {
                    for (ui32 nsid: nsIds) {
                        STORAGE_DEBUG("Detach ns: " << nsid << "...");
                        DetachNamespaceFromAll(device, nsid, AdminCmdTimeout);

                        STORAGE_DEBUG("Delete ns: " << nsid << "...");
                        DeleteNamespace(device, nsid, AdminCmdTimeout);
                    }
                }

                // Re-read controller for updated unallocated capacity
                ctrl = NVMeIdentifyCtrl(device, AdminCmdTimeout);

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

                const ui32 nsid = CreateNamespace(
                    device,
                    totalBlocks,
                    lbaFormatIndex,
                    AdminCmdTimeout);

                AttachNamespace(device, nsid, ctrl.cntlid, AdminCmdTimeout);

                return MakeError(S_OK);
            });
    }

    TVector<ui8> ReadLockdownList(
        TFileHandle& device,
        nvme_lockdown_log_scope scope,
        nvme_lockdown_log_contents contents)
    {
        nvme_lockdown_log log{};

        //
        // Get Log Page CDW10:
        //
        //   [31:16] NUMDL
        //   [15]    RAE = 0
        //   [14]    Reserved
        //   [13:12] CNTTS (Contents)
        //   [11:8]  SCP (Scope)
        //   [7:0]   LID
        //
        // NUMDL and NUMDU form a 0-based number of dwords to transfer.
        // nvme_lockdown_log is 512 bytes:
        //     512 / sizeof(uint32_t) - 1 = 127
        //

        const uint32_t numd = sizeof(nvme_lockdown_log) / sizeof(uint32_t) - 1;
        const uint32_t scp = static_cast<uint8_t>(scope);
        const uint32_t cntts = static_cast<uint8_t>(contents);

        nvme_admin_cmd cmd = {
            .opcode = NVME_OPC_GET_LOG_PAGE,
            .nsid = 0,
            .addr = reinterpret_cast<uint64_t>(&log),
            .data_len = sizeof(log),
            .cdw10 = (numd << 16) | (cntts << 12) | (scp << 8) |
                     NVME_LOG_LID_CMD_AND_FEAT_LOCKDOWN,
            .cdw11 = 0,   // LSI = 0, NUMDU = 0
            .timeout_ms = static_cast<ui32>(AdminCmdTimeout.MilliSeconds()),
        };

        const int rc = ioctl(device, NVME_IOCTL_ADMIN_CMD, &cmd);
        if (rc < 0) {
            const int err = errno;
            STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(err))
                << "NVME_IOCTL_ADMIN_CMD(Get Log Page)";
        }

        if (rc != 0) {
            STORAGE_THROW_SERVICE_ERROR(E_FAIL)
                << "Get Lockdown Log failed, NVMe status=" << Hex(rc, HF_ADDX);
        }

        // Validate what the controller says it returned.

        const uint8_t returnedScope =
            (log.cfila >> NVME_LOCKDOWN_SS_SHIFT) & NVME_LOCKDOWN_SS_MASK;
        const uint8_t returnedContents =
            (log.cfila >> NVME_LOCKDOWN_CS_SHIFT) & NVME_LOCKDOWN_CS_MASK;

        if (returnedScope != scp || returnedContents != cntts) {
            STORAGE_THROW_SERVICE_ERROR(E_INVALID_STATE)
                << "Lockdown log returned unexpected scope/contents";
        }

        return {log.cfil, log.cfil + log.lngth};
    }

    TLockdownScopeState ReadLockdownScopeState(
        TFileHandle& device,
        nvme_lockdown_log_scope scope)
    {
        return {
            .Supported =
                ReadLockdownList(device, scope, NVME_LOCKDOWN_SUPPORTED_CMD),
            .Prohibited =
                ReadLockdownList(device, scope, NVME_LOCKDOWN_PROHIBITED_CMD),
        };
    }

    TResultOrError<TLockdownState> GetLockdownState(
        const TString& ctrlPath) final
    {
        return SafeExecute<TResultOrError<TLockdownState>>(
            [&]
            {
                TFileHandle device = OpenCtrl(ctrlPath);

                auto ctrl = NVMeIdentifyCtrl(device, AdminCmdTimeout);
                if (!ctrl.oacs.lockdown) {
                    return TLockdownState{
                        .Supported = false,
                    };
                }

                return TLockdownState{
                    .Supported = true,
                    .AdminCmd =
                        ReadLockdownScopeState(device, NVME_LOCKDOWN_ADMIN_CMD),
                    .FeatureId = ReadLockdownScopeState(
                        device,
                        NVME_LOCKDOWN_FEATURE_ID),
                };
            });
    }

    static TVector<ui8> CalculateOpcodesToLock(
        TVector<ui8> allowedOpcodes,
        const TLockdownScopeState& scope)
    {
        return NNvme::CalculateOpcodesToLock(
            std::move(allowedOpcodes),
            scope.Supported,
            scope.Prohibited);
    }

    NProto::TError EnsureLockdown(
        const TString& ctrlPath,
        const TLockdownConfig& config) final
    {
        return SafeExecute<NProto::TError>(
            [&]
            {
                TFileHandle device = OpenCtrl(ctrlPath);

                auto ctrl = NVMeIdentifyCtrl(device, AdminCmdTimeout);
                if (!ctrl.oacs.lockdown) {
                    return MakeError(
                        MAKE_SYSTEM_ERROR(ENOTSUP),
                        "Lockdown command is not supported");
                }

                auto allowedAdminOpcodes = config.AllowedAdminOpcodes;

                if (config.BlockLockdownCommand) {
                    std::erase(allowedAdminOpcodes, NVME_OPS_ADMIN_LOCKDOWN);
                } else {
                    allowedAdminOpcodes.push_back(NVME_OPS_ADMIN_LOCKDOWN);
                }

                auto adminOpcodesToLock = CalculateOpcodesToLock(
                    std::move(allowedAdminOpcodes),
                    ReadLockdownScopeState(device, NVME_LOCKDOWN_ADMIN_CMD));

                auto featureIdsToLock = CalculateOpcodesToLock(
                    config.AllowedSetFeatureIds,
                    ReadLockdownScopeState(device, NVME_LOCKDOWN_FEATURE_ID));

                if (adminOpcodesToLock.empty() && featureIdsToLock.empty()) {
                    return MakeError(S_ALREADY);
                }

                Lockdown(
                    device,
                    std::move(adminOpcodesToLock),
                    std::move(featureIdsToLock));

                return MakeError(S_OK);
            });
    }

    void LockdownItem(
        TFileHandle& device,
        nvme_lockdown_log_scope scope,
        ui8 opcodeOrFeatureId)
    {
        constexpr ui32 ifc = 0;     // Admin Submission Queue
        constexpr ui32 prhbt = 1;   // Prohibit

        nvme_admin_cmd cmd = {
            .opcode = NVME_OPS_ADMIN_LOCKDOWN,
            .addr = 0,
            .data_len = 0,
            .cdw10 = (static_cast<ui32>(opcodeOrFeatureId) << 8) | (ifc << 5) |
                     (prhbt << 4) | static_cast<ui32>(scope),
            .cdw14 = 0,
            .timeout_ms = static_cast<ui32>(AdminCmdTimeout.MilliSeconds()),
        };

        InvokeAdminCmd(
            device,
            cmd,
            TStringBuilder()
                << "Lockdown item " << Hex(opcodeOrFeatureId, HF_ADDX)
                << " in scope " << Hex(scope, HF_ADDX));
    }

    void Lockdown(
        TFileHandle& device,
        TVector<ui8> adminOpcodesToLock,
        TVector<ui8> featureIdsToLock)
    {
        SortUnique(adminOpcodesToLock);
        SortUnique(featureIdsToLock);

        STORAGE_INFO(
            "Lockdown. Opcodes: " << ToString(adminOpcodesToLock)
                                  << " features: "
                                  << ToString(featureIdsToLock));

        for (ui8 feat: featureIdsToLock) {
            LockdownItem(device, NVME_LOCKDOWN_FEATURE_ID, feat);
        }

        // Lock the Lockdown command itself last, so that all other
        // lockdown operations can still be performed.
        auto it =
            std::ranges::find(adminOpcodesToLock, NVME_OPS_ADMIN_LOCKDOWN);
        if (it != adminOpcodesToLock.end()) {
            std::iter_swap(it, adminOpcodesToLock.rbegin());
        }

        for (ui8 opcode: adminOpcodesToLock) {
            LockdownItem(device, NVME_LOCKDOWN_ADMIN_CMD, opcode);
        }
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
            0,   // lbaFormatIndex
            AdminCmdTimeout);

        STORAGE_DEBUG("Query formats");
        auto ns = IdentifyAllocatedNs(device, nsid, AdminCmdTimeout);

        STORAGE_DEBUG("Delete the temporary namespace");
        DeleteNamespace(device, nsid, AdminCmdTimeout);

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

INvmeManagerPtr CreateNvmeManager(
    ILoggingServicePtr logging,
    TDuration secureEraseTimeout,
    TDuration adminCmdTimeout)
{
    return std::make_shared<TNvmeManager>(
        std::move(logging),
        CreateLongRunningTaskExecutor("SecureErase"),
        secureEraseTimeout,
        adminCmdTimeout);
}

}   // namespace NCloud::NBlockStore::NNvme
