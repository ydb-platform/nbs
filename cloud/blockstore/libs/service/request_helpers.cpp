#include "request_helpers.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

bool IsReadWriteMode(const NProto::EVolumeAccessMode mode)
{
    switch (mode) {
        case NProto::VOLUME_ACCESS_READ_ONLY:
        case NProto::VOLUME_ACCESS_USER_READ_ONLY:
            return false;
        case NProto::VOLUME_ACCESS_READ_WRITE:
        case NProto::VOLUME_ACCESS_REPAIR:
            return true;
        default:
            Y_VERIFY_DEBUG(false, "Unknown EVolumeAccessMode: %d", mode);
            return false;
    }
}

TString GetIpcTypeString(NProto::EClientIpcType ipcType)
{
    switch (ipcType) {
        case NProto::IPC_GRPC:
            return "grpc";
        case NProto::IPC_NBD:
            return "nbd";
        case NProto::IPC_VHOST:
            return "vhost";
        case NProto::IPC_NVME:
            return "nvme";
        case NProto::IPC_SCSI:
            return "scsi";
        case NProto::IPC_RDMA:
            return "rdma";
        default:
            return "undefined";
    }
}

}   // namespace NCloud::NBlockStore
