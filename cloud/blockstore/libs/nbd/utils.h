#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/block_data_ref.h>

#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/network/address.h>
#include <util/network/socket.h>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

constexpr auto MOUNT_INFO_FILE = "/proc/self/mountinfo";
constexpr auto SYS_BLOCK_DIR = "/sys/block/";

////////////////////////////////////////////////////////////////////////////////

inline TStringBuf AsStringBuf(const TBuffer& buffer)
{
    return { buffer.Data(), buffer.Size() };
}

inline TBlockDataRef AsBlockDataRef(const TBuffer& buffer)
{
    if (buffer.Empty()) {
        return {};
    }
    return { buffer.Data(), buffer.Size() };
}

bool IsTcpAddress(const NAddr::IRemoteAddr& addr);
bool IsTcpAddress(const TNetworkAddress& addr);

bool IsUnixAddress(const TNetworkAddress& addr);

TString PrintHostAndPort(const TNetworkAddress& addr);

TSet<TString> FindMountedFiles(
    const TString& device,
    const TString& mountInfoFile = MOUNT_INFO_FILE);

TVector<TString> FindLoopbackDevices(
    const TSet<TString>& mountedFiles,
    const TString& sysBlockDir = SYS_BLOCK_DIR);

int RemoveLoopbackDevice(const TString& loopDevice);

TString FindFreeNbdDevice(const TString& sysBlockDir = SYS_BLOCK_DIR);

}   // namespace NCloud::NBlockStore::NBD
