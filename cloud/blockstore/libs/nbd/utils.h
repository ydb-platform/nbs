#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/block_data_ref.h>

#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/network/address.h>
#include <util/network/socket.h>

namespace NCloud::NBlockStore::NBD {

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
    const TString& mountInfoFile = "/proc/self/mountinfo");

TVector<TString> FindLoopbackDevices(
    const TSet<TString>& mountedFiles,
    const TString& sysBlockDir = "/sys/block/");

int RemoveLoopbackDevice(const TString& loopDevice);

}   // namespace NCloud::NBlockStore::NBD
