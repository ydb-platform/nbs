#include "utils.h"

#include <util/stream/str.h>

#include <filesystem>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

static bool IsTcpAddress(int family)
{
    return family == AF_INET || family == AF_INET6;
}

static bool IsUnixAddress(int family)
{
    return family == AF_UNIX;
}

bool IsTcpAddress(const NAddr::IRemoteAddr& addr)
{
    return IsTcpAddress(addr.Addr()->sa_family);
}

bool IsTcpAddress(const TNetworkAddress& addr)
{
    return IsTcpAddress(addr.Begin()->ai_family);
}

bool IsUnixAddress(const TNetworkAddress& addr)
{
    return IsUnixAddress(addr.Begin()->ai_family);
}

TString PrintHostAndPort(const TNetworkAddress& addr)
{
    TStringStream out;
    for (auto it = addr.Begin(), end = addr.End(); it != end; ++it) {
        out << NAddr::PrintHostAndPort(NAddr::TAddrInfo(&*it));
        out << " ";
    }
    return out.Str();
}

TString FindFreeNbdDevice(const TString& sysBlockDir)
{
    auto sysBlockItems = std::filesystem::directory_iterator{
        sysBlockDir.c_str()};

    for (const auto& entry: sysBlockItems) {
        if (!entry.is_directory()) {
            continue;
        }
        TString nbd = entry.path().filename().string();
        if (!nbd.StartsWith("nbd")) {
            continue;
        }

        if (std::filesystem::exists(entry.path() / "pid")) {
            continue;
        }

        return "/dev/" + nbd;
    }

    return "";
}

}   // namespace NCloud::NBlockStore::NBD
