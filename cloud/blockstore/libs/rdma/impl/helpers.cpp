#include "helpers.h"

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

using TInterfaceAddressListPtr =
    std::unique_ptr<struct ifaddrs, decltype(&freeifaddrs)>;

TInterfaceAddressListPtr WrapPtr(struct ifaddrs* ptr)
{
    return {ptr, freeifaddrs};
}

bool IsIpv6AddrGlobal(const struct sockaddr_in6* sa6)
{
    return !(
        IN6_IS_ADDR_LINKLOCAL(&(sa6->sin6_addr)) ||
        IN6_IS_ADDR_LOOPBACK(&(sa6->sin6_addr)) ||
        IN6_IS_ADDR_SITELOCAL(&(sa6->sin6_addr)) ||
        IN6_IS_ADDR_MULTICAST(&(sa6->sin6_addr)) ||
        (sa6->sin6_addr.s6_addr[0] == 0xfd ||
         sa6->sin6_addr.s6_addr[0] == 0xfc));
}

TInterfaceAddressListPtr GetInterfaceAddressList()
{
    struct ifaddrs* ifaddr = nullptr;

    while (getifaddrs(&ifaddr) == -1) {
        if (errno != EAGAIN) {
            return WrapPtr(nullptr);
        }
    }

    return WrapPtr(ifaddr);
}

TString ResolveHostFromInterface(const TString& interface)
{
    auto ifaddr = GetInterfaceAddressList();
    if (!ifaddr) {
        return "::";
    }
    char addrBuf[INET6_ADDRSTRLEN];

    for (auto* ifa = ifaddr.get(); ifa != nullptr; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == nullptr) {
            continue;
        }
        if (interface != TStringBuf(ifa->ifa_name)) {
            continue;
        }

        int family = ifa->ifa_addr->sa_family;

        if (family != AF_INET6) {   // Only ipv6 is supported
            continue;
        }
        auto* sa6 = reinterpret_cast<struct sockaddr_in6*>(ifa->ifa_addr);

        if (!IsIpv6AddrGlobal(sa6)) {
            continue;
        }

        inet_ntop(AF_INET6, &(sa6->sin6_addr), addrBuf, INET6_ADDRSTRLEN);

        return {addrBuf};
    }

    return "::";
}

}   // namespace NCloud::NBlockStore::NRdma
