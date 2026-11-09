#include "jaegertracing/net/IPAddress.h"

#include <ifaddrs.h>
#include <net/if.h>

namespace {
ifaddrs interfaces[4] = {};
sockaddr_in loopback = {}, pod = {};
sockaddr_in6 pod6 = {};
}

// Model the interface ordering that made the legacy SDK report 127.0.0.1.
extern "C" int getifaddrs(ifaddrs** result) noexcept
{
    *result = interfaces;
    return 0;
}
extern "C" void freeifaddrs(ifaddrs*) noexcept {}

int main()
{
    loopback.sin_family = AF_INET;
    inet_pton(AF_INET, "127.0.0.1", &loopback.sin_addr);
    pod.sin_family = AF_INET;
    inet_pton(AF_INET, "10.244.2.7", &pod.sin_addr);
    pod6.sin6_family = AF_INET6;
    inet_pton(AF_INET6, "fd00:1234::beef", &pod6.sin6_addr);
    interfaces[0].ifa_addr = reinterpret_cast<sockaddr*>(&loopback);
    interfaces[0].ifa_flags = IFF_LOOPBACK;
    interfaces[0].ifa_next = interfaces + 1;
    // Interfaces without an address must not prevent selection of a later pod IP.
    interfaces[1].ifa_next = interfaces + 2;
    interfaces[2].ifa_addr = reinterpret_cast<sockaddr*>(&pod);
    interfaces[2].ifa_next = interfaces + 3;
    interfaces[3].ifa_addr = reinterpret_cast<sockaddr*>(&pod6);

    using jaegertracing::net::IPAddress;
    if (IPAddress::localIP(AF_INET).host() != "10.244.2.7") return 1;
    // Check copying from the selected interface, not full SDK IPv6 support.
    const auto selected6 = IPAddress::localIP(AF_INET6);
    const auto& raw6 = reinterpret_cast<const sockaddr_in6&>(selected6.addr());
    if (std::memcmp(&raw6.sin6_addr, &pod6.sin6_addr, sizeof(pod6.sin6_addr))) return 2;
    if (IPAddress::localIP([](const ifaddrs* item) {
            return item->ifa_addr && (item->ifa_flags & IFF_LOOPBACK);
        }).host() != "127.0.0.1") return 3;
    interfaces[0].ifa_next = nullptr;
    if (IPAddress::localIP(AF_INET).host() == "127.0.0.1") return 4;
}
