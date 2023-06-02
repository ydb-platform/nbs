#include "address.h"

#include "spdk.h"

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NCloud::NBlockStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

TString CreateNqnFromUuid(const TGUID& uuid)
{
    char buf[SPDK_UUID_STRING_LEN];
    spdk_uuid_fmt_lower(
        buf,
        SPDK_UUID_STRING_LEN,
        reinterpret_cast<const spdk_uuid*>(&uuid));

    return TStringBuilder()
        << TStringBuf(SPDK_NVMF_NQN_UUID_PRE, SPDK_NVMF_NQN_UUID_PRE_LEN)
        << TStringBuf(buf, SPDK_UUID_STRING_LEN - 1);
}

TString CreatePCIeDeviceTransportId(const TString& deviceAddress)
{
    auto out = TStringBuilder()
        << "trtype:PCIe";
    if (deviceAddress) {
        out << " traddr:" << deviceAddress;
    }
    return out;
}

TString CreateNVMeDeviceTransportId(
    const TString& transport,
    const TString& family,
    const TString& host,
    int port,
    const TString& nqn)
{
    auto out = TStringBuilder()
        << "trtype:" << transport
        << " adrfam:" << family
        << " traddr:" << host
        << " trsvcid:" << port;
    if (nqn) {
        out << " subnqn:" << nqn;
    }
    return out;
}

size_t GetNSIDFromTransportId(TStringBuf s)
{
    const TStringBuf sep = ":=";
    const TStringBuf whitespace = " \t\n";

    for (;;) {
        s.Skip(s.find_first_not_of(whitespace));

        size_t i = s.find_first_of(sep);
        if (i == TStringBuf::npos) {
            return 0;
        }

        const TStringBuf name = s.Head(i);
        s.Skip(i + 1);

        i = s.find_first_of(whitespace);
        const TStringBuf value = s.Head(i);
        s.Skip(i + 1);

        if (name == "ns") {
            return FromString<unsigned>(value);
        }
    }

    return 0;
}

TString CreateNvmeNqn(const TString& domain, const TString& ident)
{
    auto out = TStringBuilder()
        << "nqn." << domain;
    if (ident) {
        out << ":" << ident;
    }
    return out;
}

TString CreateScsiIqn(const TString& domain, const TString& ident)
{
    auto out = TStringBuilder()
        << "iqn." << domain;
    if (ident) {
        out << ":" << ident;
    }
    return out;
}

TString CreateScsiUrl(
    const TString& host,
    int port,
    const TString& iqn,
    int lun,
    const TString& username,
    const TString& password)
{
    auto out = TStringBuilder() << "iscsi://";

    if (username) {
        out << username;
        if (password) {
            out << "%" << password;
        }
        out << "@";
    }

    out << host;
    if (port) {
        out << ":" << port;
    }

    out << "/" << iqn << "/" << lun;
    return out;
}

}   // namespace NCloud::NBlockStore::NSpdk
