#pragma once

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct DevicePath
{
    TString Protocol;
    TString Host;
    ui16 Port;
    TString Uuid;

    DevicePath(
        const TString protocol,
        const TString& host = {},
        ui16 port = 0,
        const TString& uuid = {})
        : Protocol(protocol)
        , Host(host)
        , Port(port)
        , Uuid(uuid)
    {}
    NProto::TError Parse(const TString& devicePath);
    TString Serialize() const;
};

}   // namespace NCloud::NBlockStore
