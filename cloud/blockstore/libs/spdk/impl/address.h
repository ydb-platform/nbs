#pragma once

#include "public.h"

#include <util/generic/guid.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

// nqn.2014-08.org.nvmexpress:uuid:11111111-2222-3333-4444-555555555555
TString CreateNqnFromUuid(const TGUID& uuid);

// trtype:PCIe[ traddr:<deviceAddress>]
TString CreatePCIeDeviceTransportId(const TString& deviceAddress = {});

// trtype:<transport> adrfam:<family> traddr:<host> trsvcid:<port>[ subnqn:<nqn>]
TString CreateNVMeDeviceTransportId(
    const TString& transport,
    const TString& family,
    const TString& host,
    int port,
    const TString& nqn = {});

size_t GetNSIDFromTransportId(TStringBuf transportId);

// nqn.<year-month.domain>[:<ident>]
TString CreateNvmeNqn(const TString& domain, const TString& ident = {});

// iqn.<year-month.domain>[:<ident>]
TString CreateScsiIqn(const TString& domain, const TString& ident = {});

// iscsi://[<username>[%<password>]@]<host>[:<port>]/<iqn>/<lun>'
TString CreateScsiUrl(
    const TString& host,
    int port,
    const TString& iqn,
    int lun,
    const TString& username = {},
    const TString& password = {});

}   // namespace NCloud::NBlockStore::NSpdk
