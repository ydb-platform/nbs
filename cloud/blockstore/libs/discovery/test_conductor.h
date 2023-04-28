#pragma once

#include <util/generic/string.h>

#include <memory>

namespace NCloud::NBlockStore::NDiscovery {

////////////////////////////////////////////////////////////////////////////////

struct THostInfo
{
    TString Group;
    TString Host;
};

class TFakeConductor
{
public:
    TFakeConductor(ui16 port);
    ~TFakeConductor();

public:
    void ForkStart();
    void Start();
    void SetHostInfo(TVector<THostInfo> hostInfo);
    void SetDrop(TString group, bool drop);

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;
};

}   // namespace NCloud::NBlockStore::NDiscovery
