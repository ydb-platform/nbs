#pragma once

#include "message.h"

namespace NVHostUser {

////////////////////////////////////////////////////////////////////////////////

class TSetMemTable: public IMessage
{
public:
    struct Y_PACKED MemoryRegion
    {
        uint64_t GuestAddress;
        uint64_t Size;
        uint64_t UserAddress;
        uint64_t MmapOffset;
    };

private:
    TVector<MemoryRegion> Regions;
    TVector<int> Fds;

public:
    static constexpr int MAX_MEM_REGIONS = 8;

    TSetMemTable(TVector<MemoryRegion> regions, TVector<int> fds)
        : Regions(std::move(regions))
        , Fds(std::move(fds))
    {}

    bool Execute(int sock) override
    {
        if (Regions.size() > MAX_MEM_REGIONS || Regions.size() != Fds.size()) {
            return false;
        }

        struct Y_PACKED
        {
            TVhostUserMsgBase Base;
            uint32_t NumRegions;
            uint32_t Padding;
            MemoryRegion Regions[MAX_MEM_REGIONS];
        } request;

        request.Base.Request = VHOST_USER_SET_MEM_TABLE;
        request.Base.Flags = 1;
        request.Base.AdditionDataSize = Regions.size() * sizeof(MemoryRegion) +
                                        sizeof(request.NumRegions) +
                                        sizeof(request.Padding);
        request.NumRegions = Regions.size();
        request.Padding = 0;
        memcpy(
            request.Regions,
            Regions.data(),
            Regions.size() * sizeof(MemoryRegion));

        if (SendFds(sock, request.Base, Fds) < 0) {
            return false;
        }

        return true;
    }

    TString ToString() const override
    {
        return "VHOST_USER_SET_MEM_TABLE";
    }
};

}   // namespace NVHostUser
