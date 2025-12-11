#pragma once

#include "message.h"

namespace NVHostUser {

////////////////////////////////////////////////////////////////////////////////

class TSetVringAddr: public IMessage
{
private:
    uint32_t Index;
    uint32_t Flags;
    uint64_t DescriptorTable;
    uint64_t UsedRing;
    uint64_t AvailableRing;
    uint64_t Logging;

public:
    TSetVringAddr(
        uint32_t index,
        uint32_t flags,
        uint64_t descriptorTable,
        uint64_t usedRing,
        uint64_t availableRing,
        uint64_t logging)
        : Index(index)
        , Flags(flags)
        , DescriptorTable(descriptorTable)
        , UsedRing(usedRing)
        , AvailableRing(availableRing)
        , Logging(logging)
    {}

    bool Execute(int sock) override
    {
        struct Y_PACKED
        {
            TVhostUserMsgBase Base;
            uint32_t Index;
            uint32_t Flags;
            uint64_t DescriptorTable;
            uint64_t UsedRing;
            uint64_t AvailableRing;
            uint64_t Logging;
        } request;

        request.Base.Request = VHOST_USER_SET_VRING_ADDR;
        request.Base.Flags = 1;
        request.Base.AdditionDataSize = sizeof(uint32_t) + sizeof(uint32_t) +
                                        sizeof(uint64_t) + sizeof(uint64_t) +
                                        sizeof(uint64_t) + sizeof(uint64_t);
        request.Index = Index;
        request.Flags = Flags;
        request.DescriptorTable = DescriptorTable;
        request.UsedRing = UsedRing;
        request.AvailableRing = AvailableRing;
        request.Logging = Logging;

        if (Send(sock, request.Base) < 0) {
            return false;
        }
        return true;
    }

    TString ToString() const override
    {
        return "VHOST_USER_SET_VRING_ADDR";
    }
};

}   // namespace NVHostUser
