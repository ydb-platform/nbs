#pragma once

#include "message.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/task_queue.h>

#include <utility>

#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/network/socket.h>
#include <util/system/error.h>

namespace NCloud::NNetlink {

template <typename TResponse = TNetlinkMessage, typename TRequest>
NThreading::TFuture<TNetlinkResponse<TResponse>> Send(
    ITaskQueuePtr executor,
    TRequest msg,
    ui32 socketTimeoutMs = 100)
{
    auto sent = executor->Execute(
        [msg = std::move(msg), socketTimeoutMs]() mutable {
            TSocket socket(::socket(PF_NETLINK, SOCK_RAW, NETLINK_GENERIC));
            if (socket < 0) {
                STORAGE_THROW_SERVICE_ERROR(
                    MAKE_SYSTEM_ERROR(LastSystemError()))
                    << "Failed to create netlink socket";
            }
            socket.SetSocketTimeout(0, socketTimeoutMs);

            auto ret = socket.Send(&msg, sizeof(msg));
            if (ret == -1) {
                STORAGE_THROW_SERVICE_ERROR(
                    MAKE_SYSTEM_ERROR(LastSystemError()))
                    << "Failed to send netlink message";
            }
            return socket;
        });

    return sent.Apply([executor = std::move(executor)](const auto& result) {
        return executor->Execute([socket = result.GetValue()]() mutable {
            TNetlinkResponse<TResponse> response;
            auto ret = socket.Recv(&response, sizeof(response));
            if (ret < 0) {
                STORAGE_THROW_SERVICE_ERROR(
                    MAKE_SYSTEM_ERROR(LastSystemError()))
                    << "Failed to receive netlink message";
            }
            if (response.NetlinkError.MessageHeader.nlmsg_type == NLMSG_ERROR) {
                if (response.NetlinkError.MessageError.error != 0) {
                    STORAGE_THROW_SERVICE_ERROR(MAKE_SYSTEM_ERROR(
                        response.NetlinkError.MessageError.error))
                        << "Netlink error";
                }
            }
            if (!NLMSG_OK(&response.NetlinkError.MessageHeader, ret)) {
                STORAGE_THROW_SERVICE_ERROR(MAKE_ERROR(E_FAIL))
                    << "Netlink message has incorrect format";
            }
            response.Msg.Validate();
            return response;
        });
    });
}

template <size_t FamilyNameLength>
NThreading::TFuture<ui16> GetFamilyId(
    ITaskQueuePtr executor,
    const char (&familyName)[FamilyNameLength])
{
    return Send<TNetlinkFamilyIdResponse<FamilyNameLength>>(
        std::move(executor),
        TNetlinkFamilyIdRequest(familyName))
        .Apply([](const auto& result) {
            return result.GetValue().Msg.FamilyId;
        });
}

}   // namespace NCloud::NNetlink
