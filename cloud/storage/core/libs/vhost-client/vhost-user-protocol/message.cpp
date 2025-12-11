#include "message.h"

#include <sys/socket.h>
#include <sys/un.h>

namespace NVHostUser {

////////////////////////////////////////////////////////////////////////////////

int IMessage::Send(int sock, const TVhostUserMsgBase& data)
{
    iovec iov[1];
    iov[0].iov_base = (void*)&data;
    iov[0].iov_len = sizeof(TVhostUserMsgBase) + data.AdditionDataSize;

    msghdr msgh;
    memset(&msgh, 0, sizeof(msgh));
    msgh.msg_iov = iov;
    msgh.msg_iovlen = 1;
    msgh.msg_control = 0;
    msgh.msg_controllen = 0;

    int ret;
    do {
        ret = sendmsg(sock, &msgh, 0);
    } while (ret < 0 && errno == EINTR);

    return ret;
}

int IMessage::SendFds(
    int sock,
    const TVhostUserMsgBase& data,
    const TVector<int>& fds)
{
    iovec iov[1];
    iov[0].iov_base = (void*)&data;
    iov[0].iov_len = sizeof(TVhostUserMsgBase) + data.AdditionDataSize;

    msghdr msgh;
    memset(&msgh, 0, sizeof(msgh));
    msgh.msg_iov = iov;
    msgh.msg_iovlen = 1;

    size_t fd_size = fds.size() * sizeof(int);
    char control[CMSG_SPACE(fd_size)];
    memset(control, 0, sizeof(control));

    msgh.msg_control = control;
    msgh.msg_controllen = sizeof(control);

    cmsghdr* cmsg;
    cmsg = CMSG_FIRSTHDR(&msgh);
    cmsg->cmsg_len = CMSG_LEN(fd_size);
    cmsg->cmsg_level = SOL_SOCKET;
    cmsg->cmsg_type = SCM_RIGHTS;
    memcpy(CMSG_DATA(cmsg), fds.data(), fd_size);

    int ret;
    do {
        ret = sendmsg(sock, &msgh, 0);
    } while (ret < 0 && errno == EINTR);

    return ret;
}

int IMessage::Recv(int sock, TVhostUserMsgBase& data)
{
    iovec iov[1];
    iov[0].iov_base = (void*)&data;
    iov[0].iov_len = sizeof(TVhostUserMsgBase) + data.AdditionDataSize;

    msghdr msgh;
    memset(&msgh, 0, sizeof(msgh));
    msgh.msg_iov = iov;
    msgh.msg_iovlen = 1;
    msgh.msg_control = 0;
    msgh.msg_controllen = 0;

    int ret = recvmsg(sock, &msgh, 0);
    if (ret > 0 && (msgh.msg_flags & (MSG_TRUNC | MSG_CTRUNC))) {
        return -1;
    }

    return ret;
}

}   // namespace NVHostUser
