/* Reproduce RNR-induced head-of-line blocking on an ordered RC QP. */
#define _POSIX_C_SOURCE 200809L

#include <infiniband/verbs.h>

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

enum {
    Port = 1,
    DefaultRecvDepth = 256,
    DefaultSendDepth = 512,
    StallMs = 1000,
    CompletionTimeoutMs = 10000,
};

static uint64_t NowMs(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static void SleepMs(long milliseconds)
{
    struct timespec ts = {
        .tv_sec = milliseconds / 1000,
        .tv_nsec = (milliseconds % 1000) * 1000000,
    };
    while (nanosleep(&ts, &ts) && errno == EINTR) {
    }
}

static void Die(const char* what, int error)
{
    fprintf(stderr, "ERROR: %s: %s\n", what, strerror(error ? error : errno));
    exit(1);
}

static void CheckWc(const struct ibv_wc* wc, const char* cqName)
{
    if (wc->status != IBV_WC_SUCCESS) {
        fprintf(stderr,
                "ERROR: %s completion wr_id=%" PRIu64 " status=%s (%d) vendor=%u\n",
                cqName,
                wc->wr_id,
                ibv_wc_status_str(wc->status),
                wc->status,
                wc->vendor_err);
        exit(1);
    }
}

static void PollN(struct ibv_cq* cq, int count, int timeoutMs, const char* cqName)
{
    int completed = 0;
    uint64_t deadline = NowMs() + (uint64_t)timeoutMs;

    while (completed < count && NowMs() < deadline) {
        struct ibv_wc wc[32];
        int n = ibv_poll_cq(cq, 32, wc);
        if (n < 0) {
            Die("ibv_poll_cq", EIO);
        }
        for (int i = 0; i < n; ++i) {
            CheckWc(&wc[i], cqName);
        }
        completed += n;
        if (!n) {
            SleepMs(1);
        }
    }

    if (completed != count) {
        fprintf(stderr, "ERROR: %s got %d/%d completions before timeout\n",
                cqName, completed, count);
        exit(1);
    }
}

static void ExpectNoCompletion(struct ibv_cq* cq, int durationMs)
{
    uint64_t deadline = NowMs() + (uint64_t)durationMs;

    while (NowMs() < deadline) {
        struct ibv_wc wc[8];
        int n = ibv_poll_cq(cq, 8, wc);
        if (n < 0) {
            Die("ibv_poll_cq", EIO);
        }
        if (n) {
            for (int i = 0; i < n; ++i) {
                if (wc[i].status != IBV_WC_SUCCESS) {
                    CheckWc(&wc[i], "server CQ while stalled");
                }
            }
            fprintf(stderr,
                    "ERROR: %d server WR(s) completed while the first SEND had no receive\n",
                    n);
            exit(1);
        }
        SleepMs(1);
    }
}

static int FindGid(struct ibv_context* context,
                   const struct ibv_port_attr* portAttr,
                   union ibv_gid* gid)
{
    static const union ibv_gid zero = {};

    for (int i = 0; i < portAttr->gid_tbl_len; ++i) {
        if (!ibv_query_gid(context, Port, i, gid) &&
            memcmp(gid, &zero, sizeof(*gid)) != 0) {
            return i;
        }
    }
    return -1;
}

static void ConnectQp(struct ibv_qp* qp,
                      const struct ibv_port_attr* portAttr,
                      const union ibv_gid* gid,
                      int gidIndex,
                      uint32_t peerQpn)
{
    struct ibv_qp_attr attr = {
        .qp_state = IBV_QPS_INIT,
        .pkey_index = 0,
        .port_num = Port,
        .qp_access_flags = IBV_ACCESS_REMOTE_WRITE,
    };
    int rc = ibv_modify_qp(qp, &attr,
                           IBV_QP_STATE | IBV_QP_PKEY_INDEX |
                           IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
    if (rc) {
        Die("modify QP to INIT", rc);
    }

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = portAttr->active_mtu;
    attr.dest_qp_num = peerQpn;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    attr.ah_attr.port_num = Port;
    attr.ah_attr.sl = 0;
    if (portAttr->link_layer == IBV_LINK_LAYER_ETHERNET) {
        attr.ah_attr.is_global = 1;
        attr.ah_attr.grh.dgid = *gid;
        attr.ah_attr.grh.sgid_index = gidIndex;
        attr.ah_attr.grh.hop_limit = 1;
    } else {
        attr.ah_attr.dlid = portAttr->lid;
    }
    rc = ibv_modify_qp(qp, &attr,
                       IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                       IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                       IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
    if (rc) {
        Die("modify QP to RTR", rc);
    }

    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7; /* 7 means infinite RNR retries. */
    attr.sq_psn = 0;
    attr.max_rd_atomic = 1;
    rc = ibv_modify_qp(qp, &attr,
                       IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                       IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                       IBV_QP_MAX_QP_RD_ATOMIC);
    if (rc) {
        Die("modify QP to RTS", rc);
    }
}

static void PostReceives(struct ibv_qp* qp,
                         struct ibv_mr* mr,
                         uint64_t* buffers,
                         int count,
                         uint64_t firstWrId)
{
    struct ibv_sge* sge = calloc((size_t)count, sizeof(*sge));
    struct ibv_recv_wr* wr = calloc((size_t)count, sizeof(*wr));
    if (!sge || !wr) {
        Die("allocate receive WRs", ENOMEM);
    }

    for (int i = 0; i < count; ++i) {
        sge[i].addr = (uintptr_t)&buffers[i];
        sge[i].length = sizeof(buffers[i]);
        sge[i].lkey = mr->lkey;
        wr[i].wr_id = firstWrId + (uint64_t)i;
        wr[i].sg_list = &sge[i];
        wr[i].num_sge = 1;
        wr[i].next = i + 1 < count ? &wr[i + 1] : NULL;
    }

    struct ibv_recv_wr* bad = NULL;
    int rc = ibv_post_recv(qp, wr, &bad);
    free(wr);
    free(sge);
    if (rc) {
        Die("ibv_post_recv", rc);
    }
}

static void PostInitialSends(struct ibv_qp* qp,
                             struct ibv_mr* mr,
                             uint64_t* value,
                             int count)
{
    struct ibv_sge* sge = calloc((size_t)count, sizeof(*sge));
    struct ibv_send_wr* wr = calloc((size_t)count, sizeof(*wr));
    if (!sge || !wr) {
        Die("allocate initial send WRs", ENOMEM);
    }

    for (int i = 0; i < count; ++i) {
        sge[i].addr = (uintptr_t)value;
        sge[i].length = sizeof(*value);
        sge[i].lkey = mr->lkey;
        wr[i].wr_id = 0x2000u + (uint64_t)i;
        wr[i].sg_list = &sge[i];
        wr[i].num_sge = 1;
        wr[i].opcode = IBV_WR_SEND;
        wr[i].send_flags = IBV_SEND_SIGNALED;
        wr[i].next = i + 1 < count ? &wr[i + 1] : NULL;
    }

    struct ibv_send_wr* bad = NULL;
    int rc = ibv_post_send(qp, wr, &bad);
    free(wr);
    free(sge);
    if (rc) {
        Die("post initial SENDs", rc);
    }
}

static void PostStalledChain(struct ibv_qp* qp,
                             struct ibv_mr* sourceMr,
                             uint64_t* sendValue,
                             uint64_t* writeValue,
                             int count,
                             uint64_t remoteAddress,
                             uint32_t remoteKey)
{
    struct ibv_sge* sge = calloc((size_t)count, sizeof(*sge));
    struct ibv_send_wr* wr = calloc((size_t)count, sizeof(*wr));
    if (!sge || !wr) {
        Die("allocate stalled WR chain", ENOMEM);
    }

    for (int i = 0; i < count; ++i) {
        uint64_t* source = i ? writeValue : sendValue;
        sge[i].addr = (uintptr_t)source;
        sge[i].length = sizeof(*source);
        sge[i].lkey = sourceMr->lkey;
        wr[i].wr_id = 0x3000u + (uint64_t)i;
        wr[i].sg_list = &sge[i];
        wr[i].num_sge = 1;
        wr[i].opcode = i ? IBV_WR_RDMA_WRITE : IBV_WR_SEND;
        wr[i].send_flags = IBV_SEND_SIGNALED;
        wr[i].next = i + 1 < count ? &wr[i + 1] : NULL;
        if (i) {
            wr[i].wr.rdma.remote_addr = remoteAddress;
            wr[i].wr.rdma.rkey = remoteKey;
        }
    }

    struct ibv_send_wr* bad = NULL;
    int rc = ibv_post_send(qp, wr, &bad);
    free(wr);
    free(sge);
    if (rc) {
        Die("post stalled SEND/WRITE chain", rc);
    }
}

int main(int argc, char** argv)
{
    int recvDepth = argc > 1 ? atoi(argv[1]) : DefaultRecvDepth;
    int sendDepth = argc > 2 ? atoi(argv[2]) : DefaultSendDepth;
    if (recvDepth < 1 || sendDepth < 2) {
        fprintf(stderr, "usage: %s [receive-depth>=1] [send-depth>=2]\n", argv[0]);
        return 2;
    }

    int deviceCount = 0;
    struct ibv_device** devices = ibv_get_device_list(&deviceCount);
    if (!devices || !deviceCount) {
        Die("no RDMA device found", ENODEV);
    }
    struct ibv_context* context = ibv_open_device(devices[0]);
    if (!context) {
        Die("ibv_open_device", 0);
    }

    struct ibv_device_attr deviceAttr;
    struct ibv_port_attr portAttr;
    if (ibv_query_device(context, &deviceAttr)) {
        Die("ibv_query_device", 0);
    }
    if (ibv_query_port(context, Port, &portAttr)) {
        Die("ibv_query_port", 0);
    }
    if (portAttr.state != IBV_PORT_ACTIVE) {
        Die("RDMA port is not active", ENETDOWN);
    }
    if (sendDepth > deviceAttr.max_qp_wr || recvDepth > deviceAttr.max_qp_wr) {
        fprintf(stderr, "ERROR: requested queue depth exceeds device max_qp_wr=%d\n",
                deviceAttr.max_qp_wr);
        return 1;
    }

    union ibv_gid gid;
    int gidIndex = FindGid(context, &portAttr, &gid);
    if (portAttr.link_layer == IBV_LINK_LAYER_ETHERNET && gidIndex < 0) {
        Die("no usable GID found", ENXIO);
    }

    struct ibv_pd* pd = ibv_alloc_pd(context);
    if (!pd) {
        Die("ibv_alloc_pd", 0);
    }
    struct ibv_cq* clientCq = ibv_create_cq(context, recvDepth + 16, NULL, NULL, 0);
    struct ibv_cq* serverCq = ibv_create_cq(context, recvDepth + sendDepth + 16,
                                            NULL, NULL, 0);
    if (!clientCq || !serverCq) {
        Die("ibv_create_cq", 0);
    }

    struct ibv_qp_init_attr clientInit = {
        .send_cq = clientCq,
        .recv_cq = clientCq,
        .qp_type = IBV_QPT_RC,
        .cap = {
            .max_send_wr = 1,
            .max_recv_wr = (uint32_t)recvDepth,
            .max_send_sge = 1,
            .max_recv_sge = 1,
        },
    };
    struct ibv_qp_init_attr serverInit = {
        .send_cq = serverCq,
        .recv_cq = serverCq,
        .qp_type = IBV_QPT_RC,
        .cap = {
            .max_send_wr = (uint32_t)sendDepth,
            .max_recv_wr = 1,
            .max_send_sge = 1,
            .max_recv_sge = 1,
        },
    };
    struct ibv_qp* clientQp = ibv_create_qp(pd, &clientInit);
    struct ibv_qp* serverQp = ibv_create_qp(pd, &serverInit);
    if (!clientQp || !serverQp) {
        Die("ibv_create_qp", 0);
    }

    ConnectQp(clientQp, &portAttr, &gid, gidIndex, serverQp->qp_num);
    ConnectQp(serverQp, &portAttr, &gid, gidIndex, clientQp->qp_num);

    uint64_t* receiveBuffers = calloc((size_t)recvDepth + 1, sizeof(*receiveBuffers));
    volatile uint64_t* remoteTarget = calloc(1, sizeof(*remoteTarget));
    uint64_t* source = calloc(2, sizeof(*source));
    if (!receiveBuffers || !remoteTarget || !source) {
        Die("allocate buffers", ENOMEM);
    }
    source[0] = UINT64_C(0x53534e445f524e52); /* SEND_RNR */
    source[1] = UINT64_C(0x57524954455f4f4b); /* WRITE_OK */

    struct ibv_mr* receiveMr = ibv_reg_mr(pd, receiveBuffers,
                                           ((size_t)recvDepth + 1) * sizeof(*receiveBuffers),
                                           IBV_ACCESS_LOCAL_WRITE);
    struct ibv_mr* targetMr = ibv_reg_mr(pd, (void*)remoteTarget, sizeof(*remoteTarget),
                                         IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
    struct ibv_mr* sourceMr = ibv_reg_mr(pd, source, 2 * sizeof(*source), 0);
    if (!receiveMr || !targetMr || !sourceMr) {
        Die("ibv_reg_mr", 0);
    }

    printf("device=%s recv_depth=%d server_send_depth=%d rnr_retry=7\n",
           ibv_get_device_name(devices[0]), recvDepth, sendDepth);
    printf("1. Posting and consuming all %d client receive WRs...\n", recvDepth);
    PostReceives(clientQp, receiveMr, receiveBuffers, recvDepth, 0x1000);
    PostInitialSends(serverQp, sourceMr, &source[0], recvDepth);
    PollN(serverCq, recvDepth, CompletionTimeoutMs, "initial server CQ");
    PollN(clientCq, recvDepth, CompletionTimeoutMs, "initial client CQ");

    printf("2. Client RQ is empty. Posting one SEND followed by %d RDMA WRITEs...\n",
           sendDepth - 1);
    PostStalledChain(serverQp, sourceMr, &source[0], &source[1], sendDepth,
                     (uintptr_t)remoteTarget, targetMr->rkey);
    ExpectNoCompletion(serverCq, StallMs);
    if (*remoteTarget != 0) {
        fprintf(stderr, "ERROR: RDMA WRITE bypassed the RNR-blocked SEND\n");
        return 1;
    }
    printf("PASS: for %d ms, 0/%d server WRs completed and the RDMA target stayed unchanged.\n",
           StallMs, sendDepth);

    printf("3. Reposting one client receive WR to release RNR...\n");
    PostReceives(clientQp, receiveMr, &receiveBuffers[recvDepth], 1, 0x4000);
    PollN(serverCq, sendDepth, CompletionTimeoutMs, "released server CQ");
    PollN(clientCq, 1, CompletionTimeoutMs, "released client CQ");
    if (*remoteTarget != source[1]) {
        fprintf(stderr,
                "ERROR: remote target=%" PRIx64 ", expected=%" PRIx64 "\n",
                *remoteTarget, source[1]);
        return 1;
    }
    printf("PASS: all %d blocked WRs completed after one receive was reposted; "
           "RDMA target=%" PRIx64 ".\n", sendDepth, *remoteTarget);
    printf("REPRODUCED: an RNR-blocked SEND held the ordered RC send queue full, "
           "including later RDMA WRITEs.\n");

    ibv_dereg_mr(sourceMr);
    ibv_dereg_mr(targetMr);
    ibv_dereg_mr(receiveMr);
    ibv_destroy_qp(serverQp);
    ibv_destroy_qp(clientQp);
    ibv_destroy_cq(serverCq);
    ibv_destroy_cq(clientCq);
    ibv_dealloc_pd(pd);
    ibv_close_device(context);
    ibv_free_device_list(devices);
    free(source);
    free((void*)remoteTarget);
    free(receiveBuffers);
    return 0;
}
