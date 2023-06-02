#pragma once

// spdk headers are not C++ friendly
extern "C" {

#include <rte_lcore.h>

#include <spdk/bdev.h>
#include <spdk/bdev_module.h>
#include <spdk/env.h>
#include <spdk/event.h>
#include <spdk/histogram_data.h>
#include <spdk/log.h>
#include <spdk/nvme.h>
#include <spdk/nvme_spec.h>
#include <spdk/nvmf.h>
#include <spdk/nvmf_spec.h>
#include <spdk/thread.h>
#include <spdk/util.h>
#include <spdk/uuid.h>

#include <bdev/aio/bdev_aio.h>
#include <bdev/iscsi/bdev_iscsi.h>
#include <bdev/malloc/bdev_malloc.h>
#include <bdev/null/bdev_null.h>
#include <bdev/nvme/bdev_nvme.h>

// spdk/lib/nvme/nvme_internal.h
int nvme_ctrlr_cmd_format(
    struct spdk_nvme_ctrlr *ctrlr,
    uint32_t nsid,
    struct spdk_nvme_format *format,
    spdk_nvme_cmd_cb cb_fn,
    void *cb_arg);

// spdk/lib/iscsi/init_grp.h
int iscsi_init_grp_create_from_initiator_list(
    int tag,
    int num_initiator_names, char **initiator_names,
    int num_initiator_masks, char **initiator_masks);

struct spdk_iscsi_init_grp *iscsi_init_grp_unregister(int tag);
void iscsi_init_grp_destroy(struct spdk_iscsi_init_grp *ig);

// spdk/lib/iscsi/portal_grp.h
struct spdk_iscsi_portal *iscsi_portal_create(const char *host, const char *port);
void iscsi_portal_destroy(struct spdk_iscsi_portal *p);

struct spdk_iscsi_portal_grp *iscsi_portal_grp_create(int tag, bool is_private);
void iscsi_portal_grp_release(struct spdk_iscsi_portal_grp *pg);

void iscsi_portal_grp_add_portal(
    struct spdk_iscsi_portal_grp *pg,
    struct spdk_iscsi_portal *p);

int iscsi_portal_grp_open(struct spdk_iscsi_portal_grp *pg, bool pause);
int iscsi_portal_grp_register(struct spdk_iscsi_portal_grp *pg);

// spdk/lib/isci/tgt_node.h
struct spdk_iscsi_tgt_node *iscsi_tgt_node_construct(
    int target_index,
    const char *name, const char *alias,
    int *pg_tag_list, int *ig_tag_list, uint16_t num_maps,
    const char *bdev_name_list[], int *lun_id_list, int num_luns,
    int queue_depth,
    bool disable_chap, bool require_chap, bool mutual_chap, int chap_group,
    bool header_digest, bool data_digest);

typedef void (*iscsi_tgt_node_destruct_cb)(void *cb_arg, int rc);
void iscsi_shutdown_tgt_node_by_name(
    const char *target_name,
    iscsi_tgt_node_destruct_cb cb_fn, void *cb_arg);

}   // extern "C"
