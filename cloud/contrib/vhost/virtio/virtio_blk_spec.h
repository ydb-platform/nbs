/*
 * virtio blk protocol definitions according to virtio 1.0 spec
 */

#pragma once

#include "platform.h"
#include "virtio_types.h"

#ifdef __cplusplus
extern "C" {
#endif

#define VIRTIO_BLK_SECTOR_SIZE      512
#define VIRTIO_BLK_SECTOR_SHIFT     9
#define VIRTIO_BLK_DISKID_LENGTH    20
#define VIRTIO_BLK_STATUS_LENGTH    1

/* Feature bits */
#define VIRTIO_BLK_F_SIZE_MAX   1   /* Maximum size of any single segment is in size_max. */
#define VIRTIO_BLK_F_SEG_MAX    2   /* Maximum number of segments in a request is in seg_max. */
#define VIRTIO_BLK_F_GEOMETRY   4   /* Disk-style geometry specified in geometry. */
#define VIRTIO_BLK_F_RO         5   /* Device is read-only. */
#define VIRTIO_BLK_F_BLK_SIZE   6   /* Block size of disk is in blk_size. */
#define VIRTIO_BLK_F_FLUSH      9   /* Cache flush command support. */
#define VIRTIO_BLK_F_TOPOLOGY   10  /* Device exports information on optimal I/O alignment. */
#define VIRTIO_BLK_F_CONFIG_WCE 11  /* Device can toggle its cache between writeback and writethrough modes. */

/* Custom extentions */
#define VIRTIO_BLK_F_MQ         12  /* Device reports maximum supported queues in numqueues config field */

/* Legacy interface: feature bits */
#define VIRTIO_BLK_F_BARRIER    0   /* Device supports request barriers. */
#define VIRTIO_BLK_F_SCSI       7   /* Device supports scsi packet commands. */

/*
 * Device configuration layout.
 * The capacity of the device (expressed in 512-byte sectors) is always present.
 * The availability of the others all depend on various feature bits as
 * indicated above.
 */
struct VHD_PACKED virtio_blk_config {
    le64 capacity;
    le32 size_max;
    le32 seg_max;
    struct VHD_PACKED virtio_blk_geometry {
        le16 cylinders;
        u8 heads;
        u8 sectors;
    } geometry;
    le32 blk_size;
    struct VHD_PACKED virtio_blk_topology {
        /* # of logical blocks per physical block (log2) */
        u8 physical_block_exp;
        /* offset of first aligned logical block */
        u8 alignment_offset;
        /* suggested minimum I/O size in blocks */
        le16 min_io_size;
        /* optimal (suggested maximum) I/O size in blocks */
        le32 opt_io_size;
    } topology;
    u8 writeback;
    u8 _reserved;
    le16 numqueues;
};

/*
 * Device Operation
 * The driver queues requests to the virtqueue, and they are used by the device
 * (not necessarily in order).
 *
 * Request is a variable sized structure:
 * struct virtio_blk_req {
 *     le32 type;
 *     le32 reserved;
 *     le64 sector;
 *     u8 data[][512];
 *     u8 status;
 * };
 */
struct virtio_blk_req_hdr {
#define VIRTIO_BLK_T_IN         0   /* Device read */
#define VIRTIO_BLK_T_OUT        1   /* Device write */
#define VIRTIO_BLK_T_FLUSH      4   /* Flush */
#define VIRTIO_BLK_T_GET_ID     8   /* Get device id */
    le32 type;
    le32 reserved;
    le64 sector;
};

VHD_STATIC_ASSERT(sizeof(struct virtio_blk_req_hdr) == 16);

#define VIRTIO_BLK_S_OK         0
#define VIRTIO_BLK_S_IOERR      1
#define VIRTIO_BLK_S_UNSUPP     2

#ifdef __cplusplus
}
#endif
