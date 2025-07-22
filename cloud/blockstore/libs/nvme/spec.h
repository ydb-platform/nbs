#pragma once

#include <cstdint>

namespace NCloud::NBlockStore::NNvme {

////////////////////////////////////////////////////////////////////////////////

union nvme_vs_register {
    uint32_t raw;
    struct {
        /** indicates the tertiary version */
        uint32_t ter : 8;
        /** indicates the minor version */
        uint32_t mnr : 8;
        /** indicates the major version */
        uint32_t mjr : 16;
    } bits;
};

/**
 * Admin opcodes
 */
enum nvme_admin_opcode {
    NVME_OPC_DELETE_IO_SQ = 0x00,
    NVME_OPC_CREATE_IO_SQ = 0x01,
    NVME_OPC_GET_LOG_PAGE = 0x02,
    /* 0x03 - reserved */
    NVME_OPC_DELETE_IO_CQ = 0x04,
    NVME_OPC_CREATE_IO_CQ = 0x05,
    NVME_OPC_IDENTIFY = 0x06,
    /* 0x07 - reserved */
    NVME_OPC_ABORT = 0x08,
    NVME_OPC_SET_FEATURES = 0x09,
    NVME_OPC_GET_FEATURES = 0x0a,
    /* 0x0b - reserved */
    NVME_OPC_ASYNC_EVENT_REQUEST = 0x0c,
    NVME_OPC_NS_MANAGEMENT = 0x0d,
    /* 0x0e-0x0f - reserved */
    NVME_OPC_FIRMWARE_COMMIT = 0x10,
    NVME_OPC_FIRMWARE_IMAGE_DOWNLOAD = 0x11,

    NVME_OPC_DEVICE_SELF_TEST = 0x14,
    NVME_OPC_NS_ATTACHMENT = 0x15,

    NVME_OPC_KEEP_ALIVE = 0x18,
    NVME_OPC_DIRECTIVE_SEND = 0x19,
    NVME_OPC_DIRECTIVE_RECEIVE = 0x1a,

    NVME_OPC_VIRTUALIZATION_MANAGEMENT = 0x1c,
    NVME_OPC_NVME_MI_SEND = 0x1d,
    NVME_OPC_NVME_MI_RECEIVE = 0x1e,

    NVME_OPC_DOORBELL_BUFFER_CONFIG = 0x7c,

    NVME_OPC_FORMAT_NVM = 0x80,
    NVME_OPC_SECURITY_SEND = 0x81,
    NVME_OPC_SECURITY_RECEIVE = 0x82,

    NVME_OPC_SANITIZE = 0x84,

    NVME_OPC_GET_LBA_STATUS = 0x86,
};

struct nvme_power_state {
    uint16_t mp; /* bits 15:00: maximum power */

    uint8_t reserved1;

    uint8_t mps : 1; /* bit 24: max power scale */
    uint8_t nops : 1; /* bit 25: non-operational state */
    uint8_t reserved2 : 6;

    uint32_t enlat; /* bits 63:32: entry latency in microseconds */
    uint32_t exlat; /* bits 95:64: exit latency in microseconds */

    uint8_t rrt : 5; /* bits 100:96: relative read throughput */
    uint8_t reserved3 : 3;

    uint8_t rrl : 5; /* bits 108:104: relative read latency */
    uint8_t reserved4 : 3;

    uint8_t rwt : 5; /* bits 116:112: relative write throughput */
    uint8_t reserved5 : 3;

    uint8_t rwl : 5; /* bits 124:120: relative write latency */
    uint8_t reserved6 : 3;

    uint8_t reserved7[16];
};

/** Identify command CNS value */
enum nvme_identify_cns {
    /** Identify namespace indicated in CDW1.NSID */
    NVME_IDENTIFY_NS = 0x00,

    /** Identify controller */
    NVME_IDENTIFY_CTRLR = 0x01,

    /** List active NSIDs greater than CDW1.NSID */
    NVME_IDENTIFY_ACTIVE_NS_LIST = 0x02,

    /** List namespace identification descriptors */
    NVME_IDENTIFY_NS_ID_DESCRIPTOR_LIST = 0x03,

    /** Identify namespace indicated in CDW1.NSID, specific to CWD11.CSI */
    NVME_IDENTIFY_NS_IOCS = 0x05,

    /** Identify controller, specific to CWD11.CSI */
    NVME_IDENTIFY_CTRLR_IOCS = 0x06,

    /** List active NSIDs greater than CDW1.NSID, specific to CWD11.CSI */
    NVME_IDENTIFY_ACTIVE_NS_LIST_IOCS = 0x07,

    /** List allocated NSIDs greater than CDW1.NSID */
    NVME_IDENTIFY_ALLOCATED_NS_LIST = 0x10,

    /** Identify namespace if CDW1.NSID is allocated */
    NVME_IDENTIFY_NS_ALLOCATED = 0x11,

    /** Get list of controllers starting at CDW10.CNTID that are attached to CDW1.NSID */
    NVME_IDENTIFY_NS_ATTACHED_CTRLR_LIST = 0x12,

    /** Get list of controllers starting at CDW10.CNTID */
    NVME_IDENTIFY_CTRLR_LIST = 0x13,

    /** Get primary controller capabilities structure */
    NVME_IDENTIFY_PRIMARY_CTRLR_CAP = 0x14,

    /** Get secondary controller list */
    NVME_IDENTIFY_SECONDARY_CTRLR_LIST = 0x15,

    /** List allocated NSIDs greater than CDW1.NSID, specific to CWD11.CSI */
    NVME_IDENTIFY_ALLOCATED_NS_LIST_IOCS = 0x1a,

    /** Identify namespace if CDW1.NSID is allocated, specific to CDWD11.CSI */
    NVME_IDENTIFY_NS_ALLOCATED_IOCS = 0x1b,

    /** Identify I/O Command Sets */
    NVME_IDENTIFY_IOCS = 0x1c,
};

#define NVME_CTRLR_SN_LEN 20
#define NVME_CTRLR_MN_LEN 40
#define NVME_CTRLR_FR_LEN 8

#define NVME_NQN_FIELD_SIZE 256

/** Identify Controller data NVMe over Fabrics-specific fields */
struct nvme_cdata_nvmf_specific {
    /** I/O queue command capsule supported size (16-byte units) */
    uint32_t ioccsz;

    /** I/O queue response capsule supported size (16-byte units) */
    uint32_t iorcsz;

    /** In-capsule data offset (16-byte units) */
    uint16_t icdoff;

    /** Controller attributes */
    struct {
        /** Controller model: \ref nvmf_ctrlr_model */
        uint8_t ctrlr_model : 1;
        uint8_t reserved : 7;
    } ctrattr;

    /** Maximum SGL block descriptors (0 = no limit) */
    uint8_t msdbd;

    uint8_t reserved[244];
};

/** Identify Controller data SGL support */
struct nvme_cdata_sgls {
    uint32_t supported : 2;
    uint32_t keyed_sgl : 1;
    uint32_t reserved1 : 13;
    uint32_t bit_bucket_descriptor : 1;
    uint32_t metadata_pointer : 1;
    uint32_t oversized_sgl : 1;
    uint32_t metadata_address : 1;
    uint32_t sgl_offset : 1;
    uint32_t transport_sgl : 1;
    uint32_t reserved2 : 10;
};

struct __attribute__((packed)) __attribute__((aligned)) nvme_ctrlr_data {
    /* bytes 0-255: controller capabilities and features */

    /** pci vendor id */
    uint16_t vid;

    /** pci subsystem vendor id */
    uint16_t ssvid;

    /** serial number */
    int8_t sn[NVME_CTRLR_SN_LEN];

    /** model number */
    int8_t mn[NVME_CTRLR_MN_LEN];

    /** firmware revision */
    uint8_t fr[NVME_CTRLR_FR_LEN];

    /** recommended arbitration burst */
    uint8_t rab;

    /** ieee oui identifier */
    uint8_t ieee[3];

    /** controller multi-path I/O and namespace sharing capabilities */
    struct {
        uint8_t multi_port : 1;
        uint8_t multi_host : 1;
        uint8_t sr_iov : 1;
        uint8_t ana_reporting : 1;
        uint8_t reserved : 4;
    } cmic;

    /** maximum data transfer size */
    uint8_t mdts;

    /** controller id */
    uint16_t cntlid;

    /** version */
    union nvme_vs_register ver;

    /** RTD3 resume latency */
    uint32_t rtd3r;

    /** RTD3 entry latency */
    uint32_t rtd3e;

    /** optional asynchronous events supported */
    struct {
        uint32_t reserved1 : 8;

        /** Supports sending Namespace Attribute Notices. */
        uint32_t ns_attribute_notices : 1;

        /** Supports sending Firmware Activation Notices. */
        uint32_t fw_activation_notices : 1;

        uint32_t reserved2 : 1;

        /** Supports Asymmetric Namespace Access Change Notices. */
        uint32_t ana_change_notices : 1;

        uint32_t reserved3 : 19;

        /** Supports Discovery log change notices (refer to the NVMe over Fabrics specification) */
        uint32_t discovery_log_change_notices : 1;

    } oaes;

    /** controller attributes */
    struct {
        /** Supports 128-bit host identifier */
        uint32_t host_id_exhid_supported: 1;

        /** Supports non-operational power state permissive mode */
        uint32_t non_operational_power_state_permissive_mode: 1;

        uint32_t reserved: 30;
    } ctratt;

    uint8_t reserved_100[12];

    /** FRU globally unique identifier */
    uint8_t fguid[16];

    /** Command Retry Delay Time 1, 2 and 3 */
    uint16_t crdt[3];

    uint8_t reserved_122[122];

    /* bytes 256-511: admin command set attributes */

    /** optional admin command support */
    struct {
        /* supports security send/receive commands */
        uint16_t security : 1;

        /* supports format nvm command */
        uint16_t format : 1;

        /* supports firmware activate/download commands */
        uint16_t firmware : 1;

        /* supports ns manage/ns attach commands */
        uint16_t ns_manage : 1;

        /** Supports device self-test command (NVME_OPC_DEVICE_SELF_TEST) */
        uint16_t device_self_test : 1;

        /** Supports NVME_OPC_DIRECTIVE_SEND and NVME_OPC_DIRECTIVE_RECEIVE */
        uint16_t directives : 1;

        /** Supports NVMe-MI (NVME_OPC_NVME_MI_SEND, NVME_OPC_NVME_MI_RECEIVE) */
        uint16_t nvme_mi : 1;

        /** Supports NVME_OPC_VIRTUALIZATION_MANAGEMENT */
        uint16_t virtualization_management : 1;

        /** Supports NVME_OPC_DOORBELL_BUFFER_CONFIG */
        uint16_t doorbell_buffer_config : 1;

        /** Supports NVME_OPC_GET_LBA_STATUS */
        uint16_t get_lba_status : 1;

        uint16_t oacs_rsvd : 6;
    } oacs;

    /** abort command limit */
    uint8_t acl;

    /** asynchronous event request limit */
    uint8_t aerl;

    /** firmware updates */
    struct {
        /* first slot is read-only */
        uint8_t slot1_ro : 1;

        /* number of firmware slots */
        uint8_t num_slots : 3;

        /* support activation without reset */
        uint8_t activation_without_reset : 1;

        uint8_t frmw_rsvd : 3;
    } frmw;

    /** log page attributes */
    struct {
        /* per namespace smart/health log page */
        uint8_t ns_smart : 1;
        /* command effects log page */
        uint8_t celp : 1;
        /* extended data for get log page */
        uint8_t edlp: 1;
        /** telemetry log pages and notices */
        uint8_t telemetry : 1;
        uint8_t lpa_rsvd : 4;
    } lpa;

    /** error log page entries */
    uint8_t elpe;

    /** number of power states supported */
    uint8_t npss;

    /** admin vendor specific command configuration */
    struct {
        /* admin vendor specific commands use disk format */
        uint8_t spec_format : 1;

        uint8_t avscc_rsvd : 7;
    } avscc;

    /** autonomous power state transition attributes */
    struct {
        /** controller supports autonomous power state transitions */
        uint8_t supported : 1;

        uint8_t apsta_rsvd : 7;
    } apsta;

    /** warning composite temperature threshold */
    uint16_t wctemp;

    /** critical composite temperature threshold */
    uint16_t cctemp;

    /** maximum time for firmware activation */
    uint16_t mtfa;

    /** host memory buffer preferred size */
    uint32_t hmpre;

    /** host memory buffer minimum size */
    uint32_t hmmin;

    /** total NVM capacity */
    uint64_t tnvmcap[2];

    /** unallocated NVM capacity */
    uint64_t unvmcap[2];

    /** replay protected memory block support */
    struct {
        uint8_t num_rpmb_units : 3;
        uint8_t auth_method : 3;
        uint8_t reserved1 : 2;

        uint8_t reserved2;

        uint8_t total_size;
        uint8_t access_size;
    } rpmbs;

    /** extended device self-test time (in minutes) */
    uint16_t edstt;

    /** device self-test options */
    union {
        uint8_t raw;
        struct {
            /** Device supports only one device self-test operation at a time */
            uint8_t one_only : 1;

            uint8_t reserved : 7;
        } bits;
    } dsto;

    /**
     * Firmware update granularity
     *
     * 4KB units
     * 0x00 = no information provided
     * 0xFF = no restriction
     */
    uint8_t fwug;

    /**
     * Keep Alive Support
     *
     * Granularity of keep alive timer in 100 ms units
     * 0 = keep alive not supported
     */
    uint16_t kas;

    /** Host controlled thermal management attributes */
    union {
        uint16_t raw;
        struct {
            uint16_t supported : 1;
            uint16_t reserved : 15;
        } bits;
    } hctma;

    /** Minimum thermal management temperature */
    uint16_t mntmt;

    /** Maximum thermal management temperature */
    uint16_t mxtmt;

    /** Sanitize capabilities */
    union {
        uint32_t raw;
        struct {
            uint32_t crypto_erase : 1;
            uint32_t block_erase : 1;
            uint32_t overwrite : 1;
            uint32_t reserved : 29;
        } bits;
    } sanicap;

    /* bytes 332-342 */
    uint8_t reserved3[10];

    /** ANA transition time */
    uint8_t anatt;

    /* bytes 343: Asymmetric namespace access capabilities */
    struct {
        uint8_t ana_optimized_state : 1;
        uint8_t ana_non_optimized_state : 1;
        uint8_t ana_inaccessible_state : 1;
        uint8_t ana_persistent_loss_state : 1;
        uint8_t ana_change_state : 1;
        uint8_t reserved : 1;
        uint8_t no_change_anagrpid : 1;
        uint8_t non_zero_anagrpid : 1;
    } anacap;

    /* bytes 344-347: ANA group identifier maximum */
    uint32_t anagrpmax;
    /* bytes 348-351: number of ANA group identifiers */
    uint32_t nanagrpid;

    /* bytes 352-511 */
    uint8_t reserved352[160];

    /* bytes 512-703: nvm command set attributes */

    /** submission queue entry size */
    struct {
        uint8_t min : 4;
        uint8_t max : 4;
    } sqes;

    /** completion queue entry size */
    struct {
        uint8_t min : 4;
        uint8_t max : 4;
    } cqes;

    uint16_t maxcmd;

    /** number of namespaces */
    uint32_t nn;

    /** optional nvm command support */
    struct {
        uint16_t compare : 1;
        uint16_t write_unc : 1;
        uint16_t dsm: 1;
        uint16_t write_zeroes: 1;
        uint16_t set_features_save: 1;
        uint16_t reservations: 1;
        uint16_t timestamp: 1;
        uint16_t reserved: 9;
    } oncs;

    /** fused operation support */
    struct {
        uint16_t compare_and_write : 1;
        uint16_t reserved : 15;
    } fuses;

    /** format nvm attributes */
    struct {
        uint8_t format_all_ns: 1;
        uint8_t erase_all_ns: 1;
        uint8_t crypto_erase_supported: 1;
        uint8_t reserved: 5;
    } fna;

    /** volatile write cache */
    struct {
        uint8_t present : 1;
        uint8_t flush_broadcast : 2;
        uint8_t reserved : 5;
    } vwc;

    /** atomic write unit normal */
    uint16_t awun;

    /** atomic write unit power fail */
    uint16_t awupf;

    /** NVM vendor specific command configuration */
    uint8_t nvscc;

    uint8_t reserved531;

    /** atomic compare & write unit */
    uint16_t acwu;

    uint16_t reserved534;

    struct nvme_cdata_sgls sgls;

    /* maximum number of allowed namespaces */
    uint32_t mnan;

    uint8_t reserved4[224];

    uint8_t subnqn[NVME_NQN_FIELD_SIZE];

    uint8_t reserved5[768];

    struct nvme_cdata_nvmf_specific nvmf_specific;

    /* bytes 2048-3071: power state descriptors */
    struct nvme_power_state psd[32];

    /* bytes 3072-4095: vendor specific */
    uint8_t vs[1024];
};

struct nvme_ns_data {
    /** namespace size */
    uint64_t nsze;

    /** namespace capacity */
    uint64_t ncap;

    /** namespace utilization */
    uint64_t nuse;

    /** namespace features */
    struct {
        /** thin provisioning */
        uint8_t thin_prov : 1;

        /** NAWUN, NAWUPF, and NACWU are defined for this namespace */
        uint8_t ns_atomic_write_unit : 1;

        /** Supports Deallocated or Unwritten LBA error for this namespace */
        uint8_t dealloc_or_unwritten_error : 1;

        /** Non-zero NGUID and EUI64 for namespace are never reused */
        uint8_t guid_never_reused : 1;

        /** Optimal Performance field */
        uint8_t optperf : 1;

        uint8_t reserved1 : 3;
    } nsfeat;

    /** number of lba formats */
    uint8_t nlbaf;

    /** formatted lba size */
    struct {
        uint8_t format : 4;
        uint8_t extended : 1;
        uint8_t reserved2 : 3;
    } flbas;

    /** metadata capabilities */
    struct {
        /** metadata can be transferred as part of data prp list */
        uint8_t extended : 1;

        /** metadata can be transferred with separate metadata pointer */
        uint8_t pointer : 1;

        /** reserved */
        uint8_t reserved3 : 6;
    } mc;

    /** end-to-end data protection capabilities */
    struct {
        /** protection information type 1 */
        uint8_t pit1 : 1;

        /** protection information type 2 */
        uint8_t pit2 : 1;

        /** protection information type 3 */
        uint8_t pit3 : 1;

        /** first eight bytes of metadata */
        uint8_t md_start : 1;

        /** last eight bytes of metadata */
        uint8_t md_end : 1;
    } dpc;

    /** end-to-end data protection type settings */
    struct {
        /** protection information type */
        uint8_t pit : 3;

        /** 1 == protection info transferred at start of metadata */
        /** 0 == protection info transferred at end of metadata */
        uint8_t md_start : 1;

        uint8_t reserved4 : 4;
    } dps;

    /** namespace multi-path I/O and namespace sharing capabilities */
    struct {
        uint8_t can_share : 1;
        uint8_t reserved : 7;
    } nmic;

    /** reservation capabilities */
    union {
        struct {
            /** supports persist through power loss */
            uint8_t persist : 1;

            /** supports write exclusive */
            uint8_t write_exclusive : 1;

            /** supports exclusive access */
            uint8_t exclusive_access : 1;

            /** supports write exclusive - registrants only */
            uint8_t write_exclusive_reg_only : 1;

            /** supports exclusive access - registrants only */
            uint8_t exclusive_access_reg_only : 1;

            /** supports write exclusive - all registrants */
            uint8_t write_exclusive_all_reg : 1;

            /** supports exclusive access - all registrants */
            uint8_t exclusive_access_all_reg : 1;

            /** supports ignore existing key */
            uint8_t ignore_existing_key : 1;
        } rescap;
        uint8_t raw;
    } nsrescap;
    /** format progress indicator */
    struct {
        uint8_t percentage_remaining : 7;
        uint8_t fpi_supported : 1;
    } fpi;

    /** deallocate logical features */
    union {
        uint8_t raw;
        struct {
            /**
             * Value read from deallocated blocks
             *
             * 000b = not reported
             * 001b = all bytes 0x00
             * 010b = all bytes 0xFF
             *
             * \ref nvme_dealloc_logical_block_read_value
             */
            uint8_t read_value : 3;

            /** Supports Deallocate bit in Write Zeroes */
            uint8_t write_zero_deallocate : 1;

            /**
             * Guard field behavior for deallocated logical blocks
             * 0: contains 0xFFFF
             * 1: contains CRC for read value
             */
            uint8_t guard_value : 1;

            uint8_t reserved : 3;
        } bits;
    } dlfeat;

    /** namespace atomic write unit normal */
    uint16_t nawun;

    /** namespace atomic write unit power fail */
    uint16_t nawupf;

    /** namespace atomic compare & write unit */
    uint16_t nacwu;

    /** namespace atomic boundary size normal */
    uint16_t nabsn;

    /** namespace atomic boundary offset */
    uint16_t nabo;

    /** namespace atomic boundary size power fail */
    uint16_t nabspf;

    /** namespace optimal I/O boundary in logical blocks */
    uint16_t noiob;

    /** NVM capacity */
    uint64_t nvmcap[2];

    /** Namespace Preferred Write Granularity */
    uint16_t npwg;

    /** Namespace Preferred Write Alignment */
    uint16_t npwa;

    /** Namespace Preferred Deallocate Granularity */
    uint16_t npdg;

    /** Namespace Preferred Deallocate Alignment */
    uint16_t npda;

    /** Namespace Optimal Write Size */
    uint16_t nows;

    uint8_t reserved64[18];

    /** ANA group identifier */
    uint32_t anagrpid;

    uint8_t reserved96[8];

    /** namespace globally unique identifier */
    uint8_t nguid[16];

    /** IEEE extended unique identifier */
    uint64_t eui64;

    /** lba format support */
    struct {
        /** metadata size */
        uint32_t ms : 16;

        /** lba data size */
        uint32_t lbads : 8;

        /** relative performance */
        uint32_t rp : 2;

        uint32_t reserved6 : 6;
    } lbaf[16];

    uint8_t reserved6[192];

    uint8_t vendor_specific[3712];
};

enum nvme_secure_erase_setting {
    NVME_FMT_NVM_SES_NO_SECURE_ERASE = 0x0,
    NVME_FMT_NVM_SES_USER_DATA_ERASE = 0x1,
    NVME_FMT_NVM_SES_CRYPTO_ERASE = 0x2,
};

struct nvme_format {
    uint32_t lbaf : 4;
    uint32_t ms : 1;
    uint32_t pi : 3;
    uint32_t pil : 1;
    uint32_t ses : 3;
    uint32_t reserved : 20;
};

}   // namespace NCloud::NBlockStore::NNvme
