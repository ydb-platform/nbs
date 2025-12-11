#pragma once

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#if defined(__cplusplus)
extern "C" {
#endif

/******************************************************************************/
/* Magics */

#define BLOCK_PLUGIN_MAGIC 0xDEAD1EE7
#define BLOCK_PLUGIN_HOST_MAGIC 0xCAFE540B

/******************************************************************************/
/*
 * Version history:
 *
 * 1.0: First commit
 * 2.0: Removed create/destroy/resize/stat volume callbacks from API
 * 3.0: Options now supplied during plugin init
 * 4.0: Added max and opt transfer sizes to volume info
 * 5.0: Added api to get dynamic counters
 * 5.1: Added async mount/unmount api
 * 5.2: Added new mount sync api call
 * 5.3: Added mount_seq_number to BlockPlugin_MountOpts
 * 5.4: Added instance_id to BlockPluginHost
 */

/* Major versions are not backwards compatible */
#define BLOCK_PLUGIN_API_VERSION_MAJOR 0x5
/* Minor versions are backwards compatible */
#define BLOCK_PLUGIN_API_VERSION_MINOR 0x4

/******************************************************************************/
/* NOTE: Should have an exact layout of QEMUIOVector! */

struct BlockPlugin_Buffer
{
    void* iov_base;
    uint32_t iov_len;
};

struct BlockPlugin_IOVector
{
    struct BlockPlugin_Buffer* iov;
    uint32_t niov;
    uint32_t nalloc;
    uint32_t size;
};

/******************************************************************************/
/* Completion callback */

enum BlockPlugin_CompletionStatus
{
    BP_COMPLETION_INVALID = 0,
    BP_COMPLETION_INPROGRESS,
    BP_COMPLETION_READ_FINISHED,
    BP_COMPLETION_WRITE_FINISHED,
    BP_COMPLETION_ZERO_FINISHED,
    BP_COMPLETION_ERROR,
    BP_COMPLETION_MOUNT_FINISHED,
    BP_COMPLETION_UNMOUNT_FINISHED,
};

struct BlockPlugin_Completion
{
    uint32_t status;
    uint64_t id;
    void* custom_data;
};

/******************************************************************************/
/* Mount options */

enum BlockPlugin_AccessMode
{
    BLOCK_PLUGIN_ACCESS_READ_WRITE = 0,
    BLOCK_PLUGIN_ACCESS_READ_ONLY = 1,
};

enum BlockPlugin_MountMode
{
    BLOCK_PLUGIN_MOUNT_LOCAL = 0,
    BLOCK_PLUGIN_MOUNT_REMOTE = 1,
};

struct BlockPlugin_MountOpts
{
    const char* volume_name;
    const char* mount_token;
    const char* instance_id;

    enum BlockPlugin_AccessMode access_mode;
    enum BlockPlugin_MountMode mount_mode;

    uint64_t mount_seq_number;
};

/******************************************************************************/
/* Supported request types */

enum BlockPlugin_RequestType
{
    BLOCK_PLUGIN_READ_BLOCKS = 0,
    BLOCK_PLUGIN_WRITE_BLOCKS,
    BLOCK_PLUGIN_ZERO_BLOCKS,
};

struct BlockPlugin_Request
{
    uint32_t type;
    uint32_t size;
};

struct BlockPlugin_ReadBlocks
{
    struct BlockPlugin_Request header;
    uint64_t start_index;
    uint32_t blocks_count;
    struct BlockPlugin_IOVector* bp_iov;
};

struct BlockPlugin_WriteBlocks
{
    struct BlockPlugin_Request header;
    uint64_t start_index;
    uint32_t blocks_count;
    struct BlockPlugin_IOVector* bp_iov;
};

struct BlockPlugin_ZeroBlocks
{
    struct BlockPlugin_Request header;
    uint64_t start_index;
    uint32_t blocks_count;
};

/******************************************************************************/
/* Volume definition */

struct BlockPlugin_Volume
{
    uint32_t block_size;
    uint64_t blocks_count;
    uint32_t max_transfer; /* in bytes, must be multiple of block size,
                            * 0 if no limit */
    uint32_t opt_transfer; /* in bytes, must be multiple of block size,
                            * 0 if no preference */

    void* state; /* must be set to 0 before the first MountVolume call */
};

/******************************************************************************/
/* API status codes */

enum BlockPlugin_Status
{
    BLOCK_PLUGIN_E_OK = 0,
    BLOCK_PLUGIN_E_FAIL = 1,
    BLOCK_PLUGIN_E_ARGUMENT = 2,
};

/******************************************************************************/
/* Exported plugin interface */

/* Callback type called from get_dynamic_counters */
typedef void (
    *BlockPlugin_GetCountersCallback)(const char* value, void* opaque);

/*
 * Layout of this struct is required to be consistent across versions
 * for compatibility
 * New members should be appended at the end and their presence must be detected
 * by checking BlockPlugin version in client
 */
struct BlockPlugin
{
    /* Magick and version should never change or move */
    uint32_t magic;
    uint32_t version_major;
    uint32_t version_minor;

    void* state;

    /* Mount existing volume */
    int (*mount)(
        struct BlockPlugin* plugin,
        const char* volume_name,
        const char* mount_token,
        struct BlockPlugin_Volume* volume);

    /* Unmount previously mounted volume */
    int (
        *umount)(struct BlockPlugin* plugin, struct BlockPlugin_Volume* volume);

    /* Submit volume I/O request */
    int (*submit_request)(
        struct BlockPlugin* plugin,
        struct BlockPlugin_Volume* volume,
        struct BlockPlugin_Request* request,
        struct BlockPlugin_Completion* completion);

    /* API 4.0 end */

    /* Get serialized monitoring dynamic counters data */
    int (*get_dynamic_counters)(
        struct BlockPlugin* plugin,
        BlockPlugin_GetCountersCallback callback,
        void* opaque);

    /* API 5.0 end */

    /*
     * Mount existing volume (async version)
     * It is ok to call this method for already
     * mounted volume e.g to change access mode
     */
    int (*mount_async)(
        struct BlockPlugin* plugin,
        struct BlockPlugin_MountOpts* opts,
        struct BlockPlugin_Volume* volume,
        struct BlockPlugin_Completion* completion);

    /* Unmount previously mounted volume (async version) */
    int (*umount_async)(
        struct BlockPlugin* plugin,
        struct BlockPlugin_Volume* volume,
        struct BlockPlugin_Completion* completion);

    /* API 5.1 end */

    /*
     * Mount existing volume (sync version)
     * It is ok to call this method for already
     * mounted volume e.g to change access mode
     */
    int (*mount_sync)(
        struct BlockPlugin* plugin,
        struct BlockPlugin_MountOpts* opts,
        struct BlockPlugin_Volume* volume);

    /* API 5.2 end */
};

/******************************************************************************/
/* Exported host interface */

/*
 * Layout of this struct is required to be consistent across versions
 * for compatibility
 * New members should be appended at the end and their presence must be detected
 * by checking BlockPluginHost version in plugin
 */
struct BlockPluginHost
{
    /* Magick and version should never change or move */
    uint32_t magic;
    uint32_t version_major;
    uint32_t version_minor;

    void* state;

    /*
     * All host callback may be called by plugin in a parallel thread
     */

    /* Called by plugin to complete previous I/O request */
    int (*complete_request)(
        struct BlockPluginHost* host,
        struct BlockPlugin_Completion* completion);

    /* Called by plugin to log error message */
    void (*log_message)(struct BlockPluginHost* host, const char* msg);

    /* API 2.0 end */

    // VM instance id
    const char* instance_id;

    /* API 5.4 end */
};

/******************************************************************************/

#define BLOCK_PLUGIN_GET_PLUGIN_SYMBOL_NAME "BlockPlugin_GetPlugin"
typedef struct BlockPlugin* (*BlockPlugin_GetPlugin_t)(
    struct BlockPluginHost* host,
    const char* options);

#define BLOCK_PLUGIN_PUT_PLUGIN_SYMBOL_NAME "BlockPlugin_PutPlugin"
typedef void (*BlockPlugin_PutPlugin_t)(struct BlockPlugin* plugin);

#define BLOCK_PLUGIN_GET_VERSION_SYMBOL_NAME "BlockPlugin_GetVersion"
typedef const char* (*BlockPlugin_GetVersion_t)(void);

/******************************************************************************/

#if defined(__cplusplus)
} /* extern "C" */
#endif
