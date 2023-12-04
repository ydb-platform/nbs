#include <string.h>
#include <inttypes.h>
#include <sys/stat.h>

#include "queue.h"
#include "memmap.h"
#include "platform.h"
#include "logging.h"
#include "objref.h"

struct vhd_mmap_callbacks {
    /* gets called after mapping guest memory region */
    int (*map_cb)(void *addr, size_t len);
    /* gets called before unmapping guest memory region */
    int (*unmap_cb)(void *addr, size_t len);
};

struct vhd_memory_region {
    struct objref ref;

    /* start of the region in guest physical space */
    uint64_t gpa;
    /* start of the region in master's virtual space */
    uint64_t uva;
    /* start of the region in this process' virtual space */
    void *ptr;
    /* region size */
    size_t size;
    /* offset of the region from the file base */
    off_t offset;

    /* unique identifiers of this region for caching purposes */
    dev_t device;
    ino_t inode;

    /* callbacks associated with this memory region */
    struct vhd_mmap_callbacks callbacks;

    LIST_ENTRY(vhd_memory_region) region_link;
};

static LIST_HEAD(, vhd_memory_region) g_regions =
    LIST_HEAD_INITIALIZER(g_regions);

static void region_init_id(struct vhd_memory_region *reg, int fd)
{
    struct stat stat;

    if (fstat(fd, &stat) < 0) {
        return;
    }

    reg->device = stat.st_dev;
    reg->inode = stat.st_ino;
}

/*
 * This should be no less than VHOST_USER_MEM_REGIONS_MAX, to accept any
 * allowed VHOST_USER_SET_MEM_TABLE message.  The master may use more via
 * VHOST_USER_ADD_MEM_REG message if VHOST_USER_PROTOCOL_F_CONFIGURE_MEM_SLOTS
 * is negotiated.
 */
#define VHD_RAM_SLOTS_MAX 32

struct vhd_memory_map {
    struct objref ref;

    struct vhd_mmap_callbacks callbacks;

    /* actual number of slots used */
    unsigned num;
    struct vhd_memory_region *regions[VHD_RAM_SLOTS_MAX];
};

/*
 * Returns actual pointer where uva points to
 * or NULL in case of mapping absence
 */
void *uva_to_ptr(struct vhd_memory_map *mm, uint64_t uva)
{
    unsigned i;

    for (i = 0; i < mm->num; i++) {
        struct vhd_memory_region *reg = mm->regions[i];
        if (uva >= reg->uva && uva - reg->uva < reg->size) {
            return reg->ptr + (uva - reg->uva);
        }
    }

    return NULL;
}

static void *map_memory(void *addr, size_t len, int fd, off_t offset)
{
    size_t aligned_len = VHD_ALIGN_PTR_UP(len, HUGE_PAGE_SIZE);
    size_t map_len = aligned_len + HUGE_PAGE_SIZE + PAGE_SIZE;

    char *map = mmap(addr, map_len, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1,
                     0);
    if (map == MAP_FAILED) {
        VHD_LOG_ERROR("unable to map memory: %s", strerror(errno));
        return MAP_FAILED;
    }

    char *aligned_addr = VHD_ALIGN_PTR_UP(map + PAGE_SIZE, HUGE_PAGE_SIZE);
    addr = mmap(aligned_addr, len, PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_FIXED, fd, offset);
    if (addr == MAP_FAILED) {
        VHD_LOG_ERROR("unable to remap memory region %p-%p: %s", aligned_addr,
                      aligned_addr + len, strerror(errno));
        munmap(map, map_len);
        return MAP_FAILED;
    }
    aligned_addr = addr;

    size_t tail_len = aligned_len - len;
    if (tail_len) {
        char *tail = aligned_addr + len;
        addr = mmap(tail, tail_len, PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_ANONYMOUS | MAP_FIXED, -1, 0);
        if (addr == MAP_FAILED) {
            VHD_LOG_ERROR("unable to remap memory region %p-%p: %s", tail,
                          tail + tail_len, strerror(errno));
            munmap(map, map_len);
            return MAP_FAILED;
        }
    }

    char *start = aligned_addr - PAGE_SIZE;
    char *end = aligned_addr + aligned_len + PAGE_SIZE;
    munmap(map, start - map);
    munmap(end, map + map_len - end);

    return aligned_addr;
}

static int unmap_memory(void *addr, size_t len)
{
    size_t map_len = VHD_ALIGN_PTR_UP(len, HUGE_PAGE_SIZE) + PAGE_SIZE * 2;
    char *map = addr - PAGE_SIZE;
    return munmap(map, map_len);
}

static int map_region(struct vhd_memory_region *region, uint64_t gpa,
                      uint64_t uva, size_t size, int fd, off_t offset)
{
    void *ptr;

    ptr = map_memory(NULL, size, fd, offset);
    if (ptr == MAP_FAILED) {
        int ret = -errno;
        VHD_LOG_ERROR("can't mmap memory: %s", strerror(-ret));
        return ret;
    }

    if (region->callbacks.map_cb) {
        size_t len = VHD_ALIGN_PTR_UP(size, HUGE_PAGE_SIZE);
        int ret = region->callbacks.map_cb(ptr, len);
        if (ret < 0) {
            VHD_LOG_ERROR("map callback failed for region %p-%p: %s",
                          ptr, ptr + len, strerror(-ret));
            munmap(ptr, size);
            return ret;
        }
    }

    /* Mark memory as defined explicitly */
    VHD_MEMCHECK_DEFINED(ptr, size);

    region->ptr = ptr;
    region->gpa = gpa;
    region->uva = uva;
    region->size = size;
    region->offset = offset;
    region_init_id(region, fd);

    return 0;
}

static int unmap_region(struct vhd_memory_region *reg)
{
    int ret;

    if (reg->callbacks.unmap_cb) {
        size_t len = VHD_ALIGN_PTR_UP(reg->size, HUGE_PAGE_SIZE);
        ret = reg->callbacks.unmap_cb(reg->ptr, len);
        if (ret < 0) {
            VHD_LOG_ERROR("unmap callback failed for region %p-%p: %s",
                          reg->ptr, reg->ptr + reg->size, strerror(-ret));
            return ret;
        }
    }

    ret = unmap_memory(reg->ptr, reg->size);
    if (ret < 0) {
        VHD_LOG_ERROR("failed to unmap region at %p", reg->ptr);
        return ret;
    }

    return 0;
}

static void region_release(struct objref *objref)
{
    struct vhd_memory_region *reg =
            containerof(objref, struct vhd_memory_region, ref);

    LIST_REMOVE(reg, region_link);
    unmap_region(reg);
    vhd_free(reg);
}

static void region_ref(struct vhd_memory_region *reg)
{
    objref_get(&reg->ref);
}

static void region_unref(struct vhd_memory_region *reg)
{
    objref_put(&reg->ref);
}

static inline struct vhd_memory_region *region_get_cached(
    uint64_t gpa, uint64_t uva,
    size_t size, int fd,
    off_t offset,
    struct vhd_mmap_callbacks *callbacks
)
{
    struct vhd_memory_region *region;
    struct stat stat;

    if (fstat(fd, &stat) < 0) {
        return NULL;
    }

    LIST_FOREACH(region, &g_regions, region_link) {
        if (region->inode != stat.st_ino || region->device != stat.st_dev) {
            continue;
        }
        if (region->gpa != gpa || region->uva != uva ||
            region->size != size || region->offset != offset) {
            continue;
        }
        if (region->callbacks.map_cb != callbacks->map_cb ||
            region->callbacks.unmap_cb != callbacks->unmap_cb) {
            continue;
        }

        region_ref(region);
        return region;
    }

    return NULL;
}

static void memmap_release(struct objref *objref)
{
    struct vhd_memory_map *mm =
        containerof(objref, struct vhd_memory_map, ref);
    unsigned i;

    for (i = 0; i < mm->num; i++) {
        region_unref(mm->regions[i]);
    }

    vhd_free(mm);
}

void vhd_memmap_ref(struct vhd_memory_map *mm) __attribute__ ((weak));
void vhd_memmap_ref(struct vhd_memory_map *mm)
{
    objref_get(&mm->ref);
}

void vhd_memmap_unref(struct vhd_memory_map *mm) __attribute__ ((weak));
void vhd_memmap_unref(struct vhd_memory_map *mm)
{
    objref_put(&mm->ref);
}

uint64_t ptr_to_gpa(struct vhd_memory_map *mm, void *ptr)
{
    unsigned i;
    for (i = 0; i < mm->num; ++i) {
        struct vhd_memory_region *reg = mm->regions[i];
        if (ptr >= reg->ptr && ptr < reg->ptr + reg->size) {
            return (ptr - reg->ptr) + reg->gpa;
        }
    }

    VHD_LOG_WARN("Failed to translate ptr %p to gpa", ptr);
    return TRANSLATION_FAILED;
}

void *gpa_range_to_ptr(struct vhd_memory_map *mm,
                       uint64_t gpa, size_t len) __attribute__ ((weak));
void *gpa_range_to_ptr(struct vhd_memory_map *mm, uint64_t gpa, size_t len)
{
    unsigned i;

    for (i = 0; i < mm->num; i++) {
        struct vhd_memory_region *reg = mm->regions[i];
        if (gpa >= reg->gpa && gpa - reg->gpa < reg->size) {
            /*
             * Check (overflow-safe) that length fits in a single region.
             *
             * TODO: should we handle gpa areas that cross region boundaries
             *       but are otherwise valid?
             */
            if (len > reg->size || gpa - reg->gpa + len > reg->size) {
                return NULL;
            }

            return reg->ptr + (gpa - reg->gpa);
        }
    }

    return NULL;
}

struct vhd_memory_map *vhd_memmap_new(int (*map_cb)(void *, size_t),
                                      int (*unmap_cb)(void *, size_t))
{
    struct vhd_memory_map *mm = vhd_alloc(sizeof(*mm));
    *mm = (struct vhd_memory_map) {
        .callbacks = (struct vhd_mmap_callbacks) {
            .map_cb = map_cb,
            .unmap_cb = unmap_cb,
        }
    };

    objref_init(&mm->ref, memmap_release);
    return mm;
}

int vhd_memmap_add_slot(struct vhd_memory_map *mm, uint64_t gpa, uint64_t uva,
                        size_t size, int fd, off_t offset)
{
    int ret;
    unsigned i;
    struct vhd_memory_region *region;

    /* check for overflow */
    if (gpa + size < gpa || uva + size < uva) {
        return -EINVAL;
    }
    /* check for spare slots */
    if (mm->num == VHD_RAM_SLOTS_MAX) {
        return -ENOBUFS;
    }
    /* check for intersection with existing slots */
    for (i = 0; i < mm->num; i++) {
        struct vhd_memory_region *reg = mm->regions[i];
        if (reg->gpa + reg->size <= gpa || gpa + size <= reg->gpa ||
            reg->uva + reg->size <= uva || uva + size <= reg->uva) {
            continue;
        }
        return -EINVAL;
    }

    /* find appropriate position to keep ascending order in gpa */
    for (i = mm->num; i > 0; i--) {
        struct vhd_memory_region *reg = mm->regions[i - 1];
        if (reg->gpa < gpa) {
            break;
        }
    }

    region = region_get_cached(gpa, uva, size, fd, offset, &mm->callbacks);
    if (region == NULL) {
        region = vhd_calloc(1, sizeof(*region));
        *region = (struct vhd_memory_region) {
            .callbacks = mm->callbacks,
        };

        objref_init(&region->ref, region_release);

        ret = map_region(region, gpa, uva, size, fd, offset);
        if (ret < 0) {
            vhd_free(region);
            return ret;
        }

        LIST_INSERT_HEAD(&g_regions, region, region_link);
    } else {
        VHD_LOG_INFO(
            "region %jd-%ju (GPA 0x%016"PRIX64" -> 0x%016"PRIX64") cache hit, "
            "reusing (%u refs total)", region->device, region->inode,
            region->gpa, region->gpa + region->size, objref_read(&region->ref)
        );
    }

    if (i < mm->num) {
        memmove(&mm->regions[i + 1], &mm->regions[i],
                sizeof(mm->regions[0]) * (mm->num - i));
    }
    mm->regions[i] = region;
    mm->num++;

    return 0;
}

int vhd_memmap_del_slot(struct vhd_memory_map *mm, uint64_t gpa, uint64_t uva,
                        size_t size)
{
    unsigned i;

    for (i = 0; i < mm->num; i++) {
        struct vhd_memory_region *reg = mm->regions[i];
        if (reg->gpa == gpa && reg->uva == uva && reg->size == size) {
            break;
        }
    }

    if (i == mm->num) {
        return -ENXIO;
    }

    region_unref(mm->regions[i]);

    mm->num--;
    if (i < mm->num) {
        memmove(&mm->regions[i], &mm->regions[i + 1],
                sizeof(mm->regions[0]) * (mm->num - i));
    }

    return 0;
}
