#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>

int main(int argc, char **argv)
{
    if (argc != 5) {
        fprintf(stderr,
                "usage: %s <BDF> <BAR number> <BAR size> <DeviceCfg offset>\n",
                argv[0]);
        return 1;
    }

    const char *bdf = argv[1];

    char *end;
    errno = 0;

    unsigned long bar = strtoul(argv[2], &end, 0);
    if (errno || *end != '\0') {
        fprintf(stderr, "invalid BAR number: %s\n", argv[2]);
        return 1;
    }

    unsigned long bar_size = strtoul(argv[3], &end, 0);
    if (errno || *end != '\0') {
        fprintf(stderr, "invalid BAR size: %s\n", argv[3]);
        return 1;
    }

    unsigned long offset = strtoul(argv[4], &end, 0);
    if (errno || *end != '\0') {
        fprintf(stderr, "invalid offset: %s\n", argv[4]);
        return 1;
    }

    if (offset + 36 > bar_size) {
        fprintf(stderr,
                "tag range [0x%lx, 0x%lx) exceeds BAR size 0x%lx\n",
                offset, offset + 36, bar_size);
        return 1;
    }

    char path[256];
    snprintf(path, sizeof(path),
             "/sys/bus/pci/devices/%s/resource%lu", bdf, bar);

    int fd = open(path, O_RDONLY | O_SYNC);
    if (fd < 0) {
        perror("open");
        return 1;
    }

    void *p = mmap(NULL, bar_size, PROT_READ, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) {
        perror("mmap");
        close(fd);
        return 1;
    }

    volatile uint8_t *cfg = (volatile uint8_t *)p + offset;

    for (int i = 0; i < 36; ++i) {
        unsigned char c = cfg[i];
        if (!c)
            break;
        putchar(c);
    }

    putchar('\n');

    munmap(p, bar_size);
    close(fd);
    return 0;
}
