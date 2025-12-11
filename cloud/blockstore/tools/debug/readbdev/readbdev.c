#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

const int blockSize = 4096;
const int rangeSize = 1024;

void dump(const bool* usedBlocks, int size)
{
    // TODO: rle and other stuff
    for (int i = 0; i < size; ++i) {
        printf("%d", usedBlocks[i]);
    }
}

int main(int argc, char* argv[])
{
    if (argc < 2) {
        printf("Error args\n");
        return 0;
    }

    int fp = open(argv[1], O_RDONLY | O_DIRECT);
    bool verbose = argc > 2;
    fprintf(stderr, "FP=%d\n", fp);
    if (fp <= 0) {
        perror("Error opening file");
        return 1;
    }

    int alignment = 512;
    char buf[blockSize + alignment];
    char* block = (char*)(alignment * ((intptr_t)buf / alignment) + alignment);
    int offset = 0;
    bool usedBlocks[rangeSize];
    int blockIdx = 0;
    int len;
    while (len = read(fp, block + offset, blockSize - offset)) {
        if (len == -1) {
            perror("Error reading file");
            return 2;
        }

        if (len == 0) {
            break;
        }

        if (verbose) {
            for (int i = offset; i < len; ++i) {
                fprintf(stderr, "%d", block[i]);
            }
        }

        offset += len;

        if (offset == blockSize) {
            usedBlocks[blockIdx] = false;
            for (int i = 0; i < blockSize; ++i) {
                if (block[i] != 0) {
                    usedBlocks[blockIdx] = true;
                    break;
                }
            }

            if (++blockIdx == rangeSize) {
                dump(usedBlocks, blockIdx);

                blockIdx = 0;
            }

            offset = 0;
        }
    }

    if (blockIdx) {
        dump(usedBlocks, blockIdx);
    }

    close(fp);

    return 0;
}
