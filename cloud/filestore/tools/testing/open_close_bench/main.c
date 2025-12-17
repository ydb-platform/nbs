#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

int main(int argc, char *argv[]) {
    int n = 0;
    int opt;

    while ((opt = getopt(argc, argv, "n:")) != -1) {
        switch (opt) {
            case 'n':
                n = atoi(optarg);
                break;
            default:
                fprintf(stderr, "Usage: %s -n <number_of_files>\n", argv[0]);
                exit(EXIT_FAILURE);
        }
    }

    if (n <= 0) {
        fprintf(stderr, "Please specify a number of files using the -n option.\n");
        exit(EXIT_FAILURE);
    }

    char filename[100];
    int fds[100000];
    struct timespec startTime, endTime;
    double openTime, closeTime;

    for (int i = 0; i < n; i++) {
        // Generate filename
        snprintf(filename, sizeof(filename), "file%d.txt", i + 1);

        // Try to open the file in read-only mode to check if it exists
        int fd = open(filename, O_RDONLY);
        if (fd == -1) {
            // File doesn't exist, create it
            fd = open(filename, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
            if (fd == -1) {
                perror("Error creating file");
                exit(1);
            }
            printf("File %s created.\n", filename);
            close(fd);
        } else {
            // File already exists, close it
            close(fd);
        }
    }

    // Measure opening latency
    clock_gettime(CLOCK_MONOTONIC, &startTime);
    for (int i = 0; i < n; i++) {
        snprintf(filename, sizeof(filename), "file%d.txt", i + 1);
        fds[i] = open(filename, O_RDONLY);
    }
    clock_gettime(CLOCK_MONOTONIC, &endTime);

    openTime = (endTime.tv_sec - startTime.tv_sec) +
               (endTime.tv_nsec - startTime.tv_nsec) / 1e9;
    printf("Open %.6f ms\n", openTime * 1000);
    printf("Open avg %.6f ms\n", openTime * 1000 / n);

    // Measure closing latency
    clock_gettime(CLOCK_MONOTONIC, &startTime);
    for (int i = 0; i < n; i++) {
        close(fds[i]);
    }
    clock_gettime(CLOCK_MONOTONIC, &endTime);

    closeTime = (endTime.tv_sec - startTime.tv_sec) +
                (endTime.tv_nsec - startTime.tv_nsec) / 1e9;
    printf("Close %.6f ms\n", closeTime * 1000);
    printf("Close avg %.6f ms\n", closeTime * 1000 / n);

    return 0;
}
