#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <time.h>

int main(int argc, char *argv[]) {
    int N = 0;
    int opt;

    while ((opt = getopt(argc, argv, "n:")) != -1) {
        switch (opt) {
            case 'n':
                N = atoi(optarg);
                break;
            default:
                fprintf(stderr, "Usage: %s -n <number_of_files>\n", argv[0]);
                exit(EXIT_FAILURE);
        }
    }

    if (N <= 0) {
        fprintf(stderr, "Please specify a number of files using the -n option.\n");
        exit(EXIT_FAILURE);
    }

    char filename[100];
    int fds[100000];
    clock_t start_time, end_time;
    double open_time, close_time;

    for (int i = 0; i < N; i++) {
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
    start_time = clock();
    for (int i = 0; i < N; i++) {
        snprintf(filename, sizeof(filename), "file%d.txt", i + 1);
        fds[i] = open(filename, O_RDONLY);
    }
    end_time = clock();

    open_time = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    printf("Open %.6f ms\n", open_time * 1000);
    printf("Open avg %.6f ms\n", open_time * 1000 / N);

    // Measure closing latency
    start_time = clock();
    for (int i = 0; i < N; i++) {
        close(fds[i]);
    }
    end_time = clock();

    close_time = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    printf("Close %.6f ms\n", close_time * 1000);
    printf("Close avg %.6f ms\n", close_time * 1000 / N);

    return 0;
}
