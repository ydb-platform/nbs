#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define NUM_TEST_POSITIONS 5
#define MIN_DIRECTORIES 10000
#define MAX_FILENAME_LEN 256
#define SLEEP_INTERVAL_MS 100
#define MS_TO_US 1000

static const double TEST_PERCENTAGES[NUM_TEST_POSITIONS] =
    {0.40, 0.80, 0.96, 0.98, 0.996};

typedef struct
{
    long position;
    char name[MAX_FILENAME_LEN];
} dir_entry_t;

typedef struct
{
    const char* directory_path;
    const char* wait_file_path;
    int timeout_ms;
    int num_directories;
} test_config_t;

typedef struct
{
    DIR* dir1;
    DIR* dir2;
    dir_entry_t* entries;
    int entry_count;
    int test_positions[NUM_TEST_POSITIONS];
} test_state_t;

static void string_copy(char* dest, const char* src, size_t dest_size)
{
    strncpy(dest, src, dest_size - 1);
    dest[dest_size - 1] = '\0';
}

static bool parse_arguments(int argc, char** argv, test_config_t* config)
{
    if (argc != 5) {
        fprintf(
            stderr,
            "Usage: %s <directory> <wait_file> <timeout_ms> "
            "<num_directories>\n",
            argv[0]);
        return false;
    }

    config->directory_path = argv[1];
    config->wait_file_path = argv[2];
    config->timeout_ms = atoi(argv[3]);
    config->num_directories = atoi(argv[4]);

    if (config->num_directories < MIN_DIRECTORIES) {
        fprintf(
            stderr,
            "Error: Number of directories (%d) must be at least %d\n",
            config->num_directories,
            MIN_DIRECTORIES);
        return false;
    }

    if (config->timeout_ms <= 0) {
        fprintf(stderr, "Error: Timeout must be positive\n");
        return false;
    }

    return true;
}

static void calculate_test_positions(int num_directories, int* positions)
{
    for (int i = 0; i < NUM_TEST_POSITIONS; i++) {
        positions[i] = (int)(num_directories * TEST_PERCENTAGES[i]);
    }
}

static bool init_test_state(test_state_t* state, const test_config_t* config)
{
    memset(state, 0, sizeof(*state));

    state->dir1 = opendir(config->directory_path);
    state->dir2 = opendir(config->directory_path);

    if (!state->dir1 || !state->dir2) {
        perror("opendir");
        return false;
    }

    state->entries = calloc(config->num_directories, sizeof(dir_entry_t));
    if (!state->entries) {
        fprintf(stderr, "Failed to allocate memory for directory entries\n");
        return false;
    }

    calculate_test_positions(config->num_directories, state->test_positions);

    return true;
}

static void cleanup_test_state(test_state_t* state)
{
    if (state->dir1) {
        closedir(state->dir1);
        state->dir1 = NULL;
    }
    if (state->dir2) {
        closedir(state->dir2);
        state->dir2 = NULL;
    }
    if (state->entries) {
        free(state->entries);
        state->entries = NULL;
    }
}

static bool cache_directory_entries(
    test_state_t* state,
    const test_config_t* config)
{
    printf(
        "\n=== Stage 1: Read directory entries to populate FUSE cache ===\n");

    struct dirent* entry;
    state->entry_count = 0;

    while (state->entry_count < config->num_directories) {
        long pos = telldir(state->dir1);
        entry = readdir(state->dir1);

        if (!entry) {
            break;
        }

        if (entry->d_name[0] != '.') {
            state->entries[state->entry_count].position = pos;
            string_copy(
                state->entries[state->entry_count].name,
                entry->d_name,
                MAX_FILENAME_LEN);
            state->entry_count++;
        }
    }

    printf("Total entries cached: %d\n", state->entry_count);
    return state->entry_count > 0;
}

static bool wait_for_file(const char* filepath, int timeout_ms)
{
    printf(
        "Waiting for file '%s' to be created after filehost restart...\n",
        filepath);
    fflush(stdout);

    struct stat wait_st;
    while (stat(filepath, &wait_st) == -1 && timeout_ms > 0) {
        usleep(SLEEP_INTERVAL_MS * MS_TO_US);
        timeout_ms -= SLEEP_INTERVAL_MS;
    }

    if (timeout_ms <= 0) {
        printf("File '%s' not detected, continuing...\n", filepath);
        return false;
    }

    printf(
        "File '%s' detected, restart was successful, continuing...\n",
        filepath);
    fflush(stdout);
    return true;
}

static bool perform_seek_tests(test_state_t* state, const test_config_t* config)
{
    printf(
        "\n=== Stage 2: Use seekdir and readdir with interruption to trigger "
        "offset-based reads ===\n");

    printf("Test positions: ");
    for (int i = 0; i < NUM_TEST_POSITIONS; i++) {
        printf("%d", state->test_positions[i]);
        if (i < NUM_TEST_POSITIONS - 1) {
            printf(", ");
        }
    }
    printf("\n");

    int timeout_remaining = config->timeout_ms;

    for (int i = 0; i < NUM_TEST_POSITIONS; i++) {
        int target_idx = state->test_positions[i];
        if (target_idx >= state->entry_count) {
            continue;
        }

        printf(
            "Seeking to position %ld (entry ~%d: %s)\n",
            state->entries[target_idx].position,
            target_idx,
            state->entries[target_idx].name);

        seekdir(state->dir1, state->entries[target_idx].position);

        if (i == 1) {
            if (!wait_for_file(config->wait_file_path, timeout_remaining)) {
                return false;
            }
        }

        struct dirent* entry = readdir(state->dir1);
        if (entry) {
            printf("  Read from dir1 offset: %s\n", entry->d_name);
            fflush(stdout);
        }

        seekdir(state->dir2, state->entries[target_idx].position);
        entry = readdir(state->dir2);
        if (entry) {
            printf("  Read from dir2 offset: %s\n", entry->d_name);
        }

        printf(
            "  entry_names[%d]   : %s\n",
            target_idx,
            state->entries[target_idx].name);
        fflush(stdout);
    }

    return true;
}

int main(int argc, char** argv)
{
    test_config_t config;
    test_state_t state;

    if (!parse_arguments(argc, argv, &config)) {
        return EXIT_FAILURE;
    }

    printf("Using %d directories\n", config.num_directories);

    if (!init_test_state(&state, &config)) {
        cleanup_test_state(&state);
        return EXIT_FAILURE;
    }

    if (!cache_directory_entries(&state, &config)) {
        fprintf(stderr, "Failed to cache directory entries\n");
        cleanup_test_state(&state);
        return EXIT_FAILURE;
    }

    if (!perform_seek_tests(&state, &config)) {
        fprintf(stderr, "Seek tests failed\n");
        cleanup_test_state(&state);
        return EXIT_FAILURE;
    }

    printf("Directory reading test completed with success\n");
    fflush(stdout);

    return EXIT_SUCCESS;
}
