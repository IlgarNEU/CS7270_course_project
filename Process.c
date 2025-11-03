#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "IPC.h"

#define MAX_KEYS 100
#define MAX_PROCESSES 4
#define BUF_SIZE 256


typedef struct {
    unsigned char bits[256]; // 2048 bits
} BloomFilter;

void bloom_set(BloomFilter *bf, int key) {
    bf->bits[key % 2048 / 8] |= 1 << (key % 8);
}

int bloom_check(BloomFilter *bf, int key) {
    return bf->bits[key % 2048 / 8] & (1 << (key % 8));
}


int process_id;
int num_processes;
int keys[MAX_KEYS];
int num_keys = 0;
BloomFilter bloom_filters[MAX_PROCESSES]; // bloom filters of other processes

// =====================
// Helpers
// =====================
int check_own_keys(int key) {
    for (int i = 0; i < num_keys; i++) {
        if (keys[i] == key) return 1;
    }
    return 0;
}


void assign_keys_from_message(const char *msg) {
    num_keys = 0;
    const char *ptr = msg + 5;
    char *copy = strdup(ptr);
    char *tok = strtok(copy, ",");
    while (tok != NULL && num_keys < MAX_KEYS) {
        keys[num_keys++] = atoi(tok);
        tok = strtok(NULL, ",");
    }
    free(copy);
    printf("[Process %d] Assigned keys:", process_id);
    for (int i = 0; i < num_keys; i++) printf(" %d", keys[i]);
    printf("\n");
}


void handle_query_from_manager(int fd, const char *msg) {
    int key = atoi(msg + 6); 
    printf("[Process %d] Received query for key %d from manager\n", process_id, key);

    if (check_own_keys(key)) {
        printf("[Process %d] Key %d found locally\n", process_id, key);
    } else {
        // check bloom filters of other processes
        for (int p = 0; p < num_processes; p++) {
            if (p == process_id) continue;
            if (bloom_check(&bloom_filters[p], key)) {
                printf("[Process %d] Key %d might be in process %d, querying...\n",
                       process_id, key, p);
                char buf[BUF_SIZE];
                snprintf(buf, sizeof(buf), "%d", key);
                send_msg(process_id, p, buf);
            }
        }
    }
}

// Handle queries from other processes
void handle_query_from_process(int fd, const char *msg) {
    int key = atoi(msg);
    if (check_own_keys(key)) {
        printf("[Process %d] Responding to process query, key %d found\n", process_id, key);
    } else {
        printf("[Process %d] Received query from process, key %d not found\n", process_id, key);
    }
}

// =====================
// Main loop
// =====================
int main(int argc, char *argv[]) {
    if (argc < 3) {
        printf("Usage: %s <process_id> <num_processes>\n", argv[0]);
        return 1;
    }

    process_id = atoi(argv[1]);
    num_processes = atoi(argv[2]);

    int fd = initiate_communication(process_id);

    printf("[Process %d] Started, waiting for key assignment from manager\n", process_id);

    char buf[BUF_SIZE];
    while (1) {
        int n = receive_msg(fd, buf, sizeof(buf));
        if (n > 0) {
            if (strncmp(buf, "KEYS:", 5) == 0) {
                assign_keys_from_message(buf);
            } else if (strncmp(buf, "QUERY:", 6) == 0) {
                handle_query_from_manager(fd, buf);
            } else {
                handle_query_from_process(fd, buf);
            }
        }
        usleep(100000); // 0.1s
    }

    close_communication(process_id, fd);
    return 0;
}
