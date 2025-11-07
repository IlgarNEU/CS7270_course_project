#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include "IPC.h"
#include "bloom.h"

#define MAX_KEYS 100000
#define MAX_PROCESSES 16
#define BUF_SIZE 256
#define BLOOM_MSG_SIZE 131072  // 128KB for large bloom filters
#define FALSE_POSITIVE_RATE 0.01

int process_id;
int num_processes;
int *keys = NULL;
int num_keys = 0;
int keys_capacity = 0;

BloomFilter own_bloom;
BloomFilter *peer_bloom_filters = NULL;
int bloom_initialized = 0;
int *peer_bloom_received = NULL;

int comm_fd = -1;

// Signal handler for cleanup
void signal_handler(int signum) {
    printf("\n[Process %d] Received signal %d, cleaning up...\n", process_id, signum);
    
    if (bloom_initialized) {
        bloom_filter_destroy(&own_bloom);
    }
    if (peer_bloom_filters != NULL) {
        for (int i = 0; i < num_processes; i++) {
            if (peer_bloom_received && peer_bloom_received[i]) {
                bloom_filter_destroy(&peer_bloom_filters[i]);
            }
        }
        free(peer_bloom_filters);
    }
    if (peer_bloom_received != NULL) {
        free(peer_bloom_received);
    }
    if (keys != NULL) {
        free(keys);
    }
    if (comm_fd >= 0) {
        close_communication(process_id, comm_fd);
    }
    
    exit(0);
}

int check_own_keys(int key) {
    // Binary search would be faster if keys are sorted
    for (int i = 0; i < num_keys; i++) {
        if (keys[i] == key) return 1;
    }
    return 0;
}

void assign_keys_from_message(const char *msg) {
    num_keys = 0;
    const char *ptr = msg + 5;
    
    // First pass: count keys
    char *copy = strdup(ptr);
    char *tok = strtok(copy, ",");
    int count = 0;
    while (tok != NULL) {
        count++;
        tok = strtok(NULL, ",");
    }
    free(copy);
    
    // Allocate memory for keys
    if (keys_capacity < count) {
        keys = realloc(keys, count * sizeof(int));
        keys_capacity = count;
    }
    
    // Second pass: store keys
    copy = strdup(ptr);
    tok = strtok(copy, ",");
    while (tok != NULL && num_keys < keys_capacity) {
        keys[num_keys++] = atoi(tok);
        tok = strtok(NULL, ",");
    }
    free(copy);
    
    printf("[Process %d] Assigned %d keys\n", process_id, num_keys);
    
    create_own_bloom_filter();
}

void create_own_bloom_filter() {
    if (bloom_initialized) {
        bloom_filter_destroy(&own_bloom);
    }
    
    // Initialize bloom filter
    bloom_filter_init(&own_bloom, num_keys > 0 ? num_keys : 10, FALSE_POSITIVE_RATE);
    
    // Add all keys to bloom filter
    for (int i = 0; i < num_keys; i++) {
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "%d", keys[i]);
        bloom_filter_add_string(&own_bloom, key_str);
    }
    
    bloom_initialized = 1;
    printf("[Process %d] Created bloom filter with %d keys\n", process_id, num_keys);
    
    broadcast_bloom_filter();
}

void broadcast_bloom_filter() {
    char *hex_string = bloom_filter_export_hex_string(&own_bloom);
    if (hex_string == NULL) {
        fprintf(stderr, "[Process %d] Failed to export bloom filter\n", process_id);
        return;
    }
    
    size_t hex_len = strlen(hex_string);
    
    // Check if message will fit
    if (hex_len + 20 > 65000) {
        fprintf(stderr, "[Process %d] Bloom filter too large: %zu bytes\n", process_id, hex_len);
        free(hex_string);
        return;
    }
    
    size_t msg_len = 7 + 10 + 1 + hex_len + 1;
    char *msg = malloc(msg_len);
    if (msg == NULL) {
        fprintf(stderr, "[Process %d] Failed to allocate message buffer\n", process_id);
        free(hex_string);
        return;
    }
    
    snprintf(msg, msg_len, "BLOOM:%d:%s", process_id, hex_string);
    
    // Send to all other processes
    for (int p = 0; p < num_processes; p++) {
        if (p == process_id) continue;
        send_msg(process_id, p, msg);
    }
    
    printf("[Process %d] Broadcasted bloom filter (%zu bytes)\n", process_id, hex_len);
    
    free(hex_string);
    free(msg);
}

void update_peer_bloom_filter(int peer_id, const char *bloom_data) {
    printf("[Process %d] Received bloom filter from process %d\n", process_id, peer_id);
    
    // Initialize peer bloom filter array if needed
    if (peer_bloom_filters == NULL) {
        peer_bloom_filters = calloc(num_processes, sizeof(BloomFilter));
        peer_bloom_received = calloc(num_processes, sizeof(int));
    }
    
    // Destroy existing bloom filter if present
    if (peer_bloom_received[peer_id]) {
        bloom_filter_destroy(&peer_bloom_filters[peer_id]);
    }
    
    // Import the bloom filter from hex string
    int result = bloom_filter_import_hex_string(&peer_bloom_filters[peer_id], (char*)bloom_data);
    
    if (result == BLOOM_SUCCESS) {
        peer_bloom_received[peer_id] = 1;
        printf("[Process %d] Successfully imported bloom filter from process %d\n", 
               process_id, peer_id);
    } else {
        fprintf(stderr, "[Process %d] Failed to import bloom filter from process %d\n", 
                process_id, peer_id);
    }
}

void handle_query_from_manager(const char *msg) {
    int key = atoi(msg + 6);
    printf("[Process %d] Query for key %d from manager\n", process_id, key);
    
    // Check own keys first
    if (check_own_keys(key)) {
        printf("[Process %d] Key %d found locally\n", process_id, key);
        return;
    }
    
    // Check peer bloom filters
    char key_str[32];
    snprintf(key_str, sizeof(key_str), "%d", key);
    
    for (int p = 0; p < num_processes; p++) {
        if (p == process_id) continue;
        
        if (peer_bloom_received != NULL && peer_bloom_received[p] &&
            bloom_filter_check_string(&peer_bloom_filters[p], key_str) != BLOOM_FAILURE) {
            printf("[Process %d] Key %d might be in process %d, querying...\n",
                   process_id, key, p);
            char buf[BUF_SIZE];
            snprintf(buf, sizeof(buf), "PQUERY:%d", key);
            send_msg(process_id, p, buf);
        }
    }
}

void handle_bloom_message(const char *msg) {
    const char *first_colon = strchr(msg + 6, ':');
    if (first_colon == NULL) {
        fprintf(stderr, "[Process %d] Invalid BLOOM message format\n", process_id);
        return;
    }
    
    int peer_id = atoi(msg + 6);
    const char *hex_data = first_colon + 1;
    
    update_peer_bloom_filter(peer_id, hex_data);
}

void handle_query_from_process(const char *msg) {
    // Parse "PQUERY:<key>"
    if (strncmp(msg, "PQUERY:", 7) != 0) {
        return;
    }
    
    int key = atoi(msg + 7);
    
    // Check actual keys (no need to check own bloom filter)
    if (check_own_keys(key)) {
        printf("[Process %d] Key %d found (responding to peer query)\n", process_id, key);
        // In a real system, you'd send a response back
    } else {
        printf("[Process %d] Key %d not found (bloom false positive)\n", process_id, key);
    }
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <process_id> <num_processes>\n", argv[0]);
        return 1;
    }
    
    process_id = atoi(argv[1]);
    num_processes = atoi(argv[2]);
    
    // Set up signal handlers for cleanup
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    comm_fd = initiate_communication(process_id);
    
    printf("[Process %d] Started, waiting for key assignment\n", process_id);
    
    char *buf = malloc(BLOOM_MSG_SIZE);
    if (buf == NULL) {
        fprintf(stderr, "[Process %d] Failed to allocate receive buffer\n", process_id);
        return 1;
    }
    
    while (1) {
        int messages_processed = 0;
        
        // Drain all available messages before sleeping
        while (1) {
            int n = receive_msg(comm_fd, buf, BLOOM_MSG_SIZE);
            if (n <= 0) break;  // No more messages
            
            messages_processed++;
            
            // Process message based on type
            if (strncmp(buf, "KEYS:", 5) == 0) {
                assign_keys_from_message(buf);
            } else if (strncmp(buf, "QUERY:", 6) == 0) {
                handle_query_from_manager(buf);
            } else if (strncmp(buf, "BLOOM:", 6) == 0) {
                handle_bloom_message(buf);
            } else if (strncmp(buf, "PQUERY:", 7) == 0) {
                handle_query_from_process(buf);
            } else {
                fprintf(stderr, "[Process %d] Unknown message: %s\n", process_id, buf);
            }
        }
        
        // Only sleep if no messages were processed
        if (messages_processed == 0) {
            usleep(1000);  // 1ms sleep when idle
        }
    }
    
    // Cleanup (won't reach here unless loop breaks)
    free(buf);
    signal_handler(0);
    
    return 0;
}