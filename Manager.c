// manager.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <signal.h>
#include <time.h>
#include "IPC.h"

#define MAX_MSG_LEN 256
#define BLOOM_EXCHANGE_TIME 3

// Configuration
int num_processes = 4;
int keys_per_process = 10;

// Global variables
int *all_keys;
int total_keys;
pid_t *process_pids;
int manager_fd;

void create_processes() {
    process_pids = malloc(num_processes * sizeof(pid_t));
    
    for (int i = 0; i < num_processes; i++) {
        pid_t pid = fork();
        
        if (pid == 0) {
            // Child process
            char process_id_str[10];
            char num_proc_str[10];
            snprintf(process_id_str, sizeof(process_id_str), "%d", i);
            snprintf(num_proc_str, sizeof(num_proc_str), "%d", num_processes);
            
            execl("./process", "process", process_id_str, num_proc_str, NULL);
            perror("execl failed");
            exit(1);
        } else if (pid > 0) {
            // Parent process
            process_pids[i] = pid;
            printf("[Manager] Created process %d with PID %d\n", i, pid);
        } else {
            perror("fork failed");
            exit(1);
        }
    }
    
    // Give processes time to initialize sockets
    sleep(1);
}

void create_random_keys() {
    total_keys = num_processes * keys_per_process;
    all_keys = malloc(total_keys * sizeof(int));
    
    srand(time(NULL));
    
    for (int i = 0; i < total_keys; i++) {
        all_keys[i] = rand() % 1000;
    }
    
    printf("[Manager] Created %d random keys\n", total_keys);
}

void assign_random_keys() {
    for (int p = 0; p < num_processes; p++) {
        char msg[MAX_MSG_LEN];
        int msg_pos = sprintf(msg, "KEYS:");
        
        int start_idx = p * keys_per_process;
        
        // Build message with all keys for this process
        for (int k = 0; k < keys_per_process; k++) {
            char key_str[20];
            int key_str_len;
            
            if (k == 0) {
                key_str_len = snprintf(key_str, sizeof(key_str), "%d", 
                                      all_keys[start_idx + k]);
            } else {
                key_str_len = snprintf(key_str, sizeof(key_str), ",%d", 
                                      all_keys[start_idx + k]);
            }
            
            // Check if message would overflow
            if (msg_pos + key_str_len >= MAX_MSG_LEN - 1) {
                fprintf(stderr, "[Manager] ERROR: Too many keys for single message to process %d\n", p);
                fprintf(stderr, "[Manager] Reduce keys_per_process or increase MAX_MSG_LEN\n");
                exit(1);
            }
            
            // Append key
            strcpy(msg + msg_pos, key_str);
            msg_pos += key_str_len;
        }
        
        // Send single message with all keys
        send_msg(num_processes, p, msg);
        printf("[Manager] Sent %d keys to process %d\n", keys_per_process, p);
    }
    
    // Wait for bloom filter creation and exchange
    printf("[Manager] Waiting %d seconds for bloom filter exchange...\n", 
           BLOOM_EXCHANGE_TIME);
    sleep(BLOOM_EXCHANGE_TIME);
}

int main() {
    printf("[Manager] Starting with %d processes and %d keys per process\n", 
           num_processes, keys_per_process);
    
    // Initialize manager's communication
    manager_fd = initiate_communication(num_processes);
    
    // Step 1: Create processes
    create_processes();
    
    // Step 2: Create random keys
    create_random_keys();
    
    // Step 3: Assign keys to processes
    assign_random_keys();
    
    // Step 4: Send queries
    printf("[Manager] Starting query phase...\n");
    
    int num_queries = 20;
    for (int i = 0; i < num_queries; i++) {
        // Pick random process and key
        int target_process = rand() % num_processes;
        int query_key = all_keys[rand() % total_keys];
        
        char query_msg[MAX_MSG_LEN];
        snprintf(query_msg, sizeof(query_msg), "QUERY:%d", query_key);
        
        send_msg(num_processes, target_process, query_msg);
        printf("[Manager] Query %d: key=%d to process %d\n", 
               i + 1, query_key, target_process);
        
        // Variable delay to simulate different query patterns
        if (rand() % 3 == 0) {
            usleep((rand() % 200000) + 50000);  // 50-250ms (normal)
        } else {
            usleep(1000);  // 1ms (burst)
        }
    }
    
    // Wait for queries to complete
    printf("[Manager] Waiting for queries to complete...\n");
    sleep(2);
    
    // Cleanup
    printf("[Manager] Terminating processes...\n");
    for (int i = 0; i < num_processes; i++) {
        kill(process_pids[i], SIGTERM);
    }
    
    // Wait for all children
    for (int i = 0; i < num_processes; i++) {
        waitpid(process_pids[i], NULL, 0);
    }
    
    close_communication(num_processes, manager_fd);
    free(all_keys);
    free(process_pids);
    
    printf("[Manager] Shutdown complete\n");
    return 0;
}
