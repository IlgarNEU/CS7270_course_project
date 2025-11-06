// manager.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <time.h>
#include "IPC.h"

#define MAX_MSG_LEN 256

// Configuration variables at the top
int num_processes = 4;
int keys_per_process = 10;

// Global variables
int *all_keys;
int total_keys;
pid_t *process_pids;
int manager_fd;

// Function 1: Create Processes
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
    
    // Give processes time to initialize their sockets
    sleep(1);
}

// Function 2: Create Random Keys
void create_random_keys() {
    total_keys = num_processes * keys_per_process;
    all_keys = malloc(total_keys * sizeof(int));
    
    srand(time(NULL));
    
    for (int i = 0; i < total_keys; i++) {
        all_keys[i] = rand() % 1000;  // Random keys between 0-999
    }
    
    printf("[Manager] Created %d random keys\n", total_keys);
}
// Function 3: Assign Random Keys (with chunking support)
void assign_random_keys() {
    // Calculate approximate max keys per message
    // "KEYS:" = 5 chars, each key ~3-4 digits + comma = ~5 chars average
    // Leave some buffer, so (256 - 6) / 5 = ~50 keys per message max
    int max_keys_per_msg = 40;  // Conservative estimate
    
    for (int p = 0; p < num_processes; p++) {
        int start_idx = p * keys_per_process;
        int keys_sent = 0;
        int chunk_number = 0;
        
        while (keys_sent < keys_per_process) {
            char msg[MAX_MSG_LEN];
            snprintf(msg, sizeof(msg), "KEYS:");
            
            // Calculate how many keys to send in this chunk
            int keys_in_chunk = keys_per_process - keys_sent;
            if (keys_in_chunk > max_keys_per_msg) {
                keys_in_chunk = max_keys_per_msg;
            }
            
            // Build the message with keys for this chunk
            for (int k = 0; k < keys_in_chunk; k++) {
                char key_str[20];
                if (k == 0) {
                    snprintf(key_str, sizeof(key_str), "%d", 
                            all_keys[start_idx + keys_sent + k]);
                } else {
                    snprintf(key_str, sizeof(key_str), ",%d", 
                            all_keys[start_idx + keys_sent + k]);
                }
                
                // Check if adding this key would exceed message length
                if (strlen(msg) + strlen(key_str) >= MAX_MSG_LEN - 1) {
                    // Stop adding keys to this message
                    keys_in_chunk = k;
                    break;
                }
                
                strcat(msg, key_str);
            }
            
            // Send this chunk
            send_msg(num_processes, p, msg);
            printf("[Manager] Sent keys chunk %d to process %d (%d keys)\n", 
                   chunk_number, p, keys_in_chunk);
            
            keys_sent += keys_in_chunk;
            chunk_number++;
            
            // Small delay between chunks to avoid overwhelming the receiver
            usleep(10000);  // 10ms
        }
        
        printf("[Manager] Finished sending all %d keys to process %d in %d chunks\n", 
               keys_per_process, p, chunk_number);
    }
    
    // Give processes time to receive and process all key chunks
    sleep(1);
}

// Function 4: Main
int main() {
    printf("[Manager] Starting with %d processes and %d keys per process\n", 
           num_processes, keys_per_process);
    
    // Initialize manager's communication (using ID = num_processes)
    manager_fd = initiate_communication(num_processes);
    
    // Step 1: Create processes
    create_processes();
    
    // Step 2: Create random keys
    create_random_keys();
    
    // Step 3: Assign keys to processes
    assign_random_keys();
    
    // Step 4: Send random queries
    printf("[Manager] Starting query phase...\n");
    srand(time(NULL));
    
    for (int i = 0; i < 10; i++) {  // Send 10 queries as example
        // Random delay between queries
        usleep((rand() % 500000) + 100000);  // 0.1 to 0.6 seconds
        
        // Pick random process to query
        int target_process = rand() % num_processes;
        
        // Pick random key to query for
        int query_key = all_keys[rand() % total_keys];
        
        char query_msg[MAX_MSG_LEN];
        snprintf(query_msg, sizeof(query_msg), "QUERY:%d", query_key);
        
        send_msg(num_processes, target_process, query_msg);
        printf("[Manager] Sent query for key %d to process %d\n", 
               query_key, target_process);
        
        // Randomly decide if next query should be parallel (sent immediately)
        if (rand() % 2 == 0 && i < 9) {  // 50% chance of parallel query
            i++;  // Count this as an additional query
            
            // Parallel query might go to same or different process (simulating real world)
            if (rand() % 3 == 0) {  
                // 33% chance: same process (multiple users querying same server)
                // Keep same target_process
            } else {
                // 67% chance: different process
                target_process = rand() % num_processes;
            }
            
            query_key = all_keys[rand() % total_keys];
            snprintf(query_msg, sizeof(query_msg), "QUERY:%d", query_key);
            send_msg(num_processes, target_process, query_msg);
            printf("[Manager] (Parallel) Sent query for key %d to process %d\n", 
                   query_key, target_process);
        }
    }
    
    // Let queries complete
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