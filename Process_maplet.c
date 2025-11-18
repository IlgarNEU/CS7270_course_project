#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include "IPC.h"
// MODIFIED: Changed from bloom.h to gqf headers
#include "gqf.h"           // MODIFIED
#include "gqf_file.h"      // MODIFIED
#include <search.h>
#include <time.h>


#define MAX_KEYS 250000       
#define MAX_PROCESSES 64
#define BUF_SIZE 256          
#define BLOOM_MSG_SIZE 262144 
#define FALSE_POSITIVE_RATE 0.01 
// COMMENTED OUT: No longer need file directory for QF
// #define QF_FILE_DIR "/tmp"    // MODIFIED: Renamed from BLOOM_FILE_DIR

int process_id;
int num_processes;
int *keys = NULL;
int num_keys = 0;
int keys_capacity = 0;
int keys_finalized = 0;

// MODIFIED: Single QF for all processes instead of own_bloom and peer_bloom_filters array
QF all_processes_qf;                   // MODIFIED: Single QF containing keys from all processes with value=process_id
int qf_initialized = 0;                // MODIFIED: Renamed from bloom_initialized
int *peer_qf_received = NULL;          // MODIFIED: Renamed from peer_bloom_received - tracks which peers' keys we've received
int qf_broadcasted = 0;                // MODIFIED: Renamed from bloom_broadcasted - tracks if we've sent our keys

int comm_fd = -1;


void signal_handler(int signum);
int check_own_keys(int key);
void assign_keys_from_message(const char *msg);
void create_own_qf();                  // MODIFIED: Renamed from create_own_bloom_filter
void broadcast_qf();                   // MODIFIED: Renamed from broadcast_bloom_filter - now sends keys instead of files
void handle_qf_update(const char *msg);  // MODIFIED: New function to handle QF_UPDATE messages
void handle_query_from_manager(const char *msg);
void handle_query_from_process(const char *msg);


void signal_handler(int signum){
    printf("\n[Process %d] Received signal %d, cleaning up... \n", process_id, signum);

    // MODIFIED: Clean up single QF instead of own_bloom and peer_bloom_filters array
    if(qf_initialized){                                    // MODIFIED
        qf_free(&all_processes_qf);                        // MODIFIED: Free single QF
    }

    if(peer_qf_received != NULL){                          // MODIFIED
        free(peer_qf_received);                            // MODIFIED
    }
    if(keys != NULL){
        free(keys);
    }

    hdestroy();
    if(comm_fd >= 0){
        close_communication(process_id, comm_fd);
    }

    // COMMENTED OUT: No longer using files
    // char filepath[256];
    // snprintf(filepath, sizeof(filepath), "%s/qf_process_%d.dat", QF_FILE_DIR, process_id);  // MODIFIED
    // unlink(filepath);
    exit(0);
}


int check_own_keys(int key){
    if(!keys_finalized) return 0;
    char key_str[32];
    snprintf(key_str, sizeof(key_str), "%d", key);

    ENTRY e, *ep;
    e.key = key_str;
    e.data = NULL;

    ep = hsearch(e, FIND);
    return (ep != NULL);

}

void assign_keys_from_message(const char *msg){
    const char *ptr = msg+5;
    char *copy = strdup(ptr);
    char *tok = strtok(copy, ",");

    while(tok != NULL){
        if(num_keys >= keys_capacity){
            int new_capacity = keys_capacity == 0 ? 100000 : keys_capacity * 2;
            int *new_keys = realloc(keys, new_capacity * sizeof(int));
            if(new_keys == NULL){
                fprintf(stderr, "ERROR HAPPENED: process %d failed to allocate memory for keys \n", process_id);
                free(copy);
                exit(1);
            }
            keys = new_keys;
            keys_capacity = new_capacity;
        }
        keys[num_keys++] = atoi(tok);
        tok = strtok(NULL, ",");
    }
    free(copy);

    if(num_keys % 100000 == 0){
        printf("Process %d received %d keys so far\n", process_id, num_keys);
    }
}

void finalize_keys(){
    if(keys_finalized) return;
    printf("Process %d finalizign %d keys\n", process_id, num_keys);

    time_t start = time(NULL);

    if(hcreate(num_keys * 2) == 0){
        fprintf(stderr, "[ERROR HAPPENED] Process %d failed to create hash table \n", process_id);
        exit(1);
    }

    for(int i = 0; i < num_keys; i++){
        char *key_str = malloc(32);
        snprintf(key_str, 32, "%d", keys[i]);

        ENTRY e;
        e.key = key_str;
        e.data = (void*)(long)1;
        if(hsearch(e, ENTER) == NULL){
            fprintf(stderr, "Process %d failed to insert key %d\n", process_id, keys[i]);
        }

        if((i+1) % 500000 == 0){
            printf("Process %d indexed %d/%d keys (%.1f%%)\n", i+1, process_id, num_keys, (i+1) * 100.0/num_keys);
        }
    }

    time_t end = time(NULL);
    printf("Process %d hash table created in %ld seconds \n", process_id, end-start);
    keys_finalized = 1;
    create_own_qf();                                       // MODIFIED: Renamed function call
}

// MODIFIED: Renamed function and creates single QF with own keys (value = process_id)
void create_own_qf(){                                      // MODIFIED: Renamed from create_own_bloom_filter
    // MODIFIED: Initialize single QF for all processes
    if(qf_initialized){                                    // MODIFIED
        qf_free(&all_processes_qf);                        // MODIFIED
    }

    printf("Process %d creating QF for %d keys \n", process_id, num_keys);  // MODIFIED
    time_t start = time(NULL);
    
    // MODIFIED: Initialize single QF with capacity for own keys (will grow as we add peer keys)
    uint64_t nslots = num_keys > 0 ? num_keys : 10;                                    // MODIFIED
    if(!qf_malloc(&all_processes_qf, nslots, 64, 8, QF_HASH_DEFAULT, 0)){            // MODIFIED: value_bits=8 to store process_id (up to 256 processes)
        fprintf(stderr, "ERROR: Process %d failed to allocate QF\n", process_id);     // MODIFIED
        exit(1);                                                                       // MODIFIED
    }                                                                                  // MODIFIED
    
    // MODIFIED: Enable auto-resize for when we add peer keys
    qf_set_auto_resize(&all_processes_qf, true);                                      // MODIFIED

    // MODIFIED: Initialize peer tracking array
    if(peer_qf_received == NULL){                                                     // MODIFIED
        peer_qf_received = calloc(num_processes, sizeof(int));                        // MODIFIED
    }                                                                                  // MODIFIED

    // MODIFIED: Insert own keys with value = process_id
    for(int i = 0; i < num_keys; i++){
        uint64_t key_val = (uint64_t)keys[i];                                        // MODIFIED
        qf_insert(&all_processes_qf, key_val, process_id, 1, QF_NO_LOCK);           // MODIFIED: value = process_id
        
        if((i+1) % 500000 == 0){
            printf("Process %d added %d/%d keys to QF (%.1f%%)\n", process_id, i+1, num_keys, (i+1) * 100.0 / num_keys);  // MODIFIED
        }
    }

    time_t end = time(NULL);

    qf_initialized = 1;                                                               // MODIFIED
    printf("[Process %d] Created QF in %ld seconds\n", process_id, end-start);      // MODIFIED

    // MODIFIED: Print QF stats
    printf("[QF Stats] nslots: %lu, occupied: %lu, distinct elements: %lu\n",        // MODIFIED
           qf_get_nslots(&all_processes_qf),                                         // MODIFIED
           qf_get_num_occupied_slots(&all_processes_qf),                             // MODIFIED
           qf_get_num_distinct_key_value_pairs(&all_processes_qf));                 // MODIFIED
}

// MODIFIED: Now sends keys in batches using new QF_UPDATE protocol
void broadcast_qf(){                                                                  // MODIFIED: Renamed from broadcast_bloom_filter
    if(qf_broadcasted) return;                                                        // MODIFIED
    printf("PROCESS %d broadcasting keys to all processes\n", process_id);           // MODIFIED

    // MODIFIED: Send keys in batches to all other processes using QF_UPDATE protocol
    int batch_size = 10000;  // Send 10000 keys per message                          // MODIFIED
    char *msg_buf = malloc(BLOOM_MSG_SIZE);                                          // MODIFIED
    if(msg_buf == NULL){                                                             // MODIFIED
        fprintf(stderr, "Process %d failed to allocate message buffer\n", process_id);  // MODIFIED
        return;                                                                       // MODIFIED
    }                                                                                 // MODIFIED

    for (int p = 0; p < num_processes; p++){                                         // MODIFIED
        if(p == process_id) continue;                                                 // MODIFIED
        
        // MODIFIED: Send keys in batches using QF_UPDATE protocol
        for(int i = 0; i < num_keys; ){                                              // MODIFIED
            // MODIFIED: Format: "QF_UPDATE:sender_id:key1,key2,key3,..."
            int msg_len = snprintf(msg_buf, BLOOM_MSG_SIZE, "QF_UPDATE:%d:", process_id);  // MODIFIED: New protocol with sender_id
            int keys_in_batch = 0;                                                    // MODIFIED
            
            // MODIFIED: Pack as many keys as fit in the message buffer
            while(i < num_keys && keys_in_batch < batch_size){                       // MODIFIED
                char key_str[32];                                                     // MODIFIED
                snprintf(key_str, sizeof(key_str), "%d", keys[i]);                   // MODIFIED
                
                // MODIFIED: Check if adding this key would overflow the buffer
                if(msg_len + strlen(key_str) + 2 >= BLOOM_MSG_SIZE){                // MODIFIED
                    break;                                                            // MODIFIED
                }                                                                     // MODIFIED
                
                if(keys_in_batch > 0){                                               // MODIFIED
                    msg_len += snprintf(msg_buf + msg_len, BLOOM_MSG_SIZE - msg_len, ",");  // MODIFIED
                }                                                                     // MODIFIED
                msg_len += snprintf(msg_buf + msg_len, BLOOM_MSG_SIZE - msg_len, "%d", keys[i]);  // MODIFIED
                
                i++;                                                                  // MODIFIED
                keys_in_batch++;                                                      // MODIFIED
            }                                                                         // MODIFIED
            
            // MODIFIED: Send this batch
            send_msg(process_id, p, msg_buf);                                        // MODIFIED
            
            if((i % 100000) == 0 || i == num_keys){                                  // MODIFIED
                printf("Process %d sent %d/%d keys to process %d\n", process_id, i, num_keys, p);  // MODIFIED
            }                                                                         // MODIFIED
        }                                                                             // MODIFIED
        
        // MODIFIED: Send QF_UPDATE_DONE to signal completion to this peer
        snprintf(msg_buf, BLOOM_MSG_SIZE, "QF_UPDATE_DONE:%d:%d", process_id, num_keys);  // MODIFIED: Include total count for verification
        send_msg(process_id, p, msg_buf);                                            // MODIFIED
        printf("Process %d finished sending %d keys to process %d\n", process_id, num_keys, p);  // MODIFIED
    }                                                                                 // MODIFIED

    free(msg_buf);                                                                    // MODIFIED
    qf_broadcasted = 1;                                                               // MODIFIED
    printf("Process %d completed broadcasting all keys\n", process_id);              // MODIFIED

    // COMMENTED OUT: Old file-based broadcast code
    // char filepath[256];
    // snprintf(filepath, sizeof(filepath), "%s/qf_process_%d.dat", QF_FILE_DIR, process_id);
    // uint64_t result = qf_serialize(&all_processes_qf, filepath);
    // if(result < sizeof(qfmetadata) + all_processes_qf.metadata->total_size_in_bytes){
    //     fprintf(stderr, "ERROR HAPPENED: process %d failed to export QF\n", process_id);
    //     return;
    // }
    // FILE *fp = fopen(filepath, "rb");
    // if(fp){
    //     fseek(fp, 0, SEEK_END);
    //     long size = ftell(fp);
    //     fclose(fp);
    //     printf("Process %d exported QF %ld bytes\n", process_id, size);
    // }
    // char msg[256];
    // snprintf(msg, sizeof(msg), "QF_FILE:%d:%s", process_id, filepath);
    // for (int p = 0; p < num_processes; p++){
    //     if(p == process_id) continue;
    //     send_msg(process_id, p, msg);
    // }
    // qf_broadcasted = 1;
    // printf("Process %d QF location broadcasted\n", process_id);
}

// MODIFIED: New function to handle QF_UPDATE protocol messages
void handle_qf_update(const char *msg){                                              // MODIFIED
    // MODIFIED: Check for QF_UPDATE message: "QF_UPDATE:sender_id:key1,key2,..."
    if(strncmp(msg, "QF_UPDATE:", 10) == 0){                                        // MODIFIED
        // MODIFIED: Extract sender process_id
        int sender_id = atoi(msg + 10);                                              // MODIFIED
        const char *colon = strchr(msg + 10, ':');                                   // MODIFIED
        if(colon == NULL){                                                            // MODIFIED
            fprintf(stderr, "Process %d received malformed QF_UPDATE message\n", process_id);  // MODIFIED
            return;                                                                   // MODIFIED
        }                                                                             // MODIFIED
        
        // MODIFIED: Parse comma-separated keys and insert into our all_processes_qf
        const char *keys_data = colon + 1;                                           // MODIFIED
        char *copy = strdup(keys_data);                                              // MODIFIED
        char *tok = strtok(copy, ",");                                               // MODIFIED
        
        int keys_inserted = 0;                                                        // MODIFIED
        while(tok != NULL){                                                           // MODIFIED
            int key = atoi(tok);                                                      // MODIFIED
            uint64_t key_val = (uint64_t)key;                                        // MODIFIED
            
            // MODIFIED: Insert key with value = sender_id (the process that owns this key)
            qf_insert(&all_processes_qf, key_val, sender_id, 1, QF_NO_LOCK);        // MODIFIED
            keys_inserted++;                                                          // MODIFIED
            
            if(keys_inserted % 100000 == 0){                                          // MODIFIED
                printf("Process %d inserted %d keys from process %d\n", process_id, keys_inserted, sender_id);  // MODIFIED
            }                                                                         // MODIFIED
            
            tok = strtok(NULL, ",");                                                  // MODIFIED
        }                                                                             // MODIFIED
        
        free(copy);                                                                   // MODIFIED
        
        if(keys_inserted > 0){                                                        // MODIFIED
            printf("Process %d inserted batch of %d keys from process %d\n", process_id, keys_inserted, sender_id);  // MODIFIED
        }                                                                             // MODIFIED
        return;                                                                       // MODIFIED
    }                                                                                 // MODIFIED
    
    // MODIFIED: Check for QF_UPDATE_DONE message: "QF_UPDATE_DONE:sender_id:total_count"
    if(strncmp(msg, "QF_UPDATE_DONE:", 15) == 0){                                   // MODIFIED
        int sender_id = atoi(msg + 15);                                              // MODIFIED
        const char *colon = strchr(msg + 15, ':');                                   // MODIFIED
        int expected_count = 0;                                                       // MODIFIED
        if(colon != NULL){                                                            // MODIFIED
            expected_count = atoi(colon + 1);                                        // MODIFIED
        }                                                                             // MODIFIED
        
        peer_qf_received[sender_id] = 1;                                             // MODIFIED
        printf("Process %d received all keys from process %d (expected: %d)\n", process_id, sender_id, expected_count);  // MODIFIED
        return;                                                                       // MODIFIED
    }                                                                                 // MODIFIED
    
    fprintf(stderr, "Process %d received unknown QF protocol message\n", process_id);  // MODIFIED
}

void handle_query_from_manager(const char *msg){
    int key = atoi(msg + 6);


    if(check_own_keys(key)){
        printf("[QUERY LOOKUP] : Process %d found key %d locally\n", process_id, key);
        
        char response[BUF_SIZE];
        snprintf(response, sizeof(response), "FOUND:%d:PROCESS_%d", key, process_id);
        send_msg(process_id, num_processes, response);
        return;
    }

    // MODIFIED: Convert to uint64_t for QF lookup
    uint64_t key_val = (uint64_t)key;                                               // MODIFIED

    int queries_sent = 0;
    for (int p = 0; p < num_processes; p++){
        if(p == process_id) continue;
        // MODIFIED: Check single QF with specific peer's process_id value
        if(peer_qf_received != NULL && peer_qf_received[p] &&                       // MODIFIED
           qf_count_key_value(&all_processes_qf, key_val, p, 0) > 0){              // MODIFIED: Check for key with value=p (peer's process_id)
            printf("[PROCESS %d detected that] key %d might be in process %d, querying it...\n", process_id, key, p);
            char buf[BUF_SIZE];
            snprintf(buf, sizeof(buf), "PQUERY:%d:FROM_%d", key, process_id);
            send_msg(process_id, p, buf);
            queries_sent++;
        }
    }

    if(queries_sent == 0){
        printf("Process %d could not find Key %d neither locally nor in QF\n", process_id, key);  // MODIFIED
        char response[BUF_SIZE];
        snprintf(response, sizeof(response), "NOTFOUND:%d:CHECKED_BY_PROCESS_%d", key, process_id);
        send_msg(process_id, num_processes, response);
    }
    
}


void handle_query_from_process(const char *msg){
    if(strncmp(msg, "PQUERY:", 7) != 0){
        return;
    }

    int key = atoi(msg+7);

    const char *from_marker = strstr(msg, ":FROM_");
    int sender_process = -1;
    if(from_marker != NULL){
        sender_process = atoi(from_marker + 6);
    }

    printf("Process %d Received peer query for key %d from process %d\n", process_id, key, sender_process);

    if(check_own_keys(key)){
        printf("Process %d found key %d which is a peer query", process_id, key);

        if(sender_process >= 0){
            char response[BUF_SIZE];
            snprintf(response, sizeof(response), "PFOUND:%d:IN_PROCESS_%d", key, process_id);
            send_msg(process_id, sender_process, response);
        }
    } else{
        printf("Process %d could not find key %d", process_id, key);

        if(sender_process >= 0){
            char response[BUF_SIZE];
            snprintf(response, sizeof(response), "PNOTFOUND:%d:IN_PROCESS_%d", key, process_id);
            send_msg(process_id, sender_process, response);
        }
    }
}

void handle_response_from_process(const char *msg){
    if(strncmp(msg, "PFOUND:", 7) == 0){
        int key = atoi(msg + 7);
        const char *process_marker = strstr(msg, ":IN_PROCESS_");
        int found_in_process = -1;
        if(process_marker != NULL){
            found_in_process = atoi(process_marker + 12);
        }

        printf("Process %d Confirmed the existence of Key %d in process %d\n", process_id, key, found_in_process);

        char response[BUF_SIZE];
        snprintf(response, sizeof(response), "FOUND:%d:PROCESS_%d", key, found_in_process);
        send_msg(process_id, num_processes, response);
    } else if (strncmp(msg, "PNOTFOUND:", 10) == 0){
        int key = atoi(msg + 10);
        const char *process_marker = strstr(msg, ":IN_PROCESS_");
        int checked_process = -1;
        if(process_marker != NULL){
            checked_process = atoi(process_marker + 12);
        }

        printf("Process %d could not find key %d in process %d\n", process_id, key, checked_process);
    }
}


int main(int argc, char *argv[]){
    if(argc < 3){
        fprintf(stderr, "Usage: %s <process_id> <num_processes> \n", argv[0]);
        return 1;
    }

    process_id = atoi(argv[1]);
    num_processes = atoi(argv[2]);

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    comm_fd = initiate_communication(process_id);
    printf("Process %d started, waiting for key assignment\n", process_id);

    char *buf = malloc(BLOOM_MSG_SIZE);
    if(buf == NULL){
        fprintf(stderr, "Process %d failed to allocate receive buffer\n", process_id);
        return 1;
    }

    while(1){

        // MODIFIED: Renamed variables
        if(qf_initialized && !qf_broadcasted){                                       // MODIFIED
            broadcast_qf();                                                          // MODIFIED: Renamed function call
        }
        int messages_processed = 0;

        while(1){
            int n = receive_msg(comm_fd, buf, BLOOM_MSG_SIZE);
            if(n <= 0) break;

            messages_processed++;

            // MODIFIED: Keep original KEYS: handling for manager's initial key assignment
            if (strncmp(buf, "KEYS:", 5) == 0) {
                assign_keys_from_message(buf);
            } else if(strncmp(buf, "KEYS_DONE", 9) == 0){
                finalize_keys();
            } else if (strncmp(buf, "QUERY:", 6) == 0) {
                handle_query_from_manager(buf);
            } else if (strncmp(buf, "QF_UPDATE:", 10) == 0 || strncmp(buf, "QF_UPDATE_DONE:", 15) == 0) {  // MODIFIED: Handle new QF_UPDATE protocol
                handle_qf_update(buf);                                               // MODIFIED
            } else if (strncmp(buf, "PQUERY:", 7) == 0) {
                handle_query_from_process(buf);
            } else if (strncmp(buf, "PFOUND:", 7) == 0 || strncmp(buf, "PNOTFOUND:", 10) == 0) {
                handle_response_from_process(buf);
            } else {
                fprintf(stderr, "[Process %d] Unknown message: %s\n", process_id, buf);
            }
        }
        if (messages_processed == 0) {
            usleep(1000);
        }
    }
    free(buf);
    signal_handler(0);
    
    return 0;
}