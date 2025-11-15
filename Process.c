#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include "IPC.h"
#include "bloom.h"
#include <search.h>
#include <time.h>


#define MAX_KEYS 250000       //Need to discuss this with Professor for proper calculation
#define MAX_PROCESSES 16
#define BUF_SIZE 256          //Need to discuss this with Professor for proper calculation
#define BLOOM_MSG_SIZE 262144 //Need to discuss this with Professor for proper calculation
#define FALSE_POSITIVE_RATE 0.01 //Need to check this on GitHub and ask Professor for proper calculation
#define BLOOM_FILE_DIR "/tmp"

int process_id;
int num_processes;
int *keys = NULL; //Later we may convert this to hash table, ask Professor when project complete (not a priority)
int num_keys = 0;
int keys_capacity = 0;
int keys_finalized = 0;

BloomFilter own_bloom;
BloomFilter *peer_bloom_filters = NULL;
int bloom_initialized = 0;
int *peer_bloom_received = NULL;
int bloom_broadcasted = 0;

int comm_fd = -1;


void signal_handler(int signum);
int check_own_keys(int key);
void assign_keys_from_message(const char *msg);
void create_own_bloom_filter();
void broadcast_bloom_filter();
void update_peer_bloom_filter_from_file(int peer_id, const char *bloom_data);
void handle_query_from_manager(const char *msg);
void handle_bloom_message(const char *msg);
void handle_query_from_process(const char *msg);


void signal_handler(int signum){
    printf("\n[Process %d] Received signal %d, cleaning up... \n", process_id, signum);

    if(bloom_initialized){
        bloom_filter_destroy(&own_bloom);
    }
    if(peer_bloom_filters != NULL){
        for(int i = 0; i < num_processes; i++){
            if(peer_bloom_received && peer_bloom_received[i]){
                bloom_filter_destroy(&peer_bloom_filters[i]);
            }
        }
        free(peer_bloom_filters);
    }

    if(peer_bloom_received != NULL){
        free(peer_bloom_received);
    }
    if(keys != NULL){
        free(keys);
    }

    hdestroy();
    if(comm_fd >= 0){
        close_communication(process_id, comm_fd);
    }

    char filepath[256];
    snprintf(filepath, sizeof(filepath), "%s/bloom_process_%d.dat", BLOOM_FILE_DIR, process_id);
    unlink(filepath);
    exit(0);
}


//If change to hash table instead, remember to modify below function as well.
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

//If change to hash table instead, remember to modify below function as well. - DONE 
void assign_keys_from_message(const char *msg){
    const char *ptr = msg+5;
    char *copy = strdup(ptr);
    char *tok = strtok(copy, ",");

    //int keys_in_msg = 0;

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
        //keys_in_msg++;
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
    create_own_bloom_filter();
}

//Remember to modify the array part here as well if move to hash table
void create_own_bloom_filter(){
    if(bloom_initialized){
        bloom_filter_destroy(&own_bloom);
    }

    printf("Process %d creating bloom filer for %d keys \n", process_id, num_keys);
    time_t start = time(NULL);
    
    bloom_filter_init(&own_bloom, num_keys > 0 ? num_keys:10, FALSE_POSITIVE_RATE);

    for(int i = 0; i < num_keys; i++){
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "%d", keys[i]);
        bloom_filter_add_string(&own_bloom, key_str);

        if((i+1) % 500000 == 0){
            printf("Process %d added %d/%d keys to bloom (%.1f%%)\n", process_id, i+1, num_keys, (i+1) * 100.0 / num_keys);
        }
    }

    time_t end = time(NULL);

    bloom_initialized = 1;
    printf("[Process %d] Created bloom filter in %ld seconds\n", process_id, end-start);

    bloom_filter_stats(&own_bloom);
}

void broadcast_bloom_filter(){
    if(bloom_broadcasted) return;
    printf("PROCESS %d exporting bloom filter to file\n", process_id);

    char filepath[256];
    snprintf(filepath, sizeof(filepath), "%s/bloom_process_%d.dat", BLOOM_FILE_DIR, process_id);

    int result = bloom_filter_export(&own_bloom, filepath);

    if(result != BLOOM_SUCCESS){
        fprintf(stderr, "ERROR HAPPENED: process %d failed to export bloom filter", process_id);
        return;
    }

    FILE *fp = fopen(filepath, "rb");
    if(fp){
        fseek(fp, 0, SEEK_END);
        long size = ftell(fp);
        fclose(fp);
        printf("Process %d exported bloom filer %ld bytes\n", process_id, size);
    }

    char msg[256];
    snprintf(msg, sizeof(msg), "BLOOM_FILE:%d:%s", process_id, filepath);

    for (int p = 0; p < num_processes; p++){
        if(p == process_id) continue;
        send_msg(process_id, p, msg);
    }

    bloom_broadcasted = 1;
    printf("Process %d bloom filter location broadcasted\n", process_id);
}

void handle_bloom_message(const char *msg){
    if(strncmp(msg, "BLOOM_FILE:", 11) != 0){
        fprintf(stderr, "Process %d invalid bloom message \n", process_id);
        return;
    }

    int peer_id = atoi(msg+11);
    const char *colon = strchr(msg+11, ':');
    if(colon == NULL) return;

    const char *filepath = colon + 1;
    update_peer_bloom_filter_from_file(peer_id, filepath);
}


void update_peer_bloom_filter_from_file(int peer_id, const char *filepath){
    printf("SUCCESS : Process %d received bloom filter from process %d\n", process_id, peer_id);

    if(peer_bloom_filters == NULL){
        peer_bloom_filters = calloc(num_processes, sizeof(BloomFilter));
        peer_bloom_received = calloc(num_processes, sizeof(int));
    }

    if(peer_bloom_received[peer_id]){
        bloom_filter_destroy(&peer_bloom_filters[peer_id]);
    }

    int result = bloom_filter_import(&peer_bloom_filters[peer_id], (char*)filepath);

    if(result == BLOOM_SUCCESS){
        peer_bloom_received[peer_id] = 1;
        printf("SUCCESS : Process %d imported bloom filter from process %d\n", process_id, peer_id);
    } else {
        fprintf(stderr, "[ERROR HAPPENED] : Process %d failed to import bloom filter from %d\n", process_id, peer_id);
    }
}

//User query is below, it will come from manager (manager.c simulates users)
void handle_query_from_manager(const char *msg){
    int key = atoi(msg + 6);


    if(check_own_keys(key)){
        printf("[QUERY LOOKUP] : Process %d found key %d locally\n", process_id, key);
        
        char response[BUF_SIZE];
        snprintf(response, sizeof(response), "FOUND:%d:PROCESS_%d", key, process_id);
        send_msg(process_id, num_processes, response);
        return;
    }

    char key_str[32];
    snprintf(key_str, sizeof(key_str), "%d", key);

    int queries_sent = 0;
    for (int p = 0; p < num_processes; p++){
        if(p == process_id) continue;
        if(peer_bloom_received != NULL && peer_bloom_received[p] && bloom_filter_check_string(&peer_bloom_filters[p], key_str) != BLOOM_FAILURE){
            printf("[PROCESS %d detected that] key %d might be in process %d, querying it...\n", process_id, key, p);
            char buf[BUF_SIZE];
            snprintf(buf, sizeof(buf), "PQUERY:%d:FROM_%d", key, process_id);
            send_msg(process_id, p, buf);
            queries_sent++;
        }
    }

    if(queries_sent == 0){
        printf("Process %d could not find Key %d neither locally nor in blooms\n", process_id, key);
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

        if(bloom_initialized && !bloom_broadcasted){
            broadcast_bloom_filter();
        }
        int messages_processed = 0;

        while(1){
            int n = receive_msg(comm_fd, buf, BLOOM_MSG_SIZE);
            if(n <= 0) break;

            messages_processed++;

            if (strncmp(buf, "KEYS:", 5) == 0) {
                assign_keys_from_message(buf);
            } else if(strncmp(buf, "KEYS_DONE", 9) == 0){
                finalize_keys();
            } else if (strncmp(buf, "QUERY:", 6) == 0) {
                handle_query_from_manager(buf);
            } else if (strncmp(buf, "BLOOM_FILE:", 11) == 0) {
                handle_bloom_message(buf);
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





