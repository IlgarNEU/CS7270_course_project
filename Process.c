#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "IPC.h"
#include "bloom.h"

//Do we need to check its own bloom before checking its own content?

#define MAX_KEYS 100
#define MAX_PROCESSES 4
#define BUF_SIZE 256
#define BLOOM_MSG_SIZE 8192
#define FALSE_POSITIVE_RATE 0.01


int process_id;
int num_processes;
int keys[MAX_KEYS];
int num_keys = 0;

BloomFilter own_bloom;
BloomFilter *peer_bloom_filters;
int bloom_initialized = 0;
int *peer_bloom_received;

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

    create_own_bloom_filter();
}

void create_own_bloom_filter(){
    if(bloom_initialized){
        bloom_filter_destroy(&own_bloom);
    }

    bloom_filter_init(&own_bloom, num_keys > 0 ? num_keys : 10, FALSE_POSITIVE_RATE);
    for (int i = 0; i < num_keys; i++){
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "%d", keys[i]);
        bloom_filter_add_string(&own_bloom, key_str);
    }

    bloom_initialized = 1;
    printf("Process %d created bloom filter with %d keys\n", process_id, num_keys);
    bloom_filter_stats(&own_bloom);

    broadcast_bloom_filter();
}

void broadcast_bloom_filter(){
    char *hex_string = bloom_filter_export_hex_string(&own_bloom);
    if(hex_string == NULL){
        fprintf(stderr, "Process %d failed to export bloom filter\n", process_id);
        return;
    }

    size_t hex_len = strlen(hex_string);
    size_t msg_len = 7 + 10 + 1 + hex_len + 1;
    char *msg = malloc(msg_len);
    snprintf(msg, msg_len, "BLOOM:%d:%s", process_id, hex_string);

    for (int p = 0; p < num_processes; p++){
        if( p == process_id) continue;
        send_msg(process_id, p, msg);
    }

    printf("Process %d broadcasted bloom filter (hex size: %zu bytes)\n", process_id, hex_len);
    free(hex_string);
    free(msg);
}

void update_peer_bloom_filter(int peer_id, const char *bloom_data){
    printf("Process %d received bloom filter from process %d\n", process_id, peer_id);

    if(peer_bloom_filters == NULL){
        peer_bloom_filters = calloc(num_processes, sizeof(BloomFilter));
        peer_bloom_received = calloc(num_processes, sizeof(int));
    }

    if(peer_bloom_received[peer_id]){
        bloom_filter_destroy(&peer_bloom_filters[peer_id]);
    }

    int result = bloom_filter_import_hex_string(&peer_bloom_filters[peer_id], (char*)hex_data);

    if (result == BLOOM_SUCCESS){
        peer_bloom_received[peer_id] = 1;
        printf("Process %d Successfully imported bloom filter from process %d\n",
                process_id, peer_id
        );
        
    }else{
        fprintf(stderr, "Process %d failed to import bloom filter from process %d\n", process_id, peer_id);
    }
}


void handle_query_from_manager(int fd, const char *msg) {
    int key = atoi(msg + 6); 
    printf("[Process %d] Received query for key %d from manager\n", process_id, key);

    if (check_own_keys(key)) {
        printf("[Process %d] Key %d found locally\n", process_id, key);
    } else {
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "%d", key);
        for (int p = 0; p < num_processes; p++){
            if(p == process_id) continue;
            if(peer_bloom_received != NULL && peer_bloom_received[p] && bloom_filter_check_string(&peer_bloom_filters[p], key_str) != BLOOM_FAILURE){
                printf("Process %d Key %d might be in process %d, querying\n", process_id, key, p);
                char buf[BUF_SIZE];
                snprintf(buf, sizeof(buf), "%d", key);
                send_msg(process_id, p, buf);
            }
        }
    }
}


void handle_bloom_message(const char *msg){
    const char *first_colon = strchr(msg + 6, ':');
    if(first_colon == NULL){
        fprintf(stderr, "Process %d Invalid bloom message format\n", process_id);
        return;
    }
    int peer_id = atoi(msg+6);
    const char *hex_data = first_colon + 1;
    update_peer_bloom_filter(peer_id, hex_data);
}

void handle_query_from_process(int fd, const char *msg) {
    int key = atoi(msg);
    
    char key_str[32];
    snprintf(key_str, sizeof(key_str), "%d", key);

    if(check_own_keys(key)){

    }
}


int main(int argc, char *argv[]) {
    if (argc < 3) {
        printf("Usage: %s <process_id> <num_processes>\n", argv[0]);
        return 1;
    }

    process_id = atoi(argv[1]);
    num_processes = atoi(argv[2]);

    int fd = initiate_communication(process_id);

    printf("[Process %d] Started, waiting for key assignment from manager\n", process_id);

    char buf[BLOOM_MSG_SIZE];
    while (1) {
        int n = receive_msg(fd, buf, sizeof(buf));
        if (n > 0) {
            if (strncmp(buf, "KEYS:", 5) == 0) {
                assign_keys_from_message(buf);
            } else if (strncmp(buf, "QUERY:", 6) == 0) {
                handle_query_from_manager(fd, buf);
            } else if(strncmp(buf, "BLOOM:", 6) == 0){
                handle_bloom_message(buf);
            }else {
                handle_query_from_process(fd, buf);
            }
        }
        usleep(100000); 
    }

    if (bloom_initialized) {
        bloom_filter_destroy(&own_bloom);
    }
    if (peer_bloom_filters != NULL) {
        for (int i = 0; i < num_processes; i++) {
            if (peer_bloom_received[i]) {
                bloom_filter_destroy(&peer_bloom_filters[i]);
            }
        }
        free(peer_bloom_filters);
        free(peer_bloom_received);
    }

    close_communication(process_id, fd);
    return 0;
}
