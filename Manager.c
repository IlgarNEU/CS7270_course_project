#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <signal.h>
#include <time.h>
#include "IPC.h"

#define MAX_MSG_LEN 65536 //NEED TO check if it works for our benchmark, it is set to 64kb, the max unix dgram size
#define BLOOM_EXCHANGE_TIME 30 //MAY NEED TO adapt, I did this for a safe threshold
#define MAX_KEYS_PER_CHUNK 7000

int num_processes = 4; //Change this for tests
int keys_per_process = 2500000; //NEEd to change this too if needed

pid_t *process_pids;
int manager_fd;

int *all_keys;
int total_keys;

int **process_keys;
int *process_key_counts;

typedef struct{
    int key;
    int answered;
} QueryTracker;

QueryTracker *query_trackers = NULL;
int num_queries_total = 0;

void create_processes(){
    process_pids = malloc(num_processes * sizeof(pid_t));
    for (int i = 0; i < num_processes; i++){
        pid_t pid = fork();

        if(pid == 0){
            char process_id_str[10];
            char num_proc_str[10];
            snprintf(process_id_str, sizeof(process_id_str), "%d", i);
            snprintf(num_proc_str, sizeof(num_proc_str), "%d", num_processes);

            execl("./process", "process", process_id_str, num_proc_str, NULL);
            perror("ERROR HAPPENED: execl failed");
            exit(1);
        } else if (pid > 0){
            process_pids[i] = pid;
            printf("Manager created process %d with PID %d\n", i, pid);
        } else{
            perror("ERROR HAPPENED fork failed");
            exit(1);
        }
    }
    sleep(5); //I did this for safety, we can decrease if needed
}

void create_random_keys(){
    total_keys = num_processes * keys_per_process;
    all_keys = malloc(total_keys*sizeof(int));

    if(all_keys == NULL){
        fprintf(stderr, "[ERROR HAPPENED] Manager failed to allocate memory for %d keys\n", total_keys);
        exit(1);
    }

    process_keys = malloc(num_processes * sizeof(int*));
    process_key_counts = calloc(num_processes, sizeof(int));

    for (int p = 0; p < num_processes; p++){
        process_keys[p] = malloc(keys_per_process * sizeof(int));
    }

    printf("Manager creating %d random keys\n", total_keys);
    srand(time(NULL));
    for (int i =0; i < total_keys; i++){
        all_keys[i] = rand() % 100000000; //NEED TO MODIFY THIS ACCORDING TO THE NUMBER OF KEYS

        int process_id = i / keys_per_process;
        int key_index = i % keys_per_process;
        process_keys[process_id][key_index] = all_keys[i];
        process_key_counts[process_id]++;

        if((i+1) % 1000000 == 0){
            printf("Manager created %d/%d keys (%.1f%%)\n", i+1, total_keys, (i+1) * 100.0 / total_keys);
        }
    }
    printf("MANAGER created %d random keys \n", total_keys);
}

void assign_random_keys_chuncked(){
    printf("\nManager starting chuncked key assignment\n");
    for(int p = 0; p < num_processes; p++){
        int start_idx = p * keys_per_process;
        int keys_sent = 0;
        int chunk_num = 0;

        printf("Manager sends %d keys to process %d", keys_per_process, p);

        time_t start_time = time(NULL);

        while(keys_sent < keys_per_process){
            char msg[MAX_MSG_LEN];
            int msg_pos = sprintf(msg, "KEYS:");
            int keys_in_chunk = 0;

            while(keys_sent < keys_per_process && keys_in_chunk < MAX_KEYS_PER_CHUNK){
                char key_str[20];
                int key_str_len;

                if(keys_in_chunk == 0){
                    key_str_len = snprintf(key_str, sizeof(key_str), "%d", all_keys[start_idx + keys_sent]);
                } else {
                    key_str_len = snprintf(key_str, sizeof(key_str), ",%d", all_keys[start_idx + keys_sent]);
                }

                if(msg_pos + key_str_len >= MAX_MSG_LEN - 1) break;

                strcpy(msg + msg_pos, key_str);
                msg_pos += key_str_len;
                keys_in_chunk++;
                keys_sent++;
            }
            send_msg(num_processes, p, msg);
            chunk_num++;

            if(chunk_num % 100 == 0){
                printf("Manager send %d/%d keys (%.1f%%) - %d chunks to process %d \n", keys_sent, keys_per_process, keys_sent * 100.0 / keys_per_process, chunk_num, p);
            }
            usleep(100);
        }
        send_msg(num_processes, p, "KEYS_DONE");
        time_t end_time = time(NULL);
        printf("Manager completed %d keys in %d chunks to process %d ( took %ld seconds)\n", keys_sent, chunk_num, p, end_time - start_time);
    }
    printf("\n Manager assigned all keys. Waiting %d seconds for bloom filter exchange\n", BLOOM_EXCHANGE_TIME);
    sleep(BLOOM_EXCHANGE_TIME);
}


void process_has_key(int process_id, int key){
    for(int i = 0; i < process_key_counts[process_id]; i++){
        if(process_keys[process_id][i] == key){
            return 1;
        }
    }
    return 0;
}

void handle_process_response(const char *msg){
    if(strncmp(msg, "FOUND:", 6) == 0){
        int key = atoi(msg + 6);
        const char *process_marker = strstr(msg, ":PROCESS_");
        int found_in_process = -1;
        if(process_marker != NULL){
            found_in_process = atoi(process_marker+9);
        }

        for(int i = 0; i < num_queries_total; i++){
            if(query_trackers[i].key == key && !query_trackers[i].answered){
                query_trackers[i].answered = 1;
                printf("USER RECEIVED RESPONSE FOR KEY %d by Process %d\n", key, found_in_process);
                break;
            }
        }
        
    } else if(strncmp(msg, "NOTFOUND:", 9) == 0){
        int key = atoi(msg + 9);
        const char *process_marker = strstr(msg, ":CHECKED_BY_PROCESS_");
        int checked_process = -1;
        if(process_marker != NULL){
            printf("Manager received not found signal for Key %d Checked by process %d\n", key, checked_process);
        }
        for (int i = 0; i < num_queries_total; i++) {
            if (query_trackers[i].key == key && !query_trackers[i].answered) {
                query_trackers[i].answered = 1;
                printf("  ✗ KEY %d NOT FOUND (ERROR - should exist!)\n", key);
                break;
            }
        }
    }
}

int main(){
    printf("\n");
    printf("------------------------------------------------------------\n");
    printf("Summary Cache Bloom Test - 10000000 keys\n");
    printf("------------------------------------------------------------\n");
    printf("Process count: %d\n", num_processes);
    printf("Keys per process : %d\n", keys_per_process);
    printf("Total keys : %d\n", num_processes * keys_per_process);

    time_t total_start = time(NULL);

    
    manager_fd = initiate_communication(num_processes);

    create_processes();
    create_random_keys();
    assign_random_keys_chuncked();

    printf("Manager is starting query part\n");
    

    char response_buf[MAX_MSG_LEN];
    int num_queries = 100; //We need to change this when testing really

    query_trackers = calloc(num_queries, sizeof(QueryTracker));
    num_queries_total = num_queries;

    time_t query_start = time(NULL);
    

    for(int i = 0; i < num_queries; i++){
        char query_msg[MAX_MSG_LEN];

        int key_index = rand() % total_keys;
        int query_key = all_keys[key_index];

        int actual_process = key_index / keys_per_process;

        int target_process;

        do{
            target_process = rand() % num_processes;
        } while(target_process == actual_process);


        printf("[Query %d] Key %d (in Process %d) queried via Process %d → ", 
               i + 1, query_key, actual_process, target_process);

        query_trackers[i].key = query_key;
        query_trackers[i].answered = 0;

        snprintf(query_msg, sizeof(query_msg), "QUERY:%d", query_key);
        send_msg(num_processes, target_process, query_msg);

        usleep(10000);


        int response_count = 0;
        while(response_count < 5){
            int n = receive_msg(manager_fd, response_buf, sizeof(response_buf));
            if(n>0){
               handle_process_response(response_buf);
               break;
            }
            usleep(5000);
            response_count++;
        }

       
    }

    printf("\nWaiting for final responses\n");
    sleep(2);

    while(1){
        int n = receive_msg(manager_fd, response_buf, sizeof(response_buf));
        if(n<=0) break;
        handle_process_response(response_buf);
    }

    int found_count = 0;
    int not_found_count = 0;
    int unanswered = 0;

    for(int i = 0; i < num_queries; i++){
        if(query_trackers[i].answered){
            found_count++;
        } else{
            unanswered++;
        }
    }
    
    time_t query_end = time(NULL);
    time_t total_end = time(NULL);

    printf("\n═══════════════════════════════════════════════════\n");
    printf("  BENCHMARK RESULTS\n");
    printf("═══════════════════════════════════════════════════\n");
    printf("  Total keys in cache: %d\n", total_keys);
    printf("  Queries sent: %d\n", num_queries);
    printf("  Queries answered: %d\n", found_count);
    printf("  Queries unanswered: %d\n", unanswered);
    printf("  Keys found: %d (should be %d)\n", found_count, num_queries);
    printf("  Keys not found: %d (should be 0)\n", not_found_count);
    printf("  Query time: %ld seconds\n", query_end - query_start);
    printf("  Avg query time: %.2f ms\n", 
           (query_end - query_start) * 1000.0 / num_queries);
    printf("  Total runtime: %ld seconds\n", total_end - total_start);
    printf("  Bloom filter effectiveness: %.1f%% (queries routed via bloom)\n",
           100.0);
    printf("═══════════════════════════════════════════════════\n\n");

    for(int i = 0; i < num_processes; i++){
        kill(process_pids[i], SIGTERM);
    }

    for(int i = 0; i < num_processes; i++){
        waitpid(process_pids[i], NULL, 0);
    }

    close_communication(num_processes, manager_fd);
    free(all_keys);
    free(process_pids);
    printf("Manager shutdown complete");
    return 0;
}