#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <signal.h>
#include <time.h>
#include "IPC.h"

#define MAX_MSG_LEN 256 //calculate this based on the maximum possible and numebr of keys
#define BLOOM_EXCHANGE_TIME 3


int num_processes = 4;
int keys_per_process = 10;

int *all_keys;
int total_keys;
pid_t *process_pids;
int manager_fd;

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
            perror("[ERROR HAPPENED] : execl failed");
            exit(1);
        } else if(pid > 0){
            process_pids[i] = pid;
            printf("Manager created process %d with PID %d\n", i, pid);
        } else {
            perror("Error happened : fork failed");
            exit(1);
        }
    }
    sleep(1);
}


//FIX THIS PART ACCORDING TO THE NUMBER OF KEYS WE WANT TO CREATE - Probably around one million or smth
void create_random_keys(){
    total_keys = num_processes * keys_per_process;
    all_keys = malloc(total_keys * sizeof(int));

    srand(time(NULL));

    for (int i = 0; i < total_keys; i++){
        all_keys[i] = rand() % 1000;
    }

    printf("Manager created %d random keys\n", total_keys);
}

void assign_random_keys(){
    for(int p = 0; p < num_processes; p++){
        char msg[MAX_MSG_LEN];
        int msg_pos = sprintf(msg, "KEYS:");

        int start_idx = p * keys_per_process;

        for (int k = 0; k < keys_per_process; k++){
            char key_str[20];
            int key_str_len;

            if(k == 0){
                key_str_len = snprintf(key_str, sizeof(key_str), "%d", all_keys[start_idx + k]);
            } else {
                key_str_len = snprintf(key_str, sizeof(key_str), ",%d", all_keys[start_idx + k]);
            }

            if(msg_pos + key_str_len >= MAX_MSG_LEN - 1){
                fprintf(stderr, "[ERROR HAPPENED] : Too many keys");
                exit(1);
            }

            strcpy(msg + msg_pos, key_str);
            msg_pos += key_str_len;
        }
        send_msg(num_processes, p, msg);

    }
    sleep(BLOOM_EXCHANGE_TIME);
}

//MODIFIED THE PROCESS FOR ADDITIONAL QUERY MESSAGES, MAY NEED TO MODIFY THAT SECTION, REMMEBER THAT FOR MESSAGE TYPE (But I think it should be fine for now)
//We need to adjust the number of queries as well.
int main(){
    manager_fd = initiate_communication(num_processes);
    create_processes();
    create_random_keys();
    assign_random_keys();
    int num_queries = 20;
    for(int i = 0; i < num_queries; i++){
        int target_process = rand() % num_processes;
        int query_key = all_keys[rand() % total_keys];
        char query_msg[MAX_MSG_LEN];
        snprintf(query_msg, sizeof(query_msg), "QUERY:%d", query_key);
        
        send_msg(num_processes, target_process, query_msg);
        printf("[Manager] Query %d: key=%d to process %d\n", 
               i + 1, query_key, target_process);
        if (rand() % 3 == 0) {
            usleep((rand() % 200000) + 50000);  // 50-250ms (normal)
        } else {
            usleep(1000);  // 1ms (burst)
        }
    }
    sleep(2);
    for (int i = 0; i < num_processes; i++) {
        kill(process_pids[i], SIGTERM);
    }
    for (int i = 0; i < num_processes; i++) {
        waitpid(process_pids[i], NULL, 0);
    }
    
    close_communication(num_processes, manager_fd);
    free(all_keys);
    free(process_pids);
    return 0;
}