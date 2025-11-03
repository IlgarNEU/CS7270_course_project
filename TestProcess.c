#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "IPC.h"

#define BUF_SIZE 256

int main() {
    int process_id = 0;
    int num_processes = 2;

    printf("[Test] Starting process test for process_id=%d\n", process_id);

    // Start the process socket
    int fd = initiate_communication(process_id);

    // 1️⃣ Send keys to the process
    char keys_msg[] = "KEYS:10,20,30,40";
    send_msg(999, process_id, keys_msg);  // sender_id=999 just as test

    // Allow process to receive keys
    usleep(500000); // 0.5s

    // 2️⃣ Send queries for different keys
    char query1[] = "QUERY:20"; // exists
    char query2[] = "QUERY:25"; // does not exist
    char query3[] = "QUERY:40"; // exists

    printf("[Test] Sending query: %s\n", query1);
    send_msg(999, process_id, query1);
    usleep(500000);

    printf("[Test] Sending query: %s\n", query2);
    send_msg(999, process_id, query2);
    usleep(500000);

    printf("[Test] Sending query: %s\n", query3);
    send_msg(999, process_id, query3);
    usleep(500000);

    // 3️⃣ Receive any responses from process (simulate non-blocking)
    char buf[BUF_SIZE];
    for (int i = 0; i < 5; i++) {
        int n = receive_msg(fd, buf, sizeof(buf));
        if (n > 0) {
            printf("[Test] Received message: %s\n", buf);
        } else if (n == 0) {
            printf("[Test] No message yet (non-blocking)\n");
        }
        usleep(500000);
    }

    close_communication(process_id, fd);
    printf("[Test] Process test finished.\n");

    return 0;
}
