#include "IPC.h"
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <sys/wait.h>

#define BUF_SIZE 256

int main() {
    pid_t pid = fork();

    if (pid == 0) {
        // Receiver process
        int fd = initiate_communication(1);
        char buf[BUF_SIZE];
        printf("[Receiver] Listening (non-blocking)...\n");

        for (int i = 0; i < 10; i++) {
            int n = receive_msg(fd, buf, sizeof(buf));
            if (n > 0) {
                printf("[Receiver] Got message: %s\n", buf);
            } else if (n == 0) {
                printf("[Receiver] No message yet, iteration %d\n", i);
            } else {
                printf("[Receiver] Error receiving\n");
            }
            usleep(500000); // 0.5 sec
        }

        close_communication(1, fd);
        return 0;
    } else {
        // Sender process
        sleep(2); // give receiver time to loop a few times

        printf("[Sender] Sending messages...\n");
        send_msg(0, 1, "Hello after delay!");
        send_msg(0, 1, "Second message!");
        send_msg(0, 1, "Final message!");

        wait(NULL); // wait for receiver
        return 0;
    }
}
