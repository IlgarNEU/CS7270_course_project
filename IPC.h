#include <stddef.h>
#include <string.h>
#ifndef IPC_H
#define IPC_H
#define MAX_MSG_LEN 256

int initiate_communication(int process_id);
int send_msg(int sender_id, int receiver_od, const char *msg);
int receive_msg(int fd, char *buf, size_t buf_size);
void close_communication(int process_id, int fd);

#endif
