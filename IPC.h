#ifndef IPC_H
#define IPC_H

#include <stddef.h>

// Initialize communication for a process
int initiate_communication(int process_id);

// Send a message from sender to receiver
int send_msg(int sender_id, int receiver_id, const char *msg);

// Receive a message (non-blocking)
int receive_msg(int fd, char *buf, size_t buf_size);

// Close communication and cleanup
void close_communication(int process_id, int fd);

// Cleanup IPC resources
void cleanup_ipc();

#endif // IPC_H