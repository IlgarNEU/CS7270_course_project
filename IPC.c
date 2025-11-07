#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>

#define SOCKET_DIR "/tmp/dist_cache_sockets"
#define MAX_PROCESSES 16

// Cache of sender sockets to avoid creating/destroying repeatedly
static int sender_sockets[MAX_PROCESSES];
static int sender_sockets_initialized = 0;

static void init_sender_sockets() {
    if (!sender_sockets_initialized) {
        for (int i = 0; i < MAX_PROCESSES; i++) {
            sender_sockets[i] = -1;
        }
        sender_sockets_initialized = 1;
    }
}

static void make_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) {
        perror("fcntl F_GETFL");
        return;
    }
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("fcntl F_SETFL");
    }
}

int initiate_communication(int process_id) {
    char sock_path[108];
    struct sockaddr_un addr;
    int fd;

    // Initialize sender socket cache
    init_sender_sockets();

    // Create socket directory
    if (mkdir(SOCKET_DIR, 0777) < 0 && errno != EEXIST) {
        perror("mkdir");
        exit(EXIT_FAILURE);
    }

    snprintf(sock_path, sizeof(sock_path), "%s/proc_%d.sock", SOCKET_DIR, process_id);

    // Remove any existing socket file
    unlink(sock_path);

    // Create datagram socket
    if ((fd = socket(AF_UNIX, SOCK_DGRAM, 0)) < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
    }

    // Increase receive buffer size to handle bursts
    int rcvbuf = 262144; // 256KB
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf)) < 0) {
        perror("setsockopt SO_RCVBUF");
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, sock_path, sizeof(addr.sun_path) - 1);

    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(fd);
        exit(EXIT_FAILURE);
    }

    make_nonblocking(fd);
    
    printf("[IPC] Process %d initialized on %s\n", process_id, sock_path);
    return fd;
}

int send_msg(int sender_id, int receiver_id, const char *msg) {
    struct sockaddr_un addr;
    char sock_path[108];
    int fd;
    ssize_t n;
    size_t msg_len = strlen(msg) + 1;

    // Check message size (Unix datagram limit is typically 64KB)
    if (msg_len > 65000) {
        fprintf(stderr, "[IPC] Message too large: %zu bytes (max ~65000)\n", msg_len);
        return -1;
    }

    // Reuse existing socket or create new one
    if (sender_sockets[receiver_id] < 0) {
        if ((fd = socket(AF_UNIX, SOCK_DGRAM, 0)) < 0) {
            perror("socket (send)");
            return -1;
        }
        sender_sockets[receiver_id] = fd;
    } else {
        fd = sender_sockets[receiver_id];
    }

    snprintf(sock_path, sizeof(sock_path), "%s/proc_%d.sock", SOCKET_DIR, receiver_id);
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, sock_path, sizeof(addr.sun_path) - 1);

    n = sendto(fd, msg, msg_len, 0, (struct sockaddr*)&addr, sizeof(addr));
    if (n < 0) {
        if (errno == ENOENT) {
            // Receiver socket doesn't exist yet, this is OK during startup
            return -1;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            fprintf(stderr, "[IPC] Send buffer full, receiver %d is slow\n", receiver_id);
            return -1;
        }
        perror("sendto");
        return -1;
    }

    return 0;
}

int receive_msg(int fd, char *buf, size_t buf_size) {
    ssize_t n = recv(fd, buf, buf_size - 1, 0);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0; // No message available
        }
        perror("recv");
        return -1;
    }
    buf[n] = '\0';
    return n;
}

void close_communication(int process_id, int fd) {
    char sock_path[108];
    
    // Close all sender sockets
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (sender_sockets[i] >= 0) {
            close(sender_sockets[i]);
            sender_sockets[i] = -1;
        }
    }

    // Close receiver socket
    snprintf(sock_path, sizeof(sock_path), "%s/proc_%d.sock", SOCKET_DIR, process_id);
    close(fd);
    unlink(sock_path);
    
    printf("[IPC] Process %d closed communication\n", process_id);
}

// Cleanup function to call on exit
void cleanup_ipc() {
    for (int i = 0; i < MAX_PROCESSES; i++) {
        if (sender_sockets[i] >= 0) {
            close(sender_sockets[i]);
            sender_sockets[i] = -1;
        }
    }
}