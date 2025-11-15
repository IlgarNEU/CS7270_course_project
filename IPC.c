#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>

#define SOCKET_DIR "/tmp/distributed_cache_sockets"
#define MAX_PROCESSES 64

static int sender_sockets[MAX_PROCESSES];
static int sender_sockets_initialized = 0;

static void init_sender_sockets(){
    if(!sender_sockets_initialized){
        for (int i = 0; i < MAX_PROCESSES; i++){
            sender_sockets[i] = -1;
        }
        sender_sockets_initialized = 1;
    }
}

static void make_nonblocking(int fd){
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1){
        perror("[ERROR HAPPENED] : Error happened when getting flags for nonblocking communication - fcntl F_GETFL");
        return;
    }
    if(fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1){
        perror("[ERROR HAPPENED] : Error happened when setting flags for nonblocking communication - fcntl F_SETFL");
    }
}


//The recvbuf set to 1mb, but we may need to adapt according to our benchmarking
int initiate_communication(int process_id){
    char sock_path[108];
    struct sockaddr_un addr;
    int fd;

    init_sender_sockets();

    if(mkdir(SOCKET_DIR, 0777) < 0 && errno != EEXIST){
        perror("[ERROR HAPPENED] : Error happened when making the directory for sockets");
        exit(EXIT_FAILURE);
    }

    snprintf(sock_path, sizeof(sock_path), "%s/proc_%d.sock", SOCKET_DIR, process_id);

    unlink(sock_path);

    if((fd = socket(AF_UNIX, SOCK_DGRAM, 0)) < 0){
        perror("[ERROR HAPPENED] : Error happened when creating the socket");
        exit(EXIT_FAILURE);
    }

    int rcvbuf = 1048576; 
    if(setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf)) < 0){
        perror("[ERROR HAPPENED] : Error happened when increasing the socket size");
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, sock_path, sizeof(addr.sun_path) - 1);

    if(bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0){
        perror("[ERROR HAPPENED] : Error happened when binding");
        close(fd);
        exit(EXIT_FAILURE);
    }

    make_nonblocking(fd);

    printf("[SUCCESS] : Process %d initialized on %s\n", process_id, sock_path);
    return fd;
}

int send_msg(int sender_id, int receiver_id, const char *msg){
    (void) sender_id;
    struct sockaddr_un addr;
    char sock_path[108];
    int fd;
    ssize_t n;
    size_t msg_len = strlen(msg) + 1;

    if(msg_len > 65000){
        fprintf(stderr, "[ERROR HAPPENED] : Message size is too large");
        return -1;
    }

    if(sender_sockets[receiver_id] < 0){
        if((fd = socket(AF_UNIX, SOCK_DGRAM, 0)) < 0){
            perror("[ERROR HAPPENED] : Tried to initialize socket when sending a message, but failed");
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

    if(n < 0){
        if(errno == ENOENT){
            return -1;
        }
        if(errno == EAGAIN || errno == EWOULDBLOCK){
            fprintf(stderr, "[ERROR HAPPENED] : Send buffer is full, receiver %d is slow\n", receiver_id);
            return -1;
        }
        perror("[ERROR HAPPENED] : Sending the message failed");
        return -1;
    }
    printf("[SUCCESS] : Process %d send message to Process %d: %s\n", sender_id, receiver_id, msg);
    return 0;
}

int receive_msg(int fd, char *buf, size_t buf_size){
    ssize_t n = recv(fd, buf, buf_size - 1, 0);
    if(n < 0){
        if(errno == EAGAIN || errno == EWOULDBLOCK){
            return 0;
        }
        perror("[ERROR HAPPENED] : When receiving a message");
        return -1;
    }
    buf[n] = '\0';
    return n;
}

void close_communication(int process_id, int fd){
    char sock_path[108];

    for (int i = 0; i < MAX_PROCESSES; i++){
        if(sender_sockets[i] >= 0){
            close(sender_sockets[i]);
            sender_sockets[i] = -1;
        }
    }

    snprintf(sock_path, sizeof(sock_path), "%s/proc_%d.sock", SOCKET_DIR, process_id);
    close(fd);
    unlink(sock_path);

    printf("[SUCCESS] Process %d closed communication\n", process_id);
}

void cleanup_ipc(){
    for (int i = 0; i < MAX_PROCESSES; i++){
        if(sender_sockets[i] >= 0){
            close(sender_sockets[i]);
            sender_sockets[i] = -1;
        }
    }
}

