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
#define MAX_MSG_LEN 256

static void make_nonblocking(int fd){
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int initiate_communication(int process_id) {
    char sock_path[108];
    struct sockaddr_un addr;
    int fd;

    if(mkdir(SOCKET_DIR, 0777) <0 && errno != EEXIST){
	perror("mdir");
	exit(EXIT_FAILURE);	
    }  // create dir if missing
    printf("DEBUG Created: %s\n", SOCKET_DIR);
    snprintf(sock_path, sizeof(sock_path), "%s/proc_%d.sock", SOCKET_DIR, process_id);

    // remove any existing socket file
    unlink(sock_path);

    if ((fd = socket(AF_UNIX, SOCK_DGRAM, 0)) < 0) {
        perror("socket");
        exit(EXIT_FAILURE);
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
    return fd;
}

int send_msg(int sender_id, int receiver_id, const char *msg){
    struct sockaddr_un addr;
    char sock_path[108];
    int fd;
    ssize_t n;

    if((fd = socket(AF_UNIX, SOCK_DGRAM, 0)) < 0){
        perror("socket (send)");
        return -1;
    }

    snprintf(sock_path, sizeof(sock_path), "%s/proc_%d.sock", SOCKET_DIR, receiver_id);
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, sock_path, sizeof(addr.sun_path) - 1);

    n = sendto(fd, msg, strlen(msg) + 1, 0, (struct sockaddr*)&addr, sizeof(addr));
    if (n < 0) {
        if (errno != ENOENT) perror("sendto");
        close(fd);
        return -1;
    }
    printf("DEBUG - Sending from %d to %d: %s\n", sender_id, receiver_id, msg);
    close(fd);
    return 0;
}


int receive_msg(int fd, char *buf, size_t buf_size) {
    ssize_t n = recv(fd, buf, buf_size - 1, 0);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0; // no message
        perror("recv");
        return -1;
    }
    printf("[DEBUG] Received message: %s\n", buf);
    buf[n] = '\0';
    return n;
}


void close_communication(int process_id, int fd) {
    char sock_path[108];
    snprintf(sock_path, sizeof(sock_path), "%s/proc_%d.sock", SOCKET_DIR, process_id);
    close(fd);
    unlink(sock_path);
}
