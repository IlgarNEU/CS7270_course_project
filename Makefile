CC = gcc
CFLAGS = -Wall -Wextra -O3 -I.
LDFLAGS = -lm

BLOOM_DIR = ./
BLOOM_SRC = $(BLOOM_DIR)/bloom.c
BLOOM_INC = -I$(BLOOM_DIR)

OBJ_IPC = IPC.o
OBJ_BLOOM = bloom.o
OBJ_PROCESS = process.o
OBJ_MANAGER = manager.o

all: manager process

manager: $(OBJ_MANAGER) $(OBJ_IPC)
	$(CC) $(CFLAGS) -o manager $(OBJ_MANAGER) $(OBJ_IPC) $(LDFLAGS)

process: $(OBJ_PROCESS) $(OBJ_IPC) $(OBJ_BLOOM)
	$(CC) $(CFLAGS) -o process $(OBJ_PROCESS) $(OBJ_IPC) $(OBJ_BLOOM) $(LDFLAGS)

manager.o: manager.c IPC.h
	$(CC) $(CFLAGS) -c manager.c

process.o: process.c IPC.h
	$(CC) $(CFLAGS) $(BLOOM_INC) -c process.c

IPC.o: IPC.c IPC.h
	$(CC) $(CFLAGS) -c IPC.c

bloom.o: $(BLOOM_SRC)
	$(CC) $(CFLAGS) $(BLOOM_INC) -c $(BLOOM_SRC) -o bloom.o

clean:
	rm -f *.o manager process
	rm -rf /tmp/distributed_cache_sockets
	rm -f /tmp/bloom_process_*.dat

.PHONY: all clean