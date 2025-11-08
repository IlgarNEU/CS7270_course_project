CC = gcc
CFLAGS = -Wall -Wextra -g -I.
LDFLAGS = -lm

# Adjust this path to where your bloom library is located
BLOOM_DIR = ./
BLOOM_SRC = $(BLOOM_DIR)/bloom.c
BLOOM_INC = -I$(BLOOM_DIR)

# Object files
OBJ_IPC = IPC.o
OBJ_BLOOM = bloom.o
OBJ_PROCESS = process.o
OBJ_MANAGER = manager.o

# Targets
all: manager process

# Manager executable
manager: $(OBJ_MANAGER) $(OBJ_IPC)
	$(CC) $(CFLAGS) -o manager $(OBJ_MANAGER) $(OBJ_IPC) $(LDFLAGS)

# Process executable
process: $(OBJ_PROCESS) $(OBJ_IPC) $(OBJ_BLOOM)
	$(CC) $(CFLAGS) -o process $(OBJ_PROCESS) $(OBJ_IPC) $(OBJ_BLOOM) $(LDFLAGS)

# Compile object files
manager.o: manager.c IPC.h
	$(CC) $(CFLAGS) -c manager.c

process.o: process.c IPC.h
	$(CC) $(CFLAGS) $(BLOOM_INC) -c process.c

IPC.o: IPC.c IPC.h
	$(CC) $(CFLAGS) -c IPC.c

bloom.o: $(BLOOM_SRC)
	$(CC) $(CFLAGS) $(BLOOM_INC) -c $(BLOOM_SRC) -o bloom.o

# Clean up
clean:
	rm -f *.o manager process
	rm -rf /tmp/dist_cache_sockets

# Run
run: all
	./manager

# For debugging
debug: CFLAGS += -DDEBUG
debug: clean all

.PHONY: all clean run debug


