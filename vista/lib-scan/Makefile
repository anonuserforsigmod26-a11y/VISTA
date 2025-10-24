# Separate C and C++ compilers
CC = gcc
CXX = g++

# Vista is always enabled (with remapped buffer support)
USE_VISTA = 1
USE_VISTA_REMAP = 1

# Debug mode configuration
ifdef DEBUG
    CFLAGS = -Wall -O0 -g -DDEBUG -DUSE_VISTA_IO -DUSE_VISTA_REMAP
    CXXFLAGS = -Wall -O0 -g -std=c++17 -DDEBUG -DUSE_VISTA_IO -DUSE_VISTA_REMAP
else
    CFLAGS = -Wall -O3 -DUSE_VISTA_IO -DUSE_VISTA_REMAP
    CXXFLAGS = -Wall -O3 -std=c++17 -DUSE_VISTA_IO -DUSE_VISTA_REMAP
endif

# Pure DuckDB path (for building lib-scan)
PURE_DUCKDB = /path/to/your/.__duckdb
DUCKDB_LIB_DIR = $(PURE_DUCKDB)/build/release/src
DUCKDB_INCLUDE = $(PURE_DUCKDB)/src/include

# LDFLAGS with Vista support
LDFLAGS = -L$(DUCKDB_LIB_DIR) -L../libvista -lduckdb -lvista -lpthread -lcjson -lnuma -lstdc++ -Wl,-rpath,$(DUCKDB_LIB_DIR),-rpath,../libvista

# Directories
INCLUDE_DIR = ./include
SRC_DIR = ./src
BUILD_DIR = ./build

# Include paths (always with Vista)
INCLUDES = -I$(INCLUDE_DIR) -I$(DUCKDB_INCLUDE) -I../libvista/include

# C source files (always with Vista)
C_SRC_FILES = $(SRC_DIR)/file_access.c $(SRC_DIR)/tuple_access.c $(SRC_DIR)/table_config.c \
			  $(SRC_DIR)/pg_detoast.c $(SRC_DIR)/vista_wrapper.c \
			  $(SRC_DIR)/vista_init.c $(SRC_DIR)/vista_remapped_buffer_access.c $(SRC_DIR)/vista_heapam_visibility.c \
			  $(SRC_DIR)/vista_libscan_snapshot.c $(SRC_DIR)/vista_ro_slru.c

# C++ source files
CPP_SRC_FILES = $(SRC_DIR)/tuple_to_duckdb.cpp

# Object files (always with Vista)
C_OBJS = $(BUILD_DIR)/file_access.o $(BUILD_DIR)/tuple_access.o $(BUILD_DIR)/table_config.o \
		 $(BUILD_DIR)/pg_detoast.o $(BUILD_DIR)/vista_wrapper.o \
		 $(BUILD_DIR)/vista_init.o $(BUILD_DIR)/vista_remapped_buffer_access.o $(BUILD_DIR)/vista_heapam_visibility.o \
		 $(BUILD_DIR)/vista_libscan_snapshot.o $(BUILD_DIR)/vista_ro_slru.o

CPP_OBJS = $(BUILD_DIR)/tuple_to_duckdb.o $(BUILD_DIR)/libscan_c_api.o

# Targets
TARGET_LIB = libscan.so

# Default target - build optimized libscan.so with Vista
default: $(BUILD_DIR) $(TARGET_LIB)

# Debug mode - build debug libscan.so with Vista
debug:
	$(MAKE) DEBUG=1

# Create build directory
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# Shared library for DuckDB integration
$(TARGET_LIB): $(C_OBJS) $(CPP_OBJS)
	$(CXX) $(CXXFLAGS) -shared -fPIC -o $@ $^ $(LDFLAGS)

# C object files (with -fPIC for shared library)
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c | $(BUILD_DIR)
	$(CC) $(CFLAGS) -fPIC $(INCLUDES) -c -o $@ $<

# C++ object files (with -fPIC for shared library)
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.cpp | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -fPIC $(INCLUDES) -c -o $@ $<

# Clean target
clean:
	rm -rf $(BUILD_DIR) $(TARGET_LIB)

# Help target
help:
	@echo "Build modes:"
	@echo "  make       - Build libscan.so (Vista I/O + remapped buffer, optimized)"
	@echo "  make debug - Build libscan.so (Vista I/O + remapped buffer, debug)"
	@echo "  make clean - Remove build files"

.PHONY: default debug clean help
