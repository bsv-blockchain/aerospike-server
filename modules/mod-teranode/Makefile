# mod-teranode Makefile
# Builds the native TERANODE module as a shared library

include project/settings.mk

# Common module includes
COMMON = ../common
COMMON_LIB_DIR = $(COMMON)/target/Darwin-arm64/lib
COMMON_LIB = $(COMMON_LIB_DIR)/libaerospike-common.a

# Compiler flags
CC = gcc
CC_FLAGS = -std=gnu99 -g -fPIC -O3
CC_FLAGS += -fno-common -fno-strict-aliasing
CC_FLAGS += -D_FILE_OFFSET_BITS=64 -D_REENTRANT -D_GNU_SOURCE
CC_FLAGS += -Wall -Wextra

# Platform-specific flags
UNAME := $(shell uname)
ifeq ($(UNAME),Darwin)
  CC_FLAGS += -D_DARWIN_UNLIMITED_SELECT
  DYNAMIC_SUFFIX = dylib
  LD_FLAGS = -dynamiclib -undefined dynamic_lookup
else
  CC_FLAGS += -finline-functions -rdynamic
  DYNAMIC_SUFFIX = so
  LD_FLAGS = -shared
endif

# Include paths
INC_PATH = -Isrc/include
INC_PATH += -I$(COMMON)/src/include

# Source files
SOURCES = src/main/mod_teranode.c
SOURCES += src/main/mod_teranode_utxo.c

# Test source files
TEST_SOURCES = src/test/test_main.c
TEST_SOURCES += src/test/test_helpers.c
TEST_SOURCES += src/test/test_spend.c
TEST_SOURCES += src/test/test_freeze.c
TEST_SOURCES += src/test/test_state_management.c
TEST_SOURCES += src/test/test_comprehensive.c
TEST_SOURCES += src/test/test_module.c
TEST_SOURCES += src/test/mock_record.c

# Object files
TARGET_DIR = target
OBJ_DIR = $(TARGET_DIR)/obj
TEST_OBJ_DIR = $(TARGET_DIR)/obj/test
OBJECTS = $(SOURCES:src/main/%.c=$(OBJ_DIR)/%.o)
TEST_OBJECTS = $(TEST_SOURCES:src/test/%.c=$(TEST_OBJ_DIR)/%.o)

# Output library
TARGET_LIB = $(TARGET_DIR)/lib/libmod_teranode.$(DYNAMIC_SUFFIX)
TARGET_TEST = $(TARGET_DIR)/bin/test_teranode

.PHONY: all build clean test run-tests

all: build

build: $(TARGET_LIB)

$(TARGET_LIB): $(OBJECTS)
	@mkdir -p $(dir $@)
	$(CC) $(LD_FLAGS) -o $@ $^
	@echo "Built $@"

$(OBJ_DIR)/%.o: src/main/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CC_FLAGS) $(INC_PATH) -c -o $@ $<

# Test build
$(TARGET_TEST): $(TEST_OBJECTS) $(OBJECTS) $(COMMON_LIB)
	@mkdir -p $(dir $@)
	$(CC) $(CC_FLAGS) $(INC_PATH) -o $@ $(TEST_OBJECTS) $(OBJECTS) $(COMMON_LIB)
	@echo "Built test executable: $@"

$(TEST_OBJ_DIR)/%.o: src/test/%.c
	@mkdir -p $(dir $@)
	$(CC) $(CC_FLAGS) $(INC_PATH) -Isrc/test -c -o $@ $<

test: $(TARGET_TEST)
	@echo ""
	@echo "Running tests..."
	@./$(TARGET_TEST)

run-tests: test

clean:
	@rm -rf $(TARGET_DIR)
	@echo "Cleaned build artifacts"
