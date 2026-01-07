# mod-teranode settings

# Project directories
SOURCE_INCL = src/include
SOURCE_MAIN = src/main
SOURCE_TEST = src/test

# Target directories
TARGET = target
TARGET_BASE = $(TARGET)
TARGET_BIN = $(TARGET)/bin
TARGET_DOC = $(TARGET)/doc
TARGET_LIB = $(TARGET)/lib
TARGET_OBJ = $(TARGET)/obj

# Build type
ifndef O
  O = 3
endif
