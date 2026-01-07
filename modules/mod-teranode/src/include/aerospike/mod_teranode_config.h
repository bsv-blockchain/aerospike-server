/*
 * mod_teranode_config.h
 *
 * Copyright (C) 2024 Aerospike, Inc.
 *
 * Configuration for TERANODE module.
 */

#pragma once

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct mod_teranode_config_s {
    bool    server_mode;      // true when running in server (vs client)
    bool    enabled;          // module enabled/disabled
    char    user_path[256];   // path to module files (for future use)
} mod_teranode_config;

#ifdef __cplusplus
}
#endif
