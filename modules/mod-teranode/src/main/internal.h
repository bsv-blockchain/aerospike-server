/*
 * internal.h
 *
 * Copyright (C) 2024 Aerospike, Inc.
 *
 * Internal utilities for mod-teranode.
 */

#pragma once

#include <stdio.h>
#include <stdarg.h>

#ifdef __cplusplus
extern "C" {
#endif

//==========================================================
// Logging macros - simplified for now.
//

// For now, use printf-based logging
// In production, these would use the Aerospike logging system
#define LOG_ERROR(...) do { fprintf(stderr, "ERROR: "); fprintf(stderr, __VA_ARGS__); fprintf(stderr, "\n"); } while(0)
#define LOG_WARN(...) do { fprintf(stderr, "WARN: "); fprintf(stderr, __VA_ARGS__); fprintf(stderr, "\n"); } while(0)
#define LOG_INFO(...) do { fprintf(stdout, "INFO: "); fprintf(stdout, __VA_ARGS__); fprintf(stdout, "\n"); } while(0)
#define LOG_DEBUG(...) do { fprintf(stdout, "DEBUG: "); fprintf(stdout, __VA_ARGS__); fprintf(stdout, "\n"); } while(0)
#define LOG_TRACE(...) do { fprintf(stdout, "TRACE: "); fprintf(stdout, __VA_ARGS__); fprintf(stdout, "\n"); } while(0)

#ifdef __cplusplus
}
#endif
