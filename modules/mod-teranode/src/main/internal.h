/*
 * internal.h
 *
 * Copyright (C) 2026 BSV Association.
 *
 * This program is free software: you can redistribute it and/or modify it under
 * the terms of the GNU Affero General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/
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
