/*
 * mod_teranode.h
 *
 * Copyright (C) 2024 Aerospike, Inc.
 *
 * Native TERANODE module for UTXO management.
 */

#pragma once

#include <aerospike/as_module.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

//==========================================================
// Public API.
//

// Global module instance - declared in mod_teranode.c
extern as_module mod_teranode;

// Thread-safe access (following mod_lua pattern)
void mod_teranode_rdlock(void);
void mod_teranode_wrlock(void);
void mod_teranode_unlock(void);

#ifdef __cplusplus
}
#endif
