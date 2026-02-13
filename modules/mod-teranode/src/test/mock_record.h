/*
 * mock_record.h
 *
 * Copyright (C) 2024 Aerospike, Inc.
 *
 * Mock implementation of as_rec for testing.
 */

#pragma once

#include <aerospike/as_rec.h>
#include <aerospike/as_map.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_string.h>
#include <aerospike/as_aerospike.h>

#ifdef __cplusplus
extern "C" {
#endif

//==========================================================
// Mock record API.
//

/**
 * Create a new mock record.
 */
as_rec* mock_rec_new(void);

/**
 * Destroy a mock record and free all resources.
 */
void mock_rec_destroy(as_rec* rec);

/**
 * Initialize a mock record with UTXO data for testing.
 */
void mock_rec_init_utxos(as_rec* rec, uint32_t num_utxos);

/**
 * Create a mock as_aerospike context for testing.
 * rec_update is a no-op that returns 0 (success).
 */
as_aerospike* mock_aerospike_new(void);

/**
 * Destroy a mock aerospike context.
 */
void mock_aerospike_destroy(as_aerospike* as_ctx);

#ifdef __cplusplus
}
#endif
