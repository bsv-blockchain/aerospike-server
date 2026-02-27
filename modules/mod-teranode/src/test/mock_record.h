/*
 * mock_record.h
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
