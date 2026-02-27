/*
 * test_spend.c
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
 * Tests for spend/unspend functions.
 */

#include <aerospike/mod_teranode_utxo.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_hashmap.h>
#include "test_framework.h"
#include "mock_record.h"

//==========================================================
// spend() tests.
//

TEST(spend_success)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    // Get the UTXO hash we want to spend
    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    // Create spending data
    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xEE, SPENDING_DATA_SIZE);

    // Build args
    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, 0);  // offset
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));  // ignoreConflicting
    as_arraylist_append(args, (as_val*)as_boolean_new(false));  // ignoreLocked
    as_arraylist_append_int64(args, 1000);  // currentBlockHeight
    as_arraylist_append_int64(args, 100);   // blockHeightRetention

    // Call spend
    as_val* result = teranode_spend(rec, (as_list*)args, as_ctx);

    ASSERT_NOT_NULL(result);
    as_map* result_map = as_map_fromval(result);
    ASSERT_NOT_NULL(result_map);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    // Verify spent count incremented
    as_val* spent_val = as_rec_get(rec, "spentUtxos");
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 1);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(spend_utxos_not_found)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    // Create record with some bins but no utxos bin
    as_rec_set(rec, "someOtherBin", (as_val*)as_integer_new(42));

    uint8_t* hash = (uint8_t*)malloc(UTXO_HASH_SIZE);
    uint8_t* spending = (uint8_t*)malloc(SPENDING_DATA_SIZE);
    memset(hash, 0x42, UTXO_HASH_SIZE);
    memset(spending, 0xEE, SPENDING_DATA_SIZE);

    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash, UTXO_HASH_SIZE, true));
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, true));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_spend(rec, (as_list*)args, as_ctx);
    ASSERT_NOT_NULL(result);
    as_map* result_map = as_map_fromval(result);
    ASSERT_NOT_NULL(result_map);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "ERROR");

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_UTXOS_NOT_FOUND);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(spend_locked)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "locked", (as_val*)as_boolean_new(true));

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xEE, SPENDING_DATA_SIZE);

    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));  // ignoreConflicting
    as_arraylist_append(args, (as_val*)as_boolean_new(false));  // ignoreLocked = false
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_spend(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "ERROR");

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_LOCKED);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(spend_conflicting)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "conflicting", (as_val*)as_boolean_new(true));

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xEE, SPENDING_DATA_SIZE);

    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_spend(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_CONFLICTING);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(spend_creating)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "creating", (as_val*)as_boolean_new(true));

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xEE, SPENDING_DATA_SIZE);

    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_spend(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_CREATING);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(spend_ignore_locked)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "locked", (as_val*)as_boolean_new(true));

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xEE, SPENDING_DATA_SIZE);

    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append(args, (as_val*)as_boolean_new(true));   // ignoreLocked = true
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_spend(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(spend_coinbase_immature)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "spendingHeight", (as_val*)as_integer_new(2000));  // Can't spend until 2000

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xEE, SPENDING_DATA_SIZE);

    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args, 1000);  // Current height = 1000 (< 2000)
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_spend(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_COINBASE_IMMATURE);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(spend_already_spent_same_data)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xEE, SPENDING_DATA_SIZE);

    // First spend - should succeed
    as_arraylist* args1 = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args1, 0);
    as_arraylist_append(args1, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(args1, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(args1, (as_val*)as_boolean_new(false));
    as_arraylist_append(args1, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args1, 1000);
    as_arraylist_append_int64(args1, 100);

    as_val* result1 = teranode_spend(rec, (as_list*)args1, as_ctx);
    as_map* result_map1 = as_map_fromval(result1);
    as_val* status1 = as_map_get(result_map1, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status1)), "OK");

    // Second spend with same data - should succeed (idempotent)
    as_arraylist* args2 = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args2, 0);
    as_arraylist_append(args2, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(args2, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(args2, (as_val*)as_boolean_new(false));
    as_arraylist_append(args2, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args2, 1000);
    as_arraylist_append_int64(args2, 100);

    as_val* result2 = teranode_spend(rec, (as_list*)args2, as_ctx);
    as_map* result_map2 = as_map_fromval(result2);
    as_val* status2 = as_map_get(result_map2, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status2)), "OK");

    as_arraylist_destroy(args1);
    as_arraylist_destroy(args2);
    as_val_destroy(result1);
    as_val_destroy(result2);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(spend_already_spent_different_data)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    uint8_t spending1[SPENDING_DATA_SIZE];
    uint8_t spending2[SPENDING_DATA_SIZE];
    memset(spending1, 0xEE, SPENDING_DATA_SIZE);
    memset(spending2, 0xFF, SPENDING_DATA_SIZE);

    // First spend
    as_arraylist* args1 = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args1, 0);
    as_arraylist_append(args1, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(args1, (as_val*)as_bytes_new_wrap(spending1, SPENDING_DATA_SIZE, false));
    as_arraylist_append(args1, (as_val*)as_boolean_new(false));
    as_arraylist_append(args1, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args1, 1000);
    as_arraylist_append_int64(args1, 100);

    as_val* result1 = teranode_spend(rec, (as_list*)args1, as_ctx);
    as_map* result_map1 = as_map_fromval(result1);
    as_val* status1 = as_map_get(result_map1, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status1)), "OK");

    // Second spend with different data - should fail
    as_arraylist* args2 = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args2, 0);
    as_arraylist_append(args2, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(args2, (as_val*)as_bytes_new_wrap(spending2, SPENDING_DATA_SIZE, false));
    as_arraylist_append(args2, (as_val*)as_boolean_new(false));
    as_arraylist_append(args2, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args2, 1000);
    as_arraylist_append_int64(args2, 100);

    as_val* result2 = teranode_spend(rec, (as_list*)args2, as_ctx);
    as_map* result_map2 = as_map_fromval(result2);

    as_val* status2 = as_map_get(result_map2, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status2)), "ERROR");

    // Should have errors map
    as_val* errors_val = as_map_get(result_map2, (as_val*)as_string_new((char*)"errors", false));
    ASSERT_NOT_NULL(errors_val);

    as_arraylist_destroy(args1);
    as_arraylist_destroy(args2);
    as_val_destroy(result1);
    as_val_destroy(result2);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// unspend() tests.
//

TEST(unspend_success)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    // First spend a UTXO
    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xEE, SPENDING_DATA_SIZE);

    as_arraylist* spend_args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(spend_args, 0);
    as_arraylist_append(spend_args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(spend_args, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(spend_args, (as_val*)as_boolean_new(false));
    as_arraylist_append(spend_args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(spend_args, 1000);
    as_arraylist_append_int64(spend_args, 100);

    as_val* spend_result = teranode_spend(rec, (as_list*)spend_args, as_ctx);
    as_val_destroy(spend_result);

    // Now unspend
    as_arraylist* unspend_args = as_arraylist_new(4, 0);
    as_arraylist_append_int64(unspend_args, 0);
    as_arraylist_append(unspend_args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append_int64(unspend_args, 1000);
    as_arraylist_append_int64(unspend_args, 100);

    as_val* result = teranode_unspend(rec, (as_list*)unspend_args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    // Verify spent count decremented
    as_val* spent_val = as_rec_get(rec, "spentUtxos");
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 0);

    as_arraylist_destroy(spend_args);
    as_arraylist_destroy(unspend_args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(unspend_frozen)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    // Freeze the UTXO first
    as_arraylist* freeze_args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(freeze_args, 0);
    as_arraylist_append(freeze_args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));

    as_val* freeze_result = teranode_freeze(rec, (as_list*)freeze_args, as_ctx);
    as_val_destroy(freeze_result);

    // Try to unspend frozen UTXO - should fail
    as_arraylist* unspend_args = as_arraylist_new(4, 0);
    as_arraylist_append_int64(unspend_args, 0);
    as_arraylist_append(unspend_args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append_int64(unspend_args, 1000);
    as_arraylist_append_int64(unspend_args, 100);

    as_val* result = teranode_unspend(rec, (as_list*)unspend_args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_FROZEN);

    as_arraylist_destroy(freeze_args);
    as_arraylist_destroy(unspend_args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// Record existence tests.
//

TEST(spend_tx_not_found)
{
    // Empty record (no bins) should return TX_NOT_FOUND
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    // Don't initialize anything - record has no bins

    uint8_t hash[UTXO_HASH_SIZE];
    uint8_t spending[SPENDING_DATA_SIZE];
    memset(hash, 0x42, UTXO_HASH_SIZE);
    memset(spending, 0xEE, SPENDING_DATA_SIZE);

    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash, UTXO_HASH_SIZE, false));
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_spend(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "ERROR");

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_TX_NOT_FOUND);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(unspend_tx_not_found)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    // Don't initialize anything

    uint8_t hash[UTXO_HASH_SIZE];
    memset(hash, 0x42, UTXO_HASH_SIZE);

    as_arraylist* args = as_arraylist_new(4, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash, UTXO_HASH_SIZE, false));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_unspend(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_TX_NOT_FOUND);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// deletedChildren tests.
//

TEST(spend_deleted_child_tx)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    // Create spending data with a specific txid pattern
    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xAB, SPENDING_DATA_SIZE);  // txid will be "abab...abab" reversed

    // First spend - should succeed
    as_arraylist* args1 = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args1, 0);
    as_arraylist_append(args1, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(args1, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(args1, (as_val*)as_boolean_new(false));
    as_arraylist_append(args1, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args1, 1000);
    as_arraylist_append_int64(args1, 100);

    as_val* result1 = teranode_spend(rec, (as_list*)args1, as_ctx);
    as_map* result_map1 = as_map_fromval(result1);
    as_val* status1 = as_map_get(result_map1, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status1)), "OK");
    as_val_destroy(result1);

    // Now set up deletedChildren map with this txid
    // The txid is first 32 bytes reversed - so "ababab...ab" becomes txid hex
    as_hashmap* deleted_children = as_hashmap_new(4);
    // The txid in hex (32 bytes 0xAB reversed = "abababab..." 64 chars)
    char txid_hex[65];
    for (int i = 0; i < 64; i++) {
        txid_hex[i] = (i % 2 == 0) ? 'a' : 'b';
    }
    txid_hex[64] = '\0';
    as_hashmap_set(deleted_children,
        (as_val*)as_string_new(txid_hex, false),
        (as_val*)as_boolean_new(true));
    as_rec_set(rec, "deletedChildren", (as_val*)deleted_children);

    // Second spend with same data - should fail because child was deleted
    as_arraylist* args2 = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args2, 0);
    as_arraylist_append(args2, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(args2, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(args2, (as_val*)as_boolean_new(false));
    as_arraylist_append(args2, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args2, 1000);
    as_arraylist_append_int64(args2, 100);

    as_val* result2 = teranode_spend(rec, (as_list*)args2, as_ctx);
    as_map* result_map2 = as_map_fromval(result2);

    as_val* status2 = as_map_get(result_map2, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status2)), "ERROR");

    // Check for INVALID_SPEND error in the errors map
    as_val* errors_val = as_map_get(result_map2, (as_val*)as_string_new((char*)"errors", false));
    ASSERT_NOT_NULL(errors_val);
    as_map* errors = as_map_fromval(errors_val);
    ASSERT_NOT_NULL(errors);

    // Get error for idx 0
    as_val* err0_val = as_map_get(errors, (as_val*)as_integer_new(0));
    ASSERT_NOT_NULL(err0_val);
    as_map* err0 = as_map_fromval(err0_val);

    as_val* code = as_map_get(err0, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_INVALID_SPEND);

    as_arraylist_destroy(args1);
    as_arraylist_destroy(args2);
    as_val_destroy(result2);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

void run_spend_tests(void)
{
    printf("\n=== Spend/Unspend Tests ===\n");

    RUN_TEST(spend_success);
    RUN_TEST(spend_utxos_not_found);
    RUN_TEST(spend_locked);
    RUN_TEST(spend_conflicting);
    RUN_TEST(spend_creating);
    RUN_TEST(spend_ignore_locked);
    RUN_TEST(spend_coinbase_immature);
    RUN_TEST(spend_already_spent_same_data);
    RUN_TEST(spend_already_spent_different_data);

    RUN_TEST(unspend_success);
    RUN_TEST(unspend_frozen);

    // Record existence tests
    RUN_TEST(spend_tx_not_found);
    RUN_TEST(unspend_tx_not_found);

    // deletedChildren tests
    RUN_TEST(spend_deleted_child_tx);
}

TEST(spend_args_destroy_after_call_and_persist)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    // Prepare utxo hash and spending data
    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0x5A, SPENDING_DATA_SIZE);

    // Build args and call
    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_spend(rec, (as_list*)args, as_ctx);
    // Destroy args and result to test ownership safety
    as_arraylist_destroy(args);
    as_val_destroy(result);

    // Verify utxos persisted correctly
    utxos_val = as_rec_get(rec, "utxos");
    utxos = as_list_fromval(utxos_val);
    as_bytes* utxo_after = as_bytes_fromval(as_list_get(utxos, 0));
    ASSERT_EQ(as_bytes_size(utxo_after), FULL_UTXO_SIZE);

    // Verify spentUtxos incremented
    as_val* spent_val = as_rec_get(rec, "spentUtxos");
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 1);

    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

void run_additional_spend_tests(void)
{
    RUN_TEST(spend_args_destroy_after_call_and_persist);
}
