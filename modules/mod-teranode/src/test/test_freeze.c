/*
 * test_freeze.c
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
 * Tests for freeze/unfreeze/reassign functions.
 */

#include <aerospike/mod_teranode_utxo.h>
#include <aerospike/as_arraylist.h>
#include "test_framework.h"
#include "mock_record.h"

//==========================================================
// freeze() tests.
//

TEST(freeze_success)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    as_arraylist* args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));

    as_val* result = teranode_freeze(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    // Verify UTXO is now frozen (size should be 68)
    utxos_val = as_rec_get(rec, "utxos");
    utxos = as_list_fromval(utxos_val);
    as_bytes* frozen_utxo = as_bytes_fromval(as_list_get(utxos, 0));
    ASSERT_EQ(as_bytes_size(frozen_utxo), FULL_UTXO_SIZE);

    // Verify spending data is all 255
    const uint8_t* data = as_bytes_get(frozen_utxo);
    for (uint32_t i = UTXO_HASH_SIZE; i < FULL_UTXO_SIZE; i++) {
        ASSERT_EQ(data[i], FROZEN_BYTE);
    }

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(freeze_already_frozen)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    // First freeze
    as_arraylist* args1 = as_arraylist_new(2, 0);
    as_arraylist_append_int64(args1, 0);
    as_arraylist_append(args1, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));

    as_val* result1 = teranode_freeze(rec, (as_list*)args1, as_ctx);
    as_val_destroy(result1);

    // Try to freeze again - should fail
    as_arraylist* args2 = as_arraylist_new(2, 0);
    as_arraylist_append_int64(args2, 0);
    as_arraylist_append(args2, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));

    as_val* result2 = teranode_freeze(rec, (as_list*)args2, as_ctx);
    as_map* result_map = as_map_fromval(result2);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_ALREADY_FROZEN);

    as_arraylist_destroy(args1);
    as_arraylist_destroy(args2);
    as_val_destroy(result2);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(freeze_already_spent)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    // Spend the UTXO first
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

    // Try to freeze spent UTXO - should fail
    as_arraylist* freeze_args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(freeze_args, 0);
    as_arraylist_append(freeze_args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));

    as_val* result = teranode_freeze(rec, (as_list*)freeze_args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_SPENT);

    as_arraylist_destroy(spend_args);
    as_arraylist_destroy(freeze_args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// unfreeze() tests.
//

TEST(unfreeze_success)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    // Freeze first
    as_arraylist* freeze_args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(freeze_args, 0);
    as_arraylist_append(freeze_args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));

    as_val* freeze_result = teranode_freeze(rec, (as_list*)freeze_args, as_ctx);
    as_val_destroy(freeze_result);

    // Now unfreeze
    as_arraylist* unfreeze_args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(unfreeze_args, 0);
    as_arraylist_append(unfreeze_args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));

    as_val* result = teranode_unfreeze(rec, (as_list*)unfreeze_args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    // Verify UTXO is back to unspent size
    utxos_val = as_rec_get(rec, "utxos");
    utxos = as_list_fromval(utxos_val);
    as_bytes* unfrozen_utxo = as_bytes_fromval(as_list_get(utxos, 0));
    ASSERT_EQ(as_bytes_size(unfrozen_utxo), UTXO_HASH_SIZE);

    as_arraylist_destroy(freeze_args);
    as_arraylist_destroy(unfreeze_args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(unfreeze_not_frozen)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    // Try to unfreeze without freezing first
    as_arraylist* args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));

    as_val* result = teranode_unfreeze(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_UTXO_NOT_FROZEN);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// reassign() tests.
//

TEST(reassign_success)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    // Freeze first (reassignment requires frozen UTXO)
    as_arraylist* freeze_args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(freeze_args, 0);
    as_arraylist_append(freeze_args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));

    as_val* freeze_result = teranode_freeze(rec, (as_list*)freeze_args, as_ctx);
    as_val_destroy(freeze_result);

    // Create new hash
    uint8_t new_hash[UTXO_HASH_SIZE];
    memset(new_hash, 0x99, UTXO_HASH_SIZE);

    // Reassign
    as_arraylist* args = as_arraylist_new(5, 0);
    as_arraylist_append_int64(args, 0);  // offset
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(new_hash, UTXO_HASH_SIZE, false));
    as_arraylist_append_int64(args, 1000);  // blockHeight
    as_arraylist_append_int64(args, 100);   // spendableAfter

    as_val* result = teranode_reassign(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    // Verify UTXO hash changed
    utxos_val = as_rec_get(rec, "utxos");
    utxos = as_list_fromval(utxos_val);
    as_bytes* reassigned_utxo = as_bytes_fromval(as_list_get(utxos, 0));
    ASSERT_BYTES_EQ(as_bytes_get(reassigned_utxo), new_hash, UTXO_HASH_SIZE);

    // Verify reassignments list created
    as_val* reassignments_val = as_rec_get(rec, "reassignments");
    ASSERT_NOT_NULL(reassignments_val);

    // Verify utxoSpendableIn set
    as_val* spendable_in_val = as_rec_get(rec, "utxoSpendableIn");
    ASSERT_NOT_NULL(spendable_in_val);

    // Verify recordUtxos incremented
    as_val* record_utxos_val = as_rec_get(rec, "recordUtxos");
    ASSERT_EQ(as_integer_get(as_integer_fromval(record_utxos_val)), 6);  // Was 5, now 6

    as_arraylist_destroy(freeze_args);
    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(reassign_not_frozen)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    uint8_t new_hash[UTXO_HASH_SIZE];
    memset(new_hash, 0x99, UTXO_HASH_SIZE);

    // Try to reassign without freezing
    as_arraylist* args = as_arraylist_new(5, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(new_hash, UTXO_HASH_SIZE, false));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_reassign(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_UTXO_NOT_FROZEN);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// Record existence tests.
//

TEST(freeze_tx_not_found)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    // Don't initialize anything

    uint8_t hash[UTXO_HASH_SIZE];
    memset(hash, 0x42, UTXO_HASH_SIZE);

    as_arraylist* args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash, UTXO_HASH_SIZE, false));

    as_val* result = teranode_freeze(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_TX_NOT_FOUND);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(unfreeze_tx_not_found)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    // Don't initialize anything

    uint8_t hash[UTXO_HASH_SIZE];
    memset(hash, 0x42, UTXO_HASH_SIZE);

    as_arraylist* args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash, UTXO_HASH_SIZE, false));

    as_val* result = teranode_unfreeze(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_TX_NOT_FOUND);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

TEST(reassign_tx_not_found)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    // Don't initialize anything

    uint8_t hash[UTXO_HASH_SIZE];
    uint8_t new_hash[UTXO_HASH_SIZE];
    memset(hash, 0x42, UTXO_HASH_SIZE);
    memset(new_hash, 0x99, UTXO_HASH_SIZE);

    as_arraylist* args = as_arraylist_new(5, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash, UTXO_HASH_SIZE, false));
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(new_hash, UTXO_HASH_SIZE, false));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_reassign(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_TX_NOT_FOUND);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

void run_freeze_tests(void)
{
    printf("\n=== Freeze/Unfreeze/Reassign Tests ===\n");

    RUN_TEST(freeze_success);
    RUN_TEST(freeze_already_frozen);
    RUN_TEST(freeze_already_spent);

    RUN_TEST(unfreeze_success);
    RUN_TEST(unfreeze_not_frozen);

    RUN_TEST(reassign_success);
    RUN_TEST(reassign_not_frozen);

    // Record existence tests
    RUN_TEST(freeze_tx_not_found);
    RUN_TEST(unfreeze_tx_not_found);
    RUN_TEST(reassign_tx_not_found);
}
