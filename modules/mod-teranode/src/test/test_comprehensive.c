/*
 * test_comprehensive.c
 *
 * Copyright (C) 2024 Aerospike, Inc.
 *
 * Comprehensive edge-case and integration tests for mod-teranode.
 * Covers scenarios not already tested in test_spend.c, test_freeze.c,
 * and test_state_management.c.
 */

#include <aerospike/mod_teranode_utxo.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_nil.h>
#include "test_framework.h"
#include "mock_record.h"

//==========================================================
// Spend edge cases.
//

// Spend a frozen UTXO - should return FROZEN error
TEST(spend_frozen_utxo)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);

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
    as_map* freeze_map = as_map_fromval(freeze_result);
    as_val* freeze_status = as_map_get(freeze_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(freeze_status)), "OK");
    as_val_destroy(freeze_result);

    // Now try to spend the frozen UTXO
    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xCC, SPENDING_DATA_SIZE);

    as_arraylist* spend_args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(spend_args, 0);
    as_arraylist_append(spend_args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(spend_args, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(spend_args, (as_val*)as_boolean_new(false));
    as_arraylist_append(spend_args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(spend_args, 1000);
    as_arraylist_append_int64(spend_args, 100);

    as_val* result = teranode_spend(rec, (as_list*)spend_args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "ERROR");

    // Should have errors map with FROZEN error
    as_val* errors_val = as_map_get(result_map, (as_val*)as_string_new((char*)"errors", false));
    ASSERT_NOT_NULL(errors_val);
    as_map* errors = as_map_fromval(errors_val);
    as_val* err0 = as_map_get(errors, (as_val*)as_integer_new(0));
    ASSERT_NOT_NULL(err0);
    as_map* err0_map = as_map_fromval(err0);
    as_val* code = as_map_get(err0_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_FROZEN);

    as_arraylist_destroy(freeze_args);
    as_arraylist_destroy(spend_args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Spend with ignoreConflicting=true on a conflicting record
TEST(spend_ignore_conflicting)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);
    as_rec_set(rec, "conflicting", (as_val*)as_boolean_new(true));

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xDD, SPENDING_DATA_SIZE);

    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(args, (as_val*)as_boolean_new(true));   // ignoreConflicting = true
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
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

// Spend multiple UTXOs at different offsets
TEST(spend_multiple_utxos_sequentially)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    // Spend UTXO at offset 0
    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    uint8_t spending0[SPENDING_DATA_SIZE];
    memset(spending0, 0xAA, SPENDING_DATA_SIZE);

    as_arraylist* args0 = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args0, 0);
    as_arraylist_append(args0, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(args0, (as_val*)as_bytes_new_wrap(spending0, SPENDING_DATA_SIZE, false));
    as_arraylist_append(args0, (as_val*)as_boolean_new(false));
    as_arraylist_append(args0, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args0, 1000);
    as_arraylist_append_int64(args0, 100);

    as_val* result0 = teranode_spend(rec, (as_list*)args0, as_ctx);
    as_map* result_map0 = as_map_fromval(result0);
    as_val* status0 = as_map_get(result_map0, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status0)), "OK");
    as_val_destroy(result0);

    // Re-read utxos for offset 2
    utxos_val = as_rec_get(rec, "utxos");
    utxos = as_list_fromval(utxos_val);
    as_bytes* utxo2 = as_bytes_fromval(as_list_get(utxos, 2));
    uint8_t hash2[UTXO_HASH_SIZE];
    memcpy(hash2, as_bytes_get(utxo2), UTXO_HASH_SIZE);

    uint8_t spending2[SPENDING_DATA_SIZE];
    memset(spending2, 0xBB, SPENDING_DATA_SIZE);

    as_arraylist* args2 = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args2, 2);
    as_arraylist_append(args2, (as_val*)as_bytes_new_wrap(hash2, UTXO_HASH_SIZE, false));
    as_arraylist_append(args2, (as_val*)as_bytes_new_wrap(spending2, SPENDING_DATA_SIZE, false));
    as_arraylist_append(args2, (as_val*)as_boolean_new(false));
    as_arraylist_append(args2, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args2, 1000);
    as_arraylist_append_int64(args2, 100);

    as_val* result2 = teranode_spend(rec, (as_list*)args2, as_ctx);
    as_map* result_map2 = as_map_fromval(result2);
    as_val* status2 = as_map_get(result_map2, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status2)), "OK");
    as_val_destroy(result2);

    // Verify spent count is now 2
    as_val* spent_val = as_rec_get(rec, "spentUtxos");
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 2);

    as_arraylist_destroy(args0);
    as_arraylist_destroy(args2);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Coinbase mature - spending height met
TEST(spend_coinbase_mature)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);
    as_rec_set(rec, "spendingHeight", (as_val*)as_integer_new(500));

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
    as_arraylist_append_int64(args, 1000);  // current height = 1000 >= 500
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

// Spend and then unspend multiple times (round-trip)
TEST(spend_unspend_round_trip)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xAA, SPENDING_DATA_SIZE);

    // Spend
    as_arraylist* spend_args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(spend_args, 0);
    as_arraylist_append(spend_args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(spend_args, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(spend_args, (as_val*)as_boolean_new(false));
    as_arraylist_append(spend_args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(spend_args, 1000);
    as_arraylist_append_int64(spend_args, 100);

    as_val* sr = teranode_spend(rec, (as_list*)spend_args, as_ctx);
    as_val_destroy(sr);

    as_val* spent_val = as_rec_get(rec, "spentUtxos");
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 1);

    // Unspend
    as_arraylist* unspend_args = as_arraylist_new(4, 0);
    as_arraylist_append_int64(unspend_args, 0);
    as_arraylist_append(unspend_args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append_int64(unspend_args, 1000);
    as_arraylist_append_int64(unspend_args, 100);

    as_val* ur = teranode_unspend(rec, (as_list*)unspend_args, as_ctx);
    as_val_destroy(ur);

    spent_val = as_rec_get(rec, "spentUtxos");
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 0);

    // Verify UTXO is back to unspent size
    utxos_val = as_rec_get(rec, "utxos");
    utxos = as_list_fromval(utxos_val);
    as_bytes* utxo_after = as_bytes_fromval(as_list_get(utxos, 0));
    ASSERT_EQ(as_bytes_size(utxo_after), UTXO_HASH_SIZE);

    // Spend again with different data
    uint8_t spending2[SPENDING_DATA_SIZE];
    memset(spending2, 0xBB, SPENDING_DATA_SIZE);

    as_arraylist* spend_args2 = as_arraylist_new(7, 0);
    as_arraylist_append_int64(spend_args2, 0);
    as_arraylist_append(spend_args2, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(spend_args2, (as_val*)as_bytes_new_wrap(spending2, SPENDING_DATA_SIZE, false));
    as_arraylist_append(spend_args2, (as_val*)as_boolean_new(false));
    as_arraylist_append(spend_args2, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(spend_args2, 1000);
    as_arraylist_append_int64(spend_args2, 100);

    as_val* sr2 = teranode_spend(rec, (as_list*)spend_args2, as_ctx);
    as_map* sr2_map = as_map_fromval(sr2);
    as_val* status = as_map_get(sr2_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");
    as_val_destroy(sr2);

    spent_val = as_rec_get(rec, "spentUtxos");
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 1);

    as_arraylist_destroy(spend_args);
    as_arraylist_destroy(unspend_args);
    as_arraylist_destroy(spend_args2);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Unspend an unspent UTXO - should succeed without changes
TEST(unspend_already_unspent)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    as_arraylist* args = as_arraylist_new(4, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_unspend(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    // Should still succeed (no-op for unspent UTXO)
    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    // Spent count should still be 0
    as_val* spent_val = as_rec_get(rec, "spentUtxos");
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 0);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Spend with utxoSpendableIn set (reassigned UTXO not yet spendable)
TEST(spend_frozen_until)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    // Set utxoSpendableIn for offset 0 to height 5000
    as_hashmap* spendable_in = as_hashmap_new(4);
    as_hashmap_set(spendable_in,
        (as_val*)as_integer_new(0),
        (as_val*)as_integer_new(5000));
    as_rec_set(rec, "utxoSpendableIn", (as_val*)spendable_in);

    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xEE, SPENDING_DATA_SIZE);

    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args, 1000);  // current height 1000 < 5000
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_spend(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "ERROR");

    // Check for FROZEN_UNTIL error
    as_val* errors_val = as_map_get(result_map, (as_val*)as_string_new((char*)"errors", false));
    ASSERT_NOT_NULL(errors_val);
    as_map* errors = as_map_fromval(errors_val);
    as_val* err0 = as_map_get(errors, (as_val*)as_integer_new(0));
    ASSERT_NOT_NULL(err0);
    as_map* err0_map = as_map_fromval(err0);
    as_val* code = as_map_get(err0_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_FROZEN_UNTIL);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// Freeze/Unfreeze edge cases.
//

// Freeze then unfreeze then freeze again
TEST(freeze_unfreeze_refreeze)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    // Freeze
    as_arraylist* freeze_args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(freeze_args, 0);
    as_arraylist_append(freeze_args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_val* fr = teranode_freeze(rec, (as_list*)freeze_args, as_ctx);
    as_val_destroy(fr);

    // Unfreeze
    as_arraylist* unfreeze_args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(unfreeze_args, 0);
    as_arraylist_append(unfreeze_args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_val* ufr = teranode_unfreeze(rec, (as_list*)unfreeze_args, as_ctx);
    as_val_destroy(ufr);

    // Verify unspent
    utxos_val = as_rec_get(rec, "utxos");
    utxos = as_list_fromval(utxos_val);
    as_bytes* after_unfreeze = as_bytes_fromval(as_list_get(utxos, 0));
    ASSERT_EQ(as_bytes_size(after_unfreeze), UTXO_HASH_SIZE);

    // Re-freeze
    as_arraylist* refreeze_args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(refreeze_args, 0);
    as_arraylist_append(refreeze_args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_val* rfr = teranode_freeze(rec, (as_list*)refreeze_args, as_ctx);
    as_map* rfr_map = as_map_fromval(rfr);
    as_val* status = as_map_get(rfr_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");
    as_val_destroy(rfr);

    // Verify frozen again
    utxos_val = as_rec_get(rec, "utxos");
    utxos = as_list_fromval(utxos_val);
    as_bytes* after_refreeze = as_bytes_fromval(as_list_get(utxos, 0));
    ASSERT_EQ(as_bytes_size(after_refreeze), FULL_UTXO_SIZE);

    as_arraylist_destroy(freeze_args);
    as_arraylist_destroy(unfreeze_args);
    as_arraylist_destroy(refreeze_args);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Freeze UTXO at non-zero offset
TEST(freeze_non_zero_offset)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo3 = as_bytes_fromval(as_list_get(utxos, 3));
    uint8_t hash3[UTXO_HASH_SIZE];
    memcpy(hash3, as_bytes_get(utxo3), UTXO_HASH_SIZE);

    as_arraylist* args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(args, 3);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash3, UTXO_HASH_SIZE, false));

    as_val* result = teranode_freeze(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    // Verify only UTXO 3 is frozen, others untouched
    utxos_val = as_rec_get(rec, "utxos");
    utxos = as_list_fromval(utxos_val);
    for (uint32_t i = 0; i < 5; i++) {
        as_bytes* u = as_bytes_fromval(as_list_get(utxos, i));
        if (i == 3) {
            ASSERT_EQ(as_bytes_size(u), FULL_UTXO_SIZE);
        } else {
            ASSERT_EQ(as_bytes_size(u), UTXO_HASH_SIZE);
        }
    }

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Freeze with UTXOS_NOT_FOUND (has other bins but no utxos)
TEST(freeze_utxos_not_found)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    as_rec_set(rec, "someOtherBin", (as_val*)as_integer_new(42));

    uint8_t hash[UTXO_HASH_SIZE];
    memset(hash, 0x42, UTXO_HASH_SIZE);

    as_arraylist* args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash, UTXO_HASH_SIZE, false));

    as_val* result = teranode_freeze(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_UTXOS_NOT_FOUND);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// Reassign edge cases.
//

// Reassign then spend the new UTXO once spendable
TEST(reassign_then_spend_after_spendable)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    as_bytes* utxo0 = as_bytes_fromval(as_list_get(utxos, 0));
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(utxo0), UTXO_HASH_SIZE);

    // Freeze first
    as_arraylist* freeze_args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(freeze_args, 0);
    as_arraylist_append(freeze_args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_val* fr = teranode_freeze(rec, (as_list*)freeze_args, as_ctx);
    as_val_destroy(fr);

    // Reassign with spendable_after = 10, blockHeight = 500
    uint8_t new_hash[UTXO_HASH_SIZE];
    memset(new_hash, 0x99, UTXO_HASH_SIZE);

    as_arraylist* reassign_args = as_arraylist_new(5, 0);
    as_arraylist_append_int64(reassign_args, 0);
    as_arraylist_append(reassign_args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(reassign_args, (as_val*)as_bytes_new_wrap(new_hash, UTXO_HASH_SIZE, false));
    as_arraylist_append_int64(reassign_args, 500);
    as_arraylist_append_int64(reassign_args, 10);

    as_val* rr = teranode_reassign(rec, (as_list*)reassign_args, as_ctx);
    as_map* rr_map = as_map_fromval(rr);
    as_val* rr_status = as_map_get(rr_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(rr_status)), "OK");
    as_val_destroy(rr);

    // Verify the new hash is set
    utxos_val = as_rec_get(rec, "utxos");
    utxos = as_list_fromval(utxos_val);
    as_bytes* reassigned = as_bytes_fromval(as_list_get(utxos, 0));
    ASSERT_EQ(as_bytes_size(reassigned), UTXO_HASH_SIZE);
    ASSERT_BYTES_EQ(as_bytes_get(reassigned), new_hash, UTXO_HASH_SIZE);

    // Try to spend at height 500 (< 510) - should fail
    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xDD, SPENDING_DATA_SIZE);

    as_arraylist* spend_args1 = as_arraylist_new(7, 0);
    as_arraylist_append_int64(spend_args1, 0);
    as_arraylist_append(spend_args1, (as_val*)as_bytes_new_wrap(new_hash, UTXO_HASH_SIZE, false));
    as_arraylist_append(spend_args1, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(spend_args1, (as_val*)as_boolean_new(false));
    as_arraylist_append(spend_args1, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(spend_args1, 500);
    as_arraylist_append_int64(spend_args1, 100);

    as_val* sr1 = teranode_spend(rec, (as_list*)spend_args1, as_ctx);
    as_map* sr1_map = as_map_fromval(sr1);
    as_val* sr1_status = as_map_get(sr1_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(sr1_status)), "ERROR");
    as_val_destroy(sr1);

    // Now spend at height 600 (> 510) - should succeed
    as_arraylist* spend_args2 = as_arraylist_new(7, 0);
    as_arraylist_append_int64(spend_args2, 0);
    as_arraylist_append(spend_args2, (as_val*)as_bytes_new_wrap(new_hash, UTXO_HASH_SIZE, false));
    as_arraylist_append(spend_args2, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
    as_arraylist_append(spend_args2, (as_val*)as_boolean_new(false));
    as_arraylist_append(spend_args2, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(spend_args2, 600);
    as_arraylist_append_int64(spend_args2, 100);

    as_val* sr2 = teranode_spend(rec, (as_list*)spend_args2, as_ctx);
    as_map* sr2_map = as_map_fromval(sr2);
    as_val* sr2_status = as_map_get(sr2_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(sr2_status)), "OK");
    as_val_destroy(sr2);

    as_arraylist_destroy(freeze_args);
    as_arraylist_destroy(reassign_args);
    as_arraylist_destroy(spend_args1);
    as_arraylist_destroy(spend_args2);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Reassign on a spent (non-frozen) UTXO - should fail
TEST(reassign_spent_utxo)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);

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

    as_val* sr = teranode_spend(rec, (as_list*)spend_args, as_ctx);
    as_val_destroy(sr);

    // Try to reassign spent UTXO
    uint8_t new_hash[UTXO_HASH_SIZE];
    memset(new_hash, 0x99, UTXO_HASH_SIZE);

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

    as_arraylist_destroy(spend_args);
    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// setMined edge cases.
//

// Add same blockID twice - should be idempotent
TEST(setMined_duplicate_block_id)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);

    int64_t block_id = 11111;

    as_arraylist* args1 = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args1, block_id);
    as_arraylist_append_int64(args1, 500);
    as_arraylist_append_int64(args1, 1);
    as_arraylist_append_int64(args1, 1000);
    as_arraylist_append_int64(args1, 100);
    as_arraylist_append(args1, (as_val*)as_boolean_new(true));
    as_arraylist_append(args1, (as_val*)as_boolean_new(false));

    as_val* r1 = teranode_set_mined(rec, (as_list*)args1, as_ctx);
    as_val_destroy(r1);

    as_arraylist* args2 = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args2, block_id);
    as_arraylist_append_int64(args2, 500);
    as_arraylist_append_int64(args2, 1);
    as_arraylist_append_int64(args2, 1000);
    as_arraylist_append_int64(args2, 100);
    as_arraylist_append(args2, (as_val*)as_boolean_new(true));
    as_arraylist_append(args2, (as_val*)as_boolean_new(false));

    as_val* r2 = teranode_set_mined(rec, (as_list*)args2, as_ctx);
    as_val_destroy(r2);

    as_val* block_ids_val = as_rec_get(rec, "blockIDs");
    ASSERT_NOT_NULL(block_ids_val);
    as_list* block_ids = as_list_fromval(block_ids_val);
    ASSERT_EQ(as_list_size(block_ids), 1);

    as_arraylist_destroy(args1);
    as_arraylist_destroy(args2);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Add multiple different blocks
TEST(setMined_multiple_blocks)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);

    for (int i = 0; i < 3; i++) {
        as_arraylist* args = as_arraylist_new(7, 0);
        as_arraylist_append_int64(args, 10000 + i);
        as_arraylist_append_int64(args, 500 + i);
        as_arraylist_append_int64(args, i);
        as_arraylist_append_int64(args, 1000);
        as_arraylist_append_int64(args, 100);
        as_arraylist_append(args, (as_val*)as_boolean_new(true));
        as_arraylist_append(args, (as_val*)as_boolean_new(false));

        as_val* r = teranode_set_mined(rec, (as_list*)args, as_ctx);
        as_val_destroy(r);
        as_arraylist_destroy(args);
    }

    as_val* block_ids_val = as_rec_get(rec, "blockIDs");
    ASSERT_NOT_NULL(block_ids_val);
    as_list* block_ids = as_list_fromval(block_ids_val);
    ASSERT_EQ(as_list_size(block_ids), 3);

    as_val* heights_val = as_rec_get(rec, "blockHeights");
    ASSERT_NOT_NULL(heights_val);
    as_list* heights = as_list_fromval(heights_val);
    ASSERT_EQ(as_list_size(heights), 3);

    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Remove all blocks sets unminedSince
TEST(setMined_remove_all_blocks_sets_unmined)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);

    int64_t block_id = 33333;

    as_arraylist* args1 = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args1, block_id);
    as_arraylist_append_int64(args1, 500);
    as_arraylist_append_int64(args1, 1);
    as_arraylist_append_int64(args1, 1000);
    as_arraylist_append_int64(args1, 100);
    as_arraylist_append(args1, (as_val*)as_boolean_new(true));
    as_arraylist_append(args1, (as_val*)as_boolean_new(false));

    as_val* r1 = teranode_set_mined(rec, (as_list*)args1, as_ctx);
    as_val_destroy(r1);

    as_arraylist* args2 = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args2, block_id);
    as_arraylist_append_int64(args2, 500);
    as_arraylist_append_int64(args2, 1);
    as_arraylist_append_int64(args2, 2000);
    as_arraylist_append_int64(args2, 100);
    as_arraylist_append(args2, (as_val*)as_boolean_new(true));
    as_arraylist_append(args2, (as_val*)as_boolean_new(true));

    as_val* r2 = teranode_set_mined(rec, (as_list*)args2, as_ctx);
    as_val_destroy(r2);

    as_val* unmined_val = as_rec_get(rec, "unminedSince");
    ASSERT_NOT_NULL(unmined_val);
    ASSERT_EQ(as_integer_get(as_integer_fromval(unmined_val)), 2000);

    as_arraylist_destroy(args1);
    as_arraylist_destroy(args2);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// setLocked edge cases.
//

// Lock and unlock
TEST(setLocked_lock_unlock)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);

    as_arraylist* lock_args = as_arraylist_new(1, 0);
    as_arraylist_append(lock_args, (as_val*)as_boolean_new(true));

    as_val* lr = teranode_set_locked(rec, (as_list*)lock_args, as_ctx);
    as_val_destroy(lr);

    as_val* locked_val = as_rec_get(rec, "locked");
    ASSERT_TRUE(as_boolean_get(as_boolean_fromval(locked_val)));

    as_arraylist* unlock_args = as_arraylist_new(1, 0);
    as_arraylist_append(unlock_args, (as_val*)as_boolean_new(false));

    as_val* ur = teranode_set_locked(rec, (as_list*)unlock_args, as_ctx);
    as_val_destroy(ur);

    locked_val = as_rec_get(rec, "locked");
    ASSERT_FALSE(as_boolean_get(as_boolean_fromval(locked_val)));

    as_arraylist_destroy(lock_args);
    as_arraylist_destroy(unlock_args);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// setLocked returns childCount
TEST(setLocked_returns_child_count)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);
    as_rec_set(rec, "totalExtraRecs", (as_val*)as_integer_new(7));

    as_arraylist* args = as_arraylist_new(1, 0);
    as_arraylist_append(args, (as_val*)as_boolean_new(true));

    as_val* result = teranode_set_locked(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* child_count = as_map_get(result_map, (as_val*)as_string_new((char*)"childCount", false));
    ASSERT_NOT_NULL(child_count);
    ASSERT_EQ(as_integer_get(as_integer_fromval(child_count)), 7);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// setConflicting edge cases.
//

// setConflicting with external triggers deleteAtHeight signal
TEST(setConflicting_with_external_triggers_dah)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);
    as_rec_set(rec, "external", (as_val*)as_boolean_new(true));
    as_rec_set(rec, "totalExtraRecs", (as_val*)as_integer_new(5));

    as_arraylist* args = as_arraylist_new(3, 0);
    as_arraylist_append(args, (as_val*)as_boolean_new(true));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_set_conflicting(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    as_val* signal = as_map_get(result_map, (as_val*)as_string_new((char*)"signal", false));
    ASSERT_NOT_NULL(signal);
    ASSERT_STR_EQ(as_string_get(as_string_fromval(signal)), SIGNAL_DELETE_AT_HEIGHT_SET);

    as_val* dah_val = as_rec_get(rec, "deleteAtHeight");
    ASSERT_NOT_NULL(dah_val);
    ASSERT_EQ(as_integer_get(as_integer_fromval(dah_val)), 1100);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// preserveUntil edge cases.
//

// preserveUntil clears existing deleteAtHeight
TEST(preserveUntil_clears_dah)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);
    as_rec_set(rec, "deleteAtHeight", (as_val*)as_integer_new(5000));

    as_arraylist* args = as_arraylist_new(1, 0);
    as_arraylist_append_int64(args, 10000);

    as_val* result = teranode_preserve_until(rec, (as_list*)args, as_ctx);
    as_val_destroy(result);

    as_val* dah_val = as_rec_get(rec, "deleteAtHeight");
    ASSERT_TRUE(dah_val == NULL || as_val_type(dah_val) == AS_NIL);

    as_val* preserve_val = as_rec_get(rec, "preserveUntil");
    ASSERT_NOT_NULL(preserve_val);
    ASSERT_EQ(as_integer_get(as_integer_fromval(preserve_val)), 10000);

    as_arraylist_destroy(args);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// incrementSpentExtraRecs edge cases.
//

// Increment to exactly equal totalExtraRecs (boundary)
TEST(incrementSpentExtraRecs_exact_boundary)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);
    as_rec_set(rec, "totalExtraRecs", (as_val*)as_integer_new(10));
    as_rec_set(rec, "spentExtraRecs", (as_val*)as_integer_new(8));

    as_arraylist* args = as_arraylist_new(3, 0);
    as_arraylist_append_int64(args, 2);
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_increment_spent_extra_recs(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    as_val* spent_val = as_rec_get(rec, "spentExtraRecs");
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 10);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Decrement to exactly zero (boundary)
TEST(incrementSpentExtraRecs_decrement_to_zero)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);
    as_rec_set(rec, "totalExtraRecs", (as_val*)as_integer_new(10));
    as_rec_set(rec, "spentExtraRecs", (as_val*)as_integer_new(3));

    as_arraylist* args = as_arraylist_new(3, 0);
    as_arraylist_append_int64(args, -3);
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_increment_spent_extra_recs(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    as_val* spent_val = as_rec_get(rec, "spentExtraRecs");
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 0);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Increment with zero delta (no-op)
TEST(incrementSpentExtraRecs_zero_increment)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);
    as_rec_set(rec, "totalExtraRecs", (as_val*)as_integer_new(10));
    as_rec_set(rec, "spentExtraRecs", (as_val*)as_integer_new(5));

    as_arraylist* args = as_arraylist_new(3, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_increment_spent_extra_recs(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    as_val* spent_val = as_rec_get(rec, "spentExtraRecs");
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 5);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// setDeleteAtHeight edge cases.
//

// Zero retention is a no-op
TEST(setDeleteAtHeight_zero_retention)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);
    as_rec_set(rec, "totalExtraRecs", (as_val*)as_integer_new(0));
    as_rec_set(rec, "spentExtraRecs", (as_val*)as_integer_new(0));
    as_rec_set(rec, "spentUtxos", (as_val*)as_integer_new(3));

    as_arraylist* block_ids = as_arraylist_new(1, 0);
    as_arraylist_append_int64(block_ids, 44444);
    as_rec_set(rec, "blockIDs", (as_val*)block_ids);

    as_arraylist* args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 0);

    as_val* result = teranode_set_delete_at_height(rec, (as_list*)args, as_ctx);
    as_val_destroy(result);

    as_val* dah_val = as_rec_get(rec, "deleteAtHeight");
    ASSERT_TRUE(dah_val == NULL || as_val_type(dah_val) == AS_NIL);

    as_arraylist_destroy(args);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Not all spent clears existing dah
TEST(setDeleteAtHeight_not_all_spent_clears_dah)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "totalExtraRecs", (as_val*)as_integer_new(0));
    as_rec_set(rec, "spentExtraRecs", (as_val*)as_integer_new(0));
    as_rec_set(rec, "spentUtxos", (as_val*)as_integer_new(3));
    as_rec_set(rec, "deleteAtHeight", (as_val*)as_integer_new(9999));

    as_arraylist* block_ids = as_arraylist_new(1, 0);
    as_arraylist_append_int64(block_ids, 55555);
    as_rec_set(rec, "blockIDs", (as_val*)block_ids);

    as_arraylist* args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_set_delete_at_height(rec, (as_list*)args, as_ctx);
    as_val_destroy(result);

    as_val* dah_val = as_rec_get(rec, "deleteAtHeight");
    ASSERT_TRUE(dah_val == NULL || as_val_type(dah_val) == AS_NIL);

    as_arraylist_destroy(args);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// Full lifecycle tests.
//

// Lock -> mine -> spend
TEST(full_lifecycle_lock_mine_spend)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);

    // Lock
    as_arraylist* lock_args = as_arraylist_new(1, 0);
    as_arraylist_append(lock_args, (as_val*)as_boolean_new(true));
    as_val* lr = teranode_set_locked(rec, (as_list*)lock_args, as_ctx);
    as_val_destroy(lr);

    as_val* locked_val = as_rec_get(rec, "locked");
    ASSERT_TRUE(as_boolean_get(as_boolean_fromval(locked_val)));

    // Mine (should clear locked)
    as_arraylist* mine_args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(mine_args, 77777);
    as_arraylist_append_int64(mine_args, 500);
    as_arraylist_append_int64(mine_args, 0);
    as_arraylist_append_int64(mine_args, 1000);
    as_arraylist_append_int64(mine_args, 100);
    as_arraylist_append(mine_args, (as_val*)as_boolean_new(true));
    as_arraylist_append(mine_args, (as_val*)as_boolean_new(false));

    as_val* mr = teranode_set_mined(rec, (as_list*)mine_args, as_ctx);
    as_val_destroy(mr);

    locked_val = as_rec_get(rec, "locked");
    ASSERT_FALSE(as_boolean_get(as_boolean_fromval(locked_val)));

    // Spend
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

    as_val* sr = teranode_spend(rec, (as_list*)spend_args, as_ctx);
    as_map* sr_map = as_map_fromval(sr);
    as_val* sr_status = as_map_get(sr_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(sr_status)), "OK");

    // Verify response includes blockIDs
    as_val* block_ids_resp = as_map_get(sr_map, (as_val*)as_string_new((char*)"blockIDs", false));
    ASSERT_NOT_NULL(block_ids_resp);
    as_val_destroy(sr);

    as_arraylist_destroy(lock_args);
    as_arraylist_destroy(mine_args);
    as_arraylist_destroy(spend_args);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Spend all UTXOs triggers deleteAtHeight
TEST(full_lifecycle_spend_all_utxos)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);

    as_rec_set(rec, "totalExtraRecs", (as_val*)as_integer_new(0));
    as_rec_set(rec, "spentExtraRecs", (as_val*)as_integer_new(0));

    // Mine first
    as_arraylist* mine_args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(mine_args, 88888);
    as_arraylist_append_int64(mine_args, 500);
    as_arraylist_append_int64(mine_args, 0);
    as_arraylist_append_int64(mine_args, 1000);
    as_arraylist_append_int64(mine_args, 100);
    as_arraylist_append(mine_args, (as_val*)as_boolean_new(true));
    as_arraylist_append(mine_args, (as_val*)as_boolean_new(false));

    as_val* mr = teranode_set_mined(rec, (as_list*)mine_args, as_ctx);
    as_val_destroy(mr);

    // Spend all 3 UTXOs
    for (int i = 0; i < 3; i++) {
        as_val* utxos_val = as_rec_get(rec, "utxos");
        as_list* utxos = as_list_fromval(utxos_val);
        as_bytes* utxo = as_bytes_fromval(as_list_get(utxos, i));
        uint8_t hash[UTXO_HASH_SIZE];
        memcpy(hash, as_bytes_get(utxo), UTXO_HASH_SIZE);

        uint8_t spending[SPENDING_DATA_SIZE];
        memset(spending, 0xA0 + i, SPENDING_DATA_SIZE);

        as_arraylist* args = as_arraylist_new(7, 0);
        as_arraylist_append_int64(args, i);
        as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash, UTXO_HASH_SIZE, false));
        as_arraylist_append(args, (as_val*)as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false));
        as_arraylist_append(args, (as_val*)as_boolean_new(false));
        as_arraylist_append(args, (as_val*)as_boolean_new(false));
        as_arraylist_append_int64(args, 1000);
        as_arraylist_append_int64(args, 100);

        as_val* sr = teranode_spend(rec, (as_list*)args, as_ctx);
        as_val_destroy(sr);
        as_arraylist_destroy(args);
    }

    as_val* spent_val = as_rec_get(rec, "spentUtxos");
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 3);

    as_val* dah_val = as_rec_get(rec, "deleteAtHeight");
    ASSERT_NOT_NULL(dah_val);
    ASSERT_EQ(as_integer_get(as_integer_fromval(dah_val)), 1100);

    as_arraylist_destroy(mine_args);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// NULL as_ctx tests.
//

TEST(spend_null_as_ctx)
{
    as_rec* rec = mock_rec_new();
    mock_rec_init_utxos(rec, 3);

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

    as_val* result = teranode_spend(rec, (as_list*)args, NULL);
    ASSERT_NOT_NULL(result);
    as_map* result_map = as_map_fromval(result);
    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "ERROR");

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_rec_destroy(rec);
}

TEST(freeze_null_as_ctx)
{
    as_rec* rec = mock_rec_new();
    mock_rec_init_utxos(rec, 3);

    uint8_t hash[UTXO_HASH_SIZE];
    memset(hash, 0x42, UTXO_HASH_SIZE);

    as_arraylist* args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(args, 0);
    as_arraylist_append(args, (as_val*)as_bytes_new_wrap(hash, UTXO_HASH_SIZE, false));

    as_val* result = teranode_freeze(rec, (as_list*)args, NULL);
    ASSERT_NOT_NULL(result);
    as_map* result_map = as_map_fromval(result);
    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "ERROR");

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_rec_destroy(rec);
}

TEST(setMined_null_as_ctx)
{
    as_rec* rec = mock_rec_new();
    mock_rec_init_utxos(rec, 3);

    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, 11111);
    as_arraylist_append_int64(args, 500);
    as_arraylist_append_int64(args, 1);
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);
    as_arraylist_append(args, (as_val*)as_boolean_new(true));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));

    as_val* result = teranode_set_mined(rec, (as_list*)args, NULL);
    ASSERT_NOT_NULL(result);
    as_map* result_map = as_map_fromval(result);
    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "ERROR");

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_rec_destroy(rec);
}

// setMined response includes blockIDs
TEST(setMined_response_includes_block_ids)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 3);

    as_arraylist* args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(args, 12345);
    as_arraylist_append_int64(args, 500);
    as_arraylist_append_int64(args, 1);
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);
    as_arraylist_append(args, (as_val*)as_boolean_new(true));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));

    as_val* result = teranode_set_mined(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* block_ids = as_map_get(result_map, (as_val*)as_string_new((char*)"blockIDs", false));
    ASSERT_NOT_NULL(block_ids);
    as_list* block_ids_list = as_list_fromval(block_ids);
    ASSERT_EQ(as_list_size(block_ids_list), 1);
    as_integer* first_id = as_integer_fromval(as_list_get(block_ids_list, 0));
    ASSERT_EQ(as_integer_get(first_id), 12345);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// Helper to build a spend item map for spend_multi tests.
//

static as_hashmap*
make_spend_item(int64_t offset, uint8_t* hash, uint32_t hash_len,
                uint8_t* spending, uint32_t spending_len)
{
    as_hashmap* item = as_hashmap_new(3);
    as_hashmap_set(item,
        (as_val*)as_string_new((char*)"offset", false),
        (as_val*)as_integer_new(offset));
    as_hashmap_set(item,
        (as_val*)as_string_new((char*)"utxoHash", false),
        (as_val*)as_bytes_new_wrap(hash, hash_len, false));
    as_hashmap_set(item,
        (as_val*)as_string_new((char*)"spendingData", false),
        (as_val*)as_bytes_new_wrap(spending, spending_len, false));
    return item;
}

//==========================================================
// spendMulti() tests.
//

// Single-item batch spend
TEST(spend_multi_single_success)
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
    memset(spending, 0xDD, SPENDING_DATA_SIZE);

    as_arraylist* spends = as_arraylist_new(1, 0);
    as_arraylist_append(spends, (as_val*)make_spend_item(0, hash0, UTXO_HASH_SIZE, spending, SPENDING_DATA_SIZE));

    as_arraylist* args = as_arraylist_new(5, 0);
    as_arraylist_append(args, (as_val*)spends);
    as_arraylist_append(args, (as_val*)as_boolean_new(false));  // ignoreConflicting
    as_arraylist_append(args, (as_val*)as_boolean_new(false));  // ignoreLocked
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_spend_multi(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);
    ASSERT_NOT_NULL(result_map);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    as_val* spent_val = as_rec_get(rec, "spentUtxos");
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 1);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Multi-item batch spend — two different UTXOs
TEST(spend_multi_two_utxos)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);

    uint8_t hash0[UTXO_HASH_SIZE];
    uint8_t hash1[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(as_bytes_fromval(as_list_get(utxos, 0))), UTXO_HASH_SIZE);
    memcpy(hash1, as_bytes_get(as_bytes_fromval(as_list_get(utxos, 1))), UTXO_HASH_SIZE);

    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xCC, SPENDING_DATA_SIZE);

    as_arraylist* spends = as_arraylist_new(2, 0);
    as_arraylist_append(spends, (as_val*)make_spend_item(0, hash0, UTXO_HASH_SIZE, spending, SPENDING_DATA_SIZE));
    as_arraylist_append(spends, (as_val*)make_spend_item(1, hash1, UTXO_HASH_SIZE, spending, SPENDING_DATA_SIZE));

    as_arraylist* args = as_arraylist_new(5, 0);
    as_arraylist_append(args, (as_val*)spends);
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_spend_multi(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    as_val* spent_val = as_rec_get(rec, "spentUtxos");
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 2);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Batch with one valid and one invalid offset — mixed result
TEST(spend_multi_mixed_success_failure)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);

    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(as_bytes_fromval(as_list_get(utxos, 0))), UTXO_HASH_SIZE);

    uint8_t bad_hash[UTXO_HASH_SIZE];
    memset(bad_hash, 0xFF, UTXO_HASH_SIZE);  // Won't match any UTXO

    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xBB, SPENDING_DATA_SIZE);

    as_arraylist* spends = as_arraylist_new(2, 0);
    as_arraylist_append(spends, (as_val*)make_spend_item(0, hash0, UTXO_HASH_SIZE, spending, SPENDING_DATA_SIZE));
    as_arraylist_append(spends, (as_val*)make_spend_item(1, bad_hash, UTXO_HASH_SIZE, spending, SPENDING_DATA_SIZE));

    as_arraylist* args = as_arraylist_new(5, 0);
    as_arraylist_append(args, (as_val*)spends);
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_spend_multi(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    // Should be ERROR because one item failed
    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "ERROR");

    // Should have an errors map with the failed index
    as_val* errors_val = as_map_get(result_map, (as_val*)as_string_new((char*)"errors", false));
    ASSERT_NOT_NULL(errors_val);
    as_map* errors = as_map_fromval(errors_val);
    ASSERT_NOT_NULL(errors);

    // Error for index 1 (the bad hash)
    as_val* err1 = as_map_get(errors, (as_val*)as_integer_new(1));
    ASSERT_NOT_NULL(err1);

    // The valid spend (index 0) should still have been applied
    as_val* spent_val = as_rec_get(rec, "spentUtxos");
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 1);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Batch spend — creating flag blocks entire batch
TEST(spend_multi_creating)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "creating", (as_val*)as_boolean_new(true));

    uint8_t hash[UTXO_HASH_SIZE];
    memset(hash, 0, UTXO_HASH_SIZE);
    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xAA, SPENDING_DATA_SIZE);

    as_arraylist* spends = as_arraylist_new(1, 0);
    as_arraylist_append(spends, (as_val*)make_spend_item(0, hash, UTXO_HASH_SIZE, spending, SPENDING_DATA_SIZE));

    as_arraylist* args = as_arraylist_new(5, 0);
    as_arraylist_append(args, (as_val*)spends);
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_spend_multi(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_CREATING);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Batch spend — tx not found (empty record)
TEST(spend_multi_tx_not_found)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();

    uint8_t hash[UTXO_HASH_SIZE];
    memset(hash, 0, UTXO_HASH_SIZE);
    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xAA, SPENDING_DATA_SIZE);

    as_arraylist* spends = as_arraylist_new(1, 0);
    as_arraylist_append(spends, (as_val*)make_spend_item(0, hash, UTXO_HASH_SIZE, spending, SPENDING_DATA_SIZE));

    as_arraylist* args = as_arraylist_new(5, 0);
    as_arraylist_append(args, (as_val*)spends);
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_spend_multi(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_TX_NOT_FOUND);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Batch spend — locked blocks entire batch
TEST(spend_multi_locked)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "locked", (as_val*)as_boolean_new(true));

    uint8_t hash[UTXO_HASH_SIZE];
    memset(hash, 0, UTXO_HASH_SIZE);
    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xAA, SPENDING_DATA_SIZE);

    as_arraylist* spends = as_arraylist_new(1, 0);
    as_arraylist_append(spends, (as_val*)make_spend_item(0, hash, UTXO_HASH_SIZE, spending, SPENDING_DATA_SIZE));

    as_arraylist* args = as_arraylist_new(5, 0);
    as_arraylist_append(args, (as_val*)spends);
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_spend_multi(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_LOCKED);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Batch spend — already spent with different data produces SPENT error per item
TEST(spend_multi_already_spent)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);

    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(as_bytes_fromval(as_list_get(utxos, 0))), UTXO_HASH_SIZE);

    uint8_t spending1[SPENDING_DATA_SIZE];
    uint8_t spending2[SPENDING_DATA_SIZE];
    memset(spending1, 0xAA, SPENDING_DATA_SIZE);
    memset(spending2, 0xBB, SPENDING_DATA_SIZE);

    // First: spend with spending1 via single spend
    as_arraylist* spend_args = as_arraylist_new(7, 0);
    as_arraylist_append_int64(spend_args, 0);
    as_arraylist_append(spend_args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_arraylist_append(spend_args, (as_val*)as_bytes_new_wrap(spending1, SPENDING_DATA_SIZE, false));
    as_arraylist_append(spend_args, (as_val*)as_boolean_new(false));
    as_arraylist_append(spend_args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(spend_args, 1000);
    as_arraylist_append_int64(spend_args, 100);

    as_val* r1 = teranode_spend(rec, (as_list*)spend_args, as_ctx);
    as_val_destroy(r1);
    as_arraylist_destroy(spend_args);

    // Now try spend_multi with spending2 (different data) on the same UTXO
    as_arraylist* spends = as_arraylist_new(1, 0);
    as_arraylist_append(spends, (as_val*)make_spend_item(0, hash0, UTXO_HASH_SIZE, spending2, SPENDING_DATA_SIZE));

    as_arraylist* args = as_arraylist_new(5, 0);
    as_arraylist_append(args, (as_val*)spends);
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_spend_multi(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "ERROR");

    as_val* errors_val = as_map_get(result_map, (as_val*)as_string_new((char*)"errors", false));
    ASSERT_NOT_NULL(errors_val);

    // Error at index 0 should be SPENT
    as_map* errors = as_map_fromval(errors_val);
    as_val* err0 = as_map_get(errors, (as_val*)as_integer_new(0));
    ASSERT_NOT_NULL(err0);
    as_map* err0_map = as_map_fromval(err0);
    as_val* code = as_map_get(err0_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_SPENT);

    // Should include spendingData hex in error
    as_val* sd_val = as_map_get(err0_map, (as_val*)as_string_new((char*)"spendingData", false));
    ASSERT_NOT_NULL(sd_val);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Batch spend — idempotent same-data respend is OK
TEST(spend_multi_idempotent)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);

    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(as_bytes_fromval(as_list_get(utxos, 0))), UTXO_HASH_SIZE);

    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xDD, SPENDING_DATA_SIZE);

    // First spend via spend_multi
    as_arraylist* spends1 = as_arraylist_new(1, 0);
    as_arraylist_append(spends1, (as_val*)make_spend_item(0, hash0, UTXO_HASH_SIZE, spending, SPENDING_DATA_SIZE));

    as_arraylist* args1 = as_arraylist_new(5, 0);
    as_arraylist_append(args1, (as_val*)spends1);
    as_arraylist_append(args1, (as_val*)as_boolean_new(false));
    as_arraylist_append(args1, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args1, 1000);
    as_arraylist_append_int64(args1, 100);

    as_val* r1 = teranode_spend_multi(rec, (as_list*)args1, as_ctx);
    as_map* r1_map = as_map_fromval(r1);
    as_val* s1 = as_map_get(r1_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(s1)), "OK");
    as_val_destroy(r1);
    as_arraylist_destroy(args1);

    // Same spend again — should be idempotent (OK)
    as_arraylist* spends2 = as_arraylist_new(1, 0);
    as_arraylist_append(spends2, (as_val*)make_spend_item(0, hash0, UTXO_HASH_SIZE, spending, SPENDING_DATA_SIZE));

    as_arraylist* args2 = as_arraylist_new(5, 0);
    as_arraylist_append(args2, (as_val*)spends2);
    as_arraylist_append(args2, (as_val*)as_boolean_new(false));
    as_arraylist_append(args2, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args2, 1000);
    as_arraylist_append_int64(args2, 100);

    as_val* r2 = teranode_spend_multi(rec, (as_list*)args2, as_ctx);
    as_map* r2_map = as_map_fromval(r2);
    as_val* s2 = as_map_get(r2_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(s2)), "OK");

    // Spent count should still be 1 (not incremented again)
    as_val* spent_val = as_rec_get(rec, "spentUtxos");
    ASSERT_EQ(as_integer_get(as_integer_fromval(spent_val)), 1);

    as_arraylist_destroy(args2);
    as_val_destroy(r2);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Batch spend — conflicting blocks entire batch
TEST(spend_multi_conflicting)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);
    as_rec_set(rec, "conflicting", (as_val*)as_boolean_new(true));

    uint8_t hash[UTXO_HASH_SIZE];
    memset(hash, 0, UTXO_HASH_SIZE);
    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xAA, SPENDING_DATA_SIZE);

    as_arraylist* spends = as_arraylist_new(1, 0);
    as_arraylist_append(spends, (as_val*)make_spend_item(0, hash, UTXO_HASH_SIZE, spending, SPENDING_DATA_SIZE));

    as_arraylist* args = as_arraylist_new(5, 0);
    as_arraylist_append(args, (as_val*)spends);
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_spend_multi(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* code = as_map_get(result_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_CONFLICTING);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

// Batch spend — frozen UTXO produces FROZEN per-item error
TEST(spend_multi_frozen_utxo)
{
    as_rec* rec = mock_rec_new();
    as_aerospike* as_ctx = mock_aerospike_new();
    mock_rec_init_utxos(rec, 5);

    as_val* utxos_val = as_rec_get(rec, "utxos");
    as_list* utxos = as_list_fromval(utxos_val);
    uint8_t hash0[UTXO_HASH_SIZE];
    memcpy(hash0, as_bytes_get(as_bytes_fromval(as_list_get(utxos, 0))), UTXO_HASH_SIZE);

    // Freeze UTXO 0
    as_arraylist* freeze_args = as_arraylist_new(2, 0);
    as_arraylist_append_int64(freeze_args, 0);
    as_arraylist_append(freeze_args, (as_val*)as_bytes_new_wrap(hash0, UTXO_HASH_SIZE, false));
    as_val* fr = teranode_freeze(rec, (as_list*)freeze_args, as_ctx);
    as_val_destroy(fr);
    as_arraylist_destroy(freeze_args);

    // Try spend_multi on the frozen UTXO
    uint8_t spending[SPENDING_DATA_SIZE];
    memset(spending, 0xCC, SPENDING_DATA_SIZE);

    as_arraylist* spends = as_arraylist_new(1, 0);
    as_arraylist_append(spends, (as_val*)make_spend_item(0, hash0, UTXO_HASH_SIZE, spending, SPENDING_DATA_SIZE));

    as_arraylist* args = as_arraylist_new(5, 0);
    as_arraylist_append(args, (as_val*)spends);
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append(args, (as_val*)as_boolean_new(false));
    as_arraylist_append_int64(args, 1000);
    as_arraylist_append_int64(args, 100);

    as_val* result = teranode_spend_multi(rec, (as_list*)args, as_ctx);
    as_map* result_map = as_map_fromval(result);

    as_val* status = as_map_get(result_map, (as_val*)as_string_new((char*)"status", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "ERROR");

    as_val* errors_val = as_map_get(result_map, (as_val*)as_string_new((char*)"errors", false));
    ASSERT_NOT_NULL(errors_val);
    as_map* errors = as_map_fromval(errors_val);
    as_val* err0 = as_map_get(errors, (as_val*)as_integer_new(0));
    ASSERT_NOT_NULL(err0);
    as_map* err0_map = as_map_fromval(err0);
    as_val* code = as_map_get(err0_map, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_FROZEN);

    as_arraylist_destroy(args);
    as_val_destroy(result);
    mock_aerospike_destroy(as_ctx);
    mock_rec_destroy(rec);
}

//==========================================================
// Test runner.
//

void run_comprehensive_tests(void)
{
    printf("\n=== Comprehensive Edge-Case Tests ===\n");

    // spendMulti tests
    RUN_TEST(spend_multi_single_success);
    RUN_TEST(spend_multi_two_utxos);
    RUN_TEST(spend_multi_mixed_success_failure);
    RUN_TEST(spend_multi_creating);
    RUN_TEST(spend_multi_tx_not_found);
    RUN_TEST(spend_multi_locked);
    RUN_TEST(spend_multi_already_spent);
    RUN_TEST(spend_multi_idempotent);
    RUN_TEST(spend_multi_conflicting);
    RUN_TEST(spend_multi_frozen_utxo);

    // Spend edge cases
    RUN_TEST(spend_frozen_utxo);
    RUN_TEST(spend_ignore_conflicting);
    RUN_TEST(spend_multiple_utxos_sequentially);
    RUN_TEST(spend_coinbase_mature);
    RUN_TEST(spend_unspend_round_trip);
    RUN_TEST(unspend_already_unspent);
    RUN_TEST(spend_frozen_until);

    // Freeze/unfreeze edge cases
    RUN_TEST(freeze_unfreeze_refreeze);
    RUN_TEST(freeze_non_zero_offset);
    RUN_TEST(freeze_utxos_not_found);

    // Reassign edge cases
    RUN_TEST(reassign_then_spend_after_spendable);
    RUN_TEST(reassign_spent_utxo);

    // setMined edge cases
    RUN_TEST(setMined_duplicate_block_id);
    RUN_TEST(setMined_multiple_blocks);
    RUN_TEST(setMined_remove_all_blocks_sets_unmined);

    // setLocked edge cases
    RUN_TEST(setLocked_lock_unlock);
    RUN_TEST(setLocked_returns_child_count);

    // setConflicting edge cases
    RUN_TEST(setConflicting_with_external_triggers_dah);

    // preserveUntil edge cases
    RUN_TEST(preserveUntil_clears_dah);

    // incrementSpentExtraRecs edge cases
    RUN_TEST(incrementSpentExtraRecs_exact_boundary);
    RUN_TEST(incrementSpentExtraRecs_decrement_to_zero);
    RUN_TEST(incrementSpentExtraRecs_zero_increment);

    // setDeleteAtHeight edge cases
    RUN_TEST(setDeleteAtHeight_zero_retention);
    RUN_TEST(setDeleteAtHeight_not_all_spent_clears_dah);

    // Full lifecycle tests
    RUN_TEST(full_lifecycle_lock_mine_spend);
    RUN_TEST(full_lifecycle_spend_all_utxos);

    // Response field verification
    RUN_TEST(setMined_response_includes_block_ids);

    // NULL as_ctx tests
    RUN_TEST(spend_null_as_ctx);
    RUN_TEST(freeze_null_as_ctx);
    RUN_TEST(setMined_null_as_ctx);
}
