/*
 * test_helpers.c
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
 * Tests for helper functions in mod_teranode_utxo.
 */

#include <aerospike/mod_teranode_utxo.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_arraylist.h>
#include "test_framework.h"
#include "mock_record.h"

//==========================================================
// Helper function tests.
//

TEST(bytes_equal_same)
{
    uint8_t data[5] = {1, 2, 3, 4, 5};
    as_bytes* a = as_bytes_new_wrap(data, 5, false);
    as_bytes* b = as_bytes_new_wrap(data, 5, false);

    ASSERT_TRUE(utxo_bytes_equal(a, b));

    as_bytes_destroy(a);
    as_bytes_destroy(b);
}

TEST(bytes_equal_different)
{
    uint8_t data1[5] = {1, 2, 3, 4, 5};
    uint8_t data2[5] = {1, 2, 3, 4, 6};
    as_bytes* a = as_bytes_new_wrap(data1, 5, false);
    as_bytes* b = as_bytes_new_wrap(data2, 5, false);

    ASSERT_FALSE(utxo_bytes_equal(a, b));

    as_bytes_destroy(a);
    as_bytes_destroy(b);
}

TEST(bytes_equal_different_size)
{
    uint8_t data1[5] = {1, 2, 3, 4, 5};
    uint8_t data2[3] = {1, 2, 3};
    as_bytes* a = as_bytes_new_wrap(data1, 5, false);
    as_bytes* b = as_bytes_new_wrap(data2, 3, false);

    ASSERT_FALSE(utxo_bytes_equal(a, b));

    as_bytes_destroy(a);
    as_bytes_destroy(b);
}

TEST(bytes_equal_null_both)
{
    ASSERT_TRUE(utxo_bytes_equal(NULL, NULL));
}

TEST(bytes_equal_null_one)
{
    uint8_t data[5] = {1, 2, 3, 4, 5};
    as_bytes* a = as_bytes_new_wrap(data, 5, false);

    ASSERT_FALSE(utxo_bytes_equal(a, NULL));
    ASSERT_FALSE(utxo_bytes_equal(NULL, a));

    as_bytes_destroy(a);
}

TEST(is_frozen_true)
{
    uint8_t frozen[SPENDING_DATA_SIZE];
    memset(frozen, FROZEN_BYTE, SPENDING_DATA_SIZE);

    ASSERT_TRUE(utxo_is_frozen(frozen));
}

TEST(is_frozen_false)
{
    uint8_t not_frozen[SPENDING_DATA_SIZE];
    memset(not_frozen, 0, SPENDING_DATA_SIZE);

    ASSERT_FALSE(utxo_is_frozen(not_frozen));
}

TEST(is_frozen_partial)
{
    uint8_t partial[SPENDING_DATA_SIZE];
    memset(partial, FROZEN_BYTE, SPENDING_DATA_SIZE);
    partial[10] = 0;  // One byte different

    ASSERT_FALSE(utxo_is_frozen(partial));
}

TEST(is_frozen_null)
{
    ASSERT_FALSE(utxo_is_frozen(NULL));
}

TEST(create_utxo_unspent)
{
    uint8_t hash_data[UTXO_HASH_SIZE];
    memset(hash_data, 0xAB, UTXO_HASH_SIZE);
    as_bytes* hash = as_bytes_new_wrap(hash_data, UTXO_HASH_SIZE, false);

    as_bytes* utxo = utxo_create_with_spending_data(hash, NULL);

    ASSERT_NOT_NULL(utxo);
    ASSERT_EQ(as_bytes_size(utxo), UTXO_HASH_SIZE);
    ASSERT_BYTES_EQ(as_bytes_get(utxo), hash_data, UTXO_HASH_SIZE);

    as_bytes_destroy(hash);
    as_bytes_destroy(utxo);
}

TEST(create_utxo_spent)
{
    uint8_t hash_data[UTXO_HASH_SIZE];
    uint8_t spending[SPENDING_DATA_SIZE];
    memset(hash_data, 0xAB, UTXO_HASH_SIZE);
    memset(spending, 0xCD, SPENDING_DATA_SIZE);

    as_bytes* hash = as_bytes_new_wrap(hash_data, UTXO_HASH_SIZE, false);
    as_bytes* spending_data = as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false);

    as_bytes* utxo = utxo_create_with_spending_data(hash, spending_data);

    ASSERT_NOT_NULL(utxo);
    ASSERT_EQ(as_bytes_size(utxo), FULL_UTXO_SIZE);

    const uint8_t* utxo_data = as_bytes_get(utxo);
    ASSERT_BYTES_EQ(utxo_data, hash_data, UTXO_HASH_SIZE);
    ASSERT_BYTES_EQ(utxo_data + UTXO_HASH_SIZE, spending, SPENDING_DATA_SIZE);

    as_bytes_destroy(hash);
    as_bytes_destroy(spending_data);
    as_bytes_destroy(utxo);
}

TEST(create_utxo_null_hash)
{
    as_bytes* utxo = utxo_create_with_spending_data(NULL, NULL);
    ASSERT_NULL(utxo);
}

TEST(create_utxo_wrong_hash_size)
{
    uint8_t hash_data[10];
    as_bytes* hash = as_bytes_new_wrap(hash_data, 10, false);

    as_bytes* utxo = utxo_create_with_spending_data(hash, NULL);

    ASSERT_NULL(utxo);
    as_bytes_destroy(hash);
}

TEST(error_response_creation)
{
    as_map* response = utxo_create_error_response("TEST_ERROR", "Test error message");

    ASSERT_NOT_NULL(response);

    as_val* status = as_map_get(response, (as_val*)as_string_new((char*)"status", false));
    ASSERT_NOT_NULL(status);
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "ERROR");

    as_val* code = as_map_get(response, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_NOT_NULL(code);
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), "TEST_ERROR");

    as_val* msg = as_map_get(response, (as_val*)as_string_new((char*)"message", false));
    ASSERT_NOT_NULL(msg);
    ASSERT_STR_EQ(as_string_get(as_string_fromval(msg)), "Test error message");

    as_map_destroy(response);
}

TEST(ok_response_creation)
{
    as_map* response = utxo_create_ok_response();

    ASSERT_NOT_NULL(response);

    as_val* status = as_map_get(response, (as_val*)as_string_new((char*)"status", false));
    ASSERT_NOT_NULL(status);
    ASSERT_STR_EQ(as_string_get(as_string_fromval(status)), "OK");

    as_map_destroy(response);
}

TEST(spending_data_to_hex)
{
    uint8_t spending[SPENDING_DATA_SIZE];

    // First 32 bytes: 0x00..0x1F (will be reversed)
    for (int i = 0; i < 32; i++) {
        spending[i] = (uint8_t)i;
    }
    // Next 4 bytes: 0x20, 0x21, 0x22, 0x23 (little-endian, not reversed)
    spending[32] = 0x20;
    spending[33] = 0x21;
    spending[34] = 0x22;
    spending[35] = 0x23;

    as_string* hex = utxo_spending_data_to_hex(spending);

    ASSERT_NOT_NULL(hex);

    const char* hex_str = as_string_get(hex);
    ASSERT_NOT_NULL(hex_str);

    // First 32 bytes should be reversed: 1f1e1d...020100
    ASSERT_TRUE(strncmp(hex_str, "1f1e1d1c1b1a191817161514131211100f0e0d0c0b0a09080706050403020100", 64) == 0);

    // Last 4 bytes should be as-is: 20212223
    ASSERT_TRUE(strncmp(hex_str + 64, "20212223", 8) == 0);

    as_string_destroy(hex);
}

TEST(get_and_validate_success)
{
    as_arraylist* utxos = as_arraylist_new(5, 0);

    // Create a test UTXO
    uint8_t hash_data[UTXO_HASH_SIZE];
    memset(hash_data, 0x42, UTXO_HASH_SIZE);
    as_bytes* hash = as_bytes_new_wrap(hash_data, UTXO_HASH_SIZE, false);
    as_bytes* utxo = utxo_create_with_spending_data(hash, NULL);
    as_arraylist_append(utxos, (as_val*)utxo);

    // Validate it
    as_bytes* out_utxo = NULL;
    const uint8_t* out_spending = NULL;
    as_map* error = NULL;

    int rc = utxo_get_and_validate((as_list*)utxos, 0, hash, &out_utxo, &out_spending, &error);

    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(out_utxo);
    ASSERT_NULL(out_spending);  // Unspent
    ASSERT_NULL(error);

    as_bytes_destroy(hash);
    as_arraylist_destroy(utxos);
}

TEST(get_and_validate_hash_mismatch)
{
    as_arraylist* utxos = as_arraylist_new(5, 0);

    // Create a UTXO with one hash
    uint8_t hash_data[UTXO_HASH_SIZE];
    memset(hash_data, 0x42, UTXO_HASH_SIZE);
    as_bytes* hash = as_bytes_new_wrap(hash_data, UTXO_HASH_SIZE, false);
    as_bytes* utxo = utxo_create_with_spending_data(hash, NULL);
    as_arraylist_append(utxos, (as_val*)utxo);

    // Try to validate with different hash
    uint8_t wrong_hash[UTXO_HASH_SIZE];
    memset(wrong_hash, 0x99, UTXO_HASH_SIZE);
    as_bytes* wrong = as_bytes_new_wrap(wrong_hash, UTXO_HASH_SIZE, false);

    as_bytes* out_utxo = NULL;
    const uint8_t* out_spending = NULL;
    as_map* error = NULL;

    int rc = utxo_get_and_validate((as_list*)utxos, 0, wrong, &out_utxo, &out_spending, &error);

    ASSERT_NEQ(rc, 0);
    ASSERT_NOT_NULL(error);

    as_val* code = as_map_get(error, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_UTXO_HASH_MISMATCH);

    as_bytes_destroy(hash);
    as_bytes_destroy(wrong);
    as_arraylist_destroy(utxos);
    as_map_destroy(error);
}

TEST(get_and_validate_not_found)
{
    as_arraylist* utxos = as_arraylist_new(5, 0);

    uint8_t hash_data[UTXO_HASH_SIZE];
    memset(hash_data, 0x42, UTXO_HASH_SIZE);
    as_bytes* hash = as_bytes_new_wrap(hash_data, UTXO_HASH_SIZE, false);

    as_bytes* out_utxo = NULL;
    const uint8_t* out_spending = NULL;
    as_map* error = NULL;

    // Try to get UTXO at offset 5 (doesn't exist)
    int rc = utxo_get_and_validate((as_list*)utxos, 5, hash, &out_utxo, &out_spending, &error);

    ASSERT_NEQ(rc, 0);
    ASSERT_NOT_NULL(error);

    as_val* code = as_map_get(error, (as_val*)as_string_new((char*)"errorCode", false));
    ASSERT_STR_EQ(as_string_get(as_string_fromval(code)), ERROR_CODE_UTXO_NOT_FOUND);

    as_bytes_destroy(hash);
    as_arraylist_destroy(utxos);
    as_map_destroy(error);
}

TEST(get_and_validate_with_spending_data)
{
    as_arraylist* utxos = as_arraylist_new(5, 0);

    // Create a spent UTXO
    uint8_t hash_data[UTXO_HASH_SIZE];
    uint8_t spending[SPENDING_DATA_SIZE];
    memset(hash_data, 0x42, UTXO_HASH_SIZE);
    memset(spending, 0xCD, SPENDING_DATA_SIZE);

    as_bytes* hash = as_bytes_new_wrap(hash_data, UTXO_HASH_SIZE, false);
    as_bytes* spending_data = as_bytes_new_wrap(spending, SPENDING_DATA_SIZE, false);
    as_bytes* utxo = utxo_create_with_spending_data(hash, spending_data);
    as_arraylist_append(utxos, (as_val*)utxo);

    as_bytes* out_utxo = NULL;
    const uint8_t* out_spending = NULL;
    as_map* error = NULL;

    int rc = utxo_get_and_validate((as_list*)utxos, 0, hash, &out_utxo, &out_spending, &error);

    ASSERT_EQ(rc, 0);
    ASSERT_NOT_NULL(out_utxo);
    ASSERT_NOT_NULL(out_spending);
    ASSERT_NULL(error);
    ASSERT_BYTES_EQ(out_spending, spending, SPENDING_DATA_SIZE);

    as_bytes_destroy(hash);
    as_bytes_destroy(spending_data);
    // No destroy needed for out_spending - it points into existing UTXO bytes
    as_arraylist_destroy(utxos);
}

void run_helper_tests(void)
{
    printf("\n=== Helper Function Tests ===\n");

    RUN_TEST(bytes_equal_same);
    RUN_TEST(bytes_equal_different);
    RUN_TEST(bytes_equal_different_size);
    RUN_TEST(bytes_equal_null_both);
    RUN_TEST(bytes_equal_null_one);

    RUN_TEST(is_frozen_true);
    RUN_TEST(is_frozen_false);
    RUN_TEST(is_frozen_partial);
    RUN_TEST(is_frozen_null);

    RUN_TEST(create_utxo_unspent);
    RUN_TEST(create_utxo_spent);
    RUN_TEST(create_utxo_null_hash);
    RUN_TEST(create_utxo_wrong_hash_size);

    RUN_TEST(error_response_creation);
    RUN_TEST(ok_response_creation);
    RUN_TEST(spending_data_to_hex);

    RUN_TEST(get_and_validate_success);
    RUN_TEST(get_and_validate_hash_mismatch);
    RUN_TEST(get_and_validate_not_found);
    RUN_TEST(get_and_validate_with_spending_data);
}
