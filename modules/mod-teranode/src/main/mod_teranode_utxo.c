/*
 * mod_teranode_utxo.c
 *
 * Copyright (C) 2024 Aerospike, Inc.
 *
 * UTXO function implementations.
 * Direct port from teranode.lua to C.
 */

#include <aerospike/mod_teranode_utxo.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_nil.h>

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "internal.h"

//==========================================================
// Forward declarations and inline helpers.
//

// Helper to get boolean from list
static inline bool get_list_bool(as_list* list, uint32_t index) {
    if (list == NULL) return false;
    as_val* val = as_list_get(list, index);
    if (val == NULL) return false;
    if (as_val_type(val) != AS_BOOLEAN) return false;
    as_boolean* b = as_boolean_fromval(val);
    if (b == NULL) return false;
    return as_boolean_get(b);
}

// Helper to get integer from list
static inline int64_t get_list_int64(as_list* list, uint32_t index) {
    if (list == NULL) return 0;
    as_val* val = as_list_get(list, index);
    if (val == NULL) return 0;
    if (as_val_type(val) != AS_INTEGER) return 0;
    as_integer* i = as_integer_fromval(val);
    if (i == NULL) return 0;
    return as_integer_get(i);
}

// Helper to get bytes from list
static inline as_bytes* get_list_bytes(as_list* list, uint32_t index) {
    if (list == NULL) return NULL;
    as_val* val = as_list_get(list, index);
    if (val == NULL) return NULL;
    if (as_val_type(val) != AS_BYTES) return NULL;
    return as_bytes_fromval(val);
}

//==========================================================
// Helper function implementations.
//

bool
utxo_bytes_equal(as_bytes* a, as_bytes* b)
{
    if (a == NULL || b == NULL) {
        return a == b;
    }

    uint32_t size_a = as_bytes_size(a);
    uint32_t size_b = as_bytes_size(b);

    if (size_a != size_b) {
        return false;
    }

    const uint8_t* data_a = as_bytes_get(a);
    const uint8_t* data_b = as_bytes_get(b);

    if (data_a == NULL || data_b == NULL) {
        return false;
    }

    return memcmp(data_a, data_b, size_a) == 0;
}

bool
utxo_is_frozen(as_bytes* spending_data)
{
    if (spending_data == NULL) {
        return false;
    }

    uint32_t size = as_bytes_size(spending_data);
    if (size != SPENDING_DATA_SIZE) {
        return false;
    }

    const uint8_t* data = as_bytes_get(spending_data);
    if (data == NULL) {
        return false;
    }
    for (uint32_t i = 0; i < SPENDING_DATA_SIZE; i++) {
        if (data[i] != FROZEN_BYTE) {
            return false;
        }
    }

    return true;
}

as_bytes*
utxo_create_with_spending_data(as_bytes* utxo_hash, as_bytes* spending_data)
{
    if (utxo_hash == NULL) {
        return NULL;
    }

    uint32_t hash_size = as_bytes_size(utxo_hash);
    if (hash_size != UTXO_HASH_SIZE) {
        return NULL;
    }

    uint32_t new_size = (spending_data != NULL) ? FULL_UTXO_SIZE : UTXO_HASH_SIZE;
    as_bytes* new_utxo = as_bytes_new(new_size);

    if (new_utxo == NULL) {
        return NULL;
    }

    // Copy utxo hash
    const uint8_t* hash_data = as_bytes_get(utxo_hash);
    if (hash_data == NULL) {
        as_bytes_destroy(new_utxo);
        return NULL;
    }
    as_bytes_set(new_utxo, 0, hash_data, UTXO_HASH_SIZE);

    // Copy spending data if provided
    if (spending_data != NULL) {
        if (as_bytes_size(spending_data) != SPENDING_DATA_SIZE) {
            as_bytes_destroy(new_utxo);
            return NULL;
        }
        const uint8_t* spending_ptr = as_bytes_get(spending_data);
        if (spending_ptr == NULL) {
            as_bytes_destroy(new_utxo);
            return NULL;
        }
        as_bytes_set(new_utxo, UTXO_HASH_SIZE, spending_ptr, SPENDING_DATA_SIZE);
    }

    return new_utxo;
}

int
utxo_get_and_validate(
    as_list* utxos,
    int64_t offset,
    as_bytes* expected_hash,
    as_bytes** out_utxo,
    as_bytes** out_spending_data,
    as_map** out_error_response)
{
    if (out_utxo == NULL || out_spending_data == NULL || out_error_response == NULL) {
        return -1;
    }
    *out_utxo = NULL;
    *out_spending_data = NULL;
    *out_error_response = NULL;

    if (utxos == NULL || expected_hash == NULL) {
        *out_error_response = utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Invalid parameters");
        return -1;
    }

    if (offset < 0) {
        *out_error_response = utxo_create_error_response(
            ERROR_CODE_UTXO_NOT_FOUND, ERR_UTXO_NOT_FOUND);
        return -1;
    }

    uint32_t utxos_sz = as_list_size(utxos);
    if ((uint64_t)offset >= (uint64_t)utxos_sz) {
        *out_error_response = utxo_create_error_response(
            ERROR_CODE_UTXO_NOT_FOUND, ERR_UTXO_NOT_FOUND);
        return -1;
    }

    uint32_t expected_size = as_bytes_size(expected_hash);
    if (expected_size != UTXO_HASH_SIZE) {
        *out_error_response = utxo_create_error_response(
            ERROR_CODE_UTXO_INVALID_SIZE, ERR_UTXO_INVALID_SIZE);
        return -1;
    }

    // Lua arrays are 1-based, but as_list is 0-based
    as_val* val = as_list_get(utxos, (uint32_t)offset);
    if (val == NULL) {
        *out_error_response = utxo_create_error_response(
            ERROR_CODE_UTXO_NOT_FOUND, ERR_UTXO_NOT_FOUND);
        return -1;
    }

    if (as_val_type(val) != AS_BYTES) {
        *out_error_response = utxo_create_error_response(
            ERROR_CODE_UTXO_INVALID_SIZE, ERR_UTXO_INVALID_SIZE);
        return -1;
    }

    as_bytes* utxo = NULL;
    if (as_val_type(val) == AS_BYTES) {
        utxo = as_bytes_fromval(val);
    }
    if (utxo == NULL) {
        *out_error_response = utxo_create_error_response(
            ERROR_CODE_UTXO_INVALID_SIZE, ERR_UTXO_INVALID_SIZE);
        return -1;
    }

    uint32_t utxo_size = as_bytes_size(utxo);
    if (utxo_size != UTXO_HASH_SIZE && utxo_size != FULL_UTXO_SIZE) {
        *out_error_response = utxo_create_error_response(
            ERROR_CODE_UTXO_INVALID_SIZE, ERR_UTXO_INVALID_SIZE);
        return -1;
    }

    // Extract and compare hash (first 32 bytes)
    const uint8_t* utxo_data = as_bytes_get(utxo);
    const uint8_t* expected_data = as_bytes_get(expected_hash);

    if (utxo_data == NULL || expected_data == NULL) {
        *out_error_response = utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Failed to get UTXO data");
        return -1;
    }

    if (memcmp(utxo_data, expected_data, UTXO_HASH_SIZE) != 0) {
        *out_error_response = utxo_create_error_response(
            ERROR_CODE_UTXO_HASH_MISMATCH, ERR_UTXO_HASH_MISMATCH);
        return -1;
    }

    *out_utxo = utxo;

    // Extract spending data if present
    if (utxo_size == FULL_UTXO_SIZE) {
        // Create a new bytes object for spending data
        as_bytes* spending = as_bytes_new(SPENDING_DATA_SIZE);
        if (spending == NULL) {
            *out_error_response = utxo_create_error_response(
                ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
            return -1;
        }
        as_bytes_set(spending, 0, utxo_data + UTXO_HASH_SIZE, SPENDING_DATA_SIZE);
        *out_spending_data = spending;
    }

    return 0;
}

as_string*
utxo_spending_data_to_hex(as_bytes* spending_data)
{
    if (spending_data == NULL || as_bytes_size(spending_data) != SPENDING_DATA_SIZE) {
        return NULL;
    }

    // Output: 64 chars (32 bytes reversed) + 8 chars (4 bytes LE) + null
    char hex[73];
    const uint8_t* data = as_bytes_get(spending_data);
    if (data == NULL) {
        return NULL;
    }

    // First 32 bytes reversed (txID)
    int pos = 0;
    for (int i = 31; i >= 0; i--) {
        sprintf(&hex[pos], "%02x", data[i]);
        pos += 2;
    }

    // Next 4 bytes as-is (vin in little-endian)
    for (int i = 32; i < 36; i++) {
        sprintf(&hex[pos], "%02x", data[i]);
        pos += 2;
    }

    hex[72] = '\0';
    // Use strdup to copy string to heap, true flag means as_string will free it
    char* hex_copy = strdup(hex);
    if (hex_copy == NULL) {
        return NULL;
    }
    as_string* s = as_string_new(hex_copy, true);
    if (s == NULL) {
        free(hex_copy);
        return NULL;
    }
    return s;
}

// Convert spending data to just txid hex (32 bytes reversed) - for deletedChildren check
static as_string*
utxo_spending_data_to_txid_hex(as_bytes* spending_data)
{
    if (spending_data == NULL || as_bytes_size(spending_data) != SPENDING_DATA_SIZE) {
        return NULL;
    }

    // Output: 64 chars (32 bytes reversed) + null
    char hex[65];
    const uint8_t* data = as_bytes_get(spending_data);
    if (data == NULL) {
        return NULL;
    }

    // First 32 bytes reversed (txID)
    int pos = 0;
    for (int i = 31; i >= 0; i--) {
        sprintf(&hex[pos], "%02x", data[i]);
        pos += 2;
    }

    hex[64] = '\0';
    // Use strdup to copy string to heap, true flag means as_string will free it
    char* hex_copy = strdup(hex);
    if (hex_copy == NULL) {
        return NULL;
    }
    as_string* s = as_string_new(hex_copy, true);
    if (s == NULL) {
        free(hex_copy);
        return NULL;
    }
    return s;
}

as_map*
utxo_create_error_response(const char* error_code, const char* message)
{
    as_hashmap* response = as_hashmap_new(4);
    if (response == NULL) {
        return NULL;
    }

    as_string* k_status = as_string_new((char*)FIELD_STATUS, false);
    as_string* v_status = as_string_new((char*)STATUS_ERROR, false);
    if (k_status == NULL || v_status == NULL) {
        if (k_status) as_string_destroy(k_status);
        if (v_status) as_string_destroy(v_status);
        as_hashmap_destroy(response);
        return NULL;
    }
    as_hashmap_set(response, (as_val*)k_status, (as_val*)v_status);

    as_string* k_code = as_string_new((char*)FIELD_ERROR_CODE, false);
    as_string* v_code = as_string_new((char*)error_code, false);
    if (k_code == NULL || v_code == NULL) {
        if (k_code) as_string_destroy(k_code);
        if (v_code) as_string_destroy(v_code);
        as_hashmap_destroy(response);
        return NULL;
    }
    as_hashmap_set(response, (as_val*)k_code, (as_val*)v_code);

    char* message_copy = strdup(message);
    if (message_copy == NULL) {
        as_hashmap_destroy(response);
        return NULL;
    }
    as_string* k_msg = as_string_new((char*)FIELD_MESSAGE, false);
    as_string* v_msg = as_string_new(message_copy, true);
    if (k_msg == NULL || v_msg == NULL) {
        if (k_msg) as_string_destroy(k_msg);
        if (v_msg == NULL) {
            free(message_copy);
        } else {
            as_string_destroy(v_msg);
        }
        as_hashmap_destroy(response);
        return NULL;
    }
    as_hashmap_set(response, (as_val*)k_msg, (as_val*)v_msg);

    return (as_map*)response;
}

as_map*
utxo_create_ok_response(void)
{
    as_hashmap* response = as_hashmap_new(2);
    if (response == NULL) {
        return NULL;
    }

    as_string* k_status = as_string_new((char*)FIELD_STATUS, false);
    as_string* v_status = as_string_new((char*)STATUS_OK, false);
    if (k_status == NULL || v_status == NULL) {
        if (k_status) as_string_destroy(k_status);
        if (v_status) as_string_destroy(v_status);
        as_hashmap_destroy(response);
        return NULL;
    }
    as_hashmap_set(response, (as_val*)k_status, (as_val*)v_status);

    return (as_map*)response;
}

//==========================================================
// setDeleteAtHeight implementation (internal helper).
//

const char*
utxo_set_delete_at_height_impl(
    as_rec* rec,
    int64_t current_block_height,
    int64_t block_height_retention,
    int64_t* out_child_count)
{
    *out_child_count = 0;

    if (block_height_retention == 0) {
        return "";
    }

    // Check preserveUntil
    as_val* preserve_until_val = as_rec_get(rec, BIN_PRESERVE_UNTIL);
    if (preserve_until_val != NULL && as_val_type(preserve_until_val) != AS_NIL) {
        return "";
    }

    // Get relevant bins
    as_val* block_ids_val = as_rec_get(rec, BIN_BLOCK_IDS);
    as_val* total_extra_recs_val = as_rec_get(rec, BIN_TOTAL_EXTRA_RECS);
    as_val* spent_extra_recs_val = as_rec_get(rec, BIN_SPENT_EXTRA_RECS);
    as_val* existing_dah_val = as_rec_get(rec, BIN_DELETE_AT_HEIGHT);
    as_val* conflicting_val = as_rec_get(rec, BIN_CONFLICTING);
    as_val* external_val = as_rec_get(rec, BIN_EXTERNAL);
    as_val* spent_utxos_val = as_rec_get(rec, BIN_SPENT_UTXOS);
    as_val* record_utxos_val = as_rec_get(rec, BIN_RECORD_UTXOS);
    as_val* unmined_since_val = as_rec_get(rec, BIN_UNMINED_SINCE);
    as_val* last_spent_state_val = as_rec_get(rec, BIN_LAST_SPENT_STATE);

    int64_t new_delete_height = current_block_height + block_height_retention;
    int64_t spent_extra_recs = 0;
    bool has_external = (external_val != NULL && as_val_type(external_val) != AS_NIL);

    if (spent_extra_recs_val != NULL && as_val_type(spent_extra_recs_val) == AS_INTEGER) {
        spent_extra_recs = as_integer_get(as_integer_fromval(spent_extra_recs_val));
    }

    // Handle conflicting transactions
    if (conflicting_val != NULL && as_val_type(conflicting_val) == AS_BOOLEAN) {
        if (as_boolean_get(as_boolean_fromval(conflicting_val))) {
            if (existing_dah_val == NULL || as_val_type(existing_dah_val) == AS_NIL) {
                // Extract total_extra_recs BEFORE as_rec_set to avoid use-after-free
                bool should_signal = false;
                int64_t total_extra_recs = 0;
                if (has_external && total_extra_recs_val != NULL && as_val_type(total_extra_recs_val) == AS_INTEGER) {
                    total_extra_recs = as_integer_get(as_integer_fromval(total_extra_recs_val));
                    should_signal = true;
                }

                as_rec_set(rec, BIN_DELETE_AT_HEIGHT, (as_val*)as_integer_new(new_delete_height));

                if (should_signal) {
                    *out_child_count = total_extra_recs;
                    return SIGNAL_DELETE_AT_HEIGHT_SET;
                }
            }
            return "";
        }
    }

    // Handle pagination records (no totalExtraRecs = child record)
    if (total_extra_recs_val == NULL || as_val_type(total_extra_recs_val) == AS_NIL) {
        // Default nil to NOTALLSPENT
        const char* last_state = SIGNAL_NOT_ALL_SPENT;
        if (last_spent_state_val != NULL && as_val_type(last_spent_state_val) == AS_STRING) {
            last_state = as_string_get(as_string_fromval(last_spent_state_val));
        }

        int64_t spent_utxos = 0;
        int64_t record_utxos = 0;

        if (spent_utxos_val != NULL && as_val_type(spent_utxos_val) == AS_INTEGER) {
            spent_utxos = as_integer_get(as_integer_fromval(spent_utxos_val));
        }
        if (record_utxos_val != NULL && as_val_type(record_utxos_val) == AS_INTEGER) {
            record_utxos = as_integer_get(as_integer_fromval(record_utxos_val));
        }

        const char* current_state = (spent_utxos == record_utxos) ?
            SIGNAL_ALL_SPENT : SIGNAL_NOT_ALL_SPENT;

        if (strcmp(last_state, current_state) != 0) {
            as_rec_set(rec, BIN_LAST_SPENT_STATE, (as_val*)as_string_new((char*)current_state, false));
            return current_state;
        }
        return "";
    }

    // Master record: check all conditions for deletion
    if (as_val_type(total_extra_recs_val) != AS_INTEGER) {
        return "";
    }
    int64_t total_extra_recs = as_integer_get(as_integer_fromval(total_extra_recs_val));
    *out_child_count = total_extra_recs;

    int64_t spent_utxos = 0;
    int64_t record_utxos = 0;

    if (spent_utxos_val != NULL && as_val_type(spent_utxos_val) == AS_INTEGER) {
        spent_utxos = as_integer_get(as_integer_fromval(spent_utxos_val));
    }
    if (record_utxos_val != NULL && as_val_type(record_utxos_val) == AS_INTEGER) {
        record_utxos = as_integer_get(as_integer_fromval(record_utxos_val));
    }

    bool all_spent = (total_extra_recs == spent_extra_recs) && (spent_utxos == record_utxos);

    bool has_block_ids = false;
    if (block_ids_val != NULL && as_val_type(block_ids_val) == AS_LIST) {
        has_block_ids = as_list_size(as_list_fromval(block_ids_val)) > 0;
    }

    bool is_on_longest_chain = (unmined_since_val == NULL || as_val_type(unmined_since_val) == AS_NIL);

    if (all_spent && has_block_ids && is_on_longest_chain) {
        int64_t existing_dah = 0;
        if (existing_dah_val != NULL && as_val_type(existing_dah_val) == AS_INTEGER) {
            existing_dah = as_integer_get(as_integer_fromval(existing_dah_val));
        }

        if (existing_dah == 0 || existing_dah < new_delete_height) {
            as_rec_set(rec, BIN_DELETE_AT_HEIGHT, (as_val*)as_integer_new(new_delete_height));
            if (has_external) {
                return SIGNAL_DELETE_AT_HEIGHT_SET;
            }
        }
    } else if (existing_dah_val != NULL && as_val_type(existing_dah_val) != AS_NIL) {
        as_rec_set(rec, BIN_DELETE_AT_HEIGHT, (as_val*)&as_nil);
        if (has_external) {
            return SIGNAL_DELETE_AT_HEIGHT_UNSET;
        }
    }

    return "";
}

//==========================================================
// UTXO Function Implementations.
//

as_val*
teranode_spend(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    int64_t offset = get_list_int64(args, 0);
    as_bytes* utxo_hash = get_list_bytes(args, 1);
    as_bytes* spending_data = get_list_bytes(args, 2);
    bool ignore_conflicting = get_list_bool(args, 3);
    bool ignore_locked = get_list_bool(args, 4);
    int64_t current_block_height = get_list_int64(args, 5);
    int64_t block_height_retention = get_list_int64(args, 6);

    // Validate required arguments
    if (utxo_hash == NULL || spending_data == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Missing utxo_hash or spending_data");
    }

    // Build single spend item for spendMulti
    as_hashmap* spend_item = as_hashmap_new(4);
    if (spend_item == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    // Safely add fields to spend_item with allocation checks
    as_string* k_offset = as_string_new((char*)"offset", false);
    as_integer* v_offset = as_integer_new(offset);
    if (k_offset == NULL || v_offset == NULL) {
        if (k_offset) as_string_destroy(k_offset);
        if (v_offset) as_integer_destroy(v_offset);
        as_hashmap_destroy(spend_item);
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    as_hashmap_set(spend_item, (as_val*)k_offset, (as_val*)v_offset);

    as_string* k_utxo_hash = as_string_new((char*)"utxoHash", false);
    if (k_utxo_hash == NULL) {
        as_hashmap_destroy(spend_item);
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    as_hashmap_set(spend_item, (as_val*)k_utxo_hash, as_val_reserve((as_val*)utxo_hash));

    as_string* k_spending = as_string_new((char*)"spendingData", false);
    if (k_spending == NULL) {
        as_hashmap_destroy(spend_item);
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    as_hashmap_set(spend_item, (as_val*)k_spending, as_val_reserve((as_val*)spending_data));

    as_arraylist* spends = as_arraylist_new(1, 0);
    if (spends == NULL) {
        as_hashmap_destroy(spend_item);
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    as_arraylist_append(spends, (as_val*)spend_item);

    // Build args for spendMulti
    as_arraylist* multi_args = as_arraylist_new(5, 0);
    if (multi_args == NULL) {
        as_arraylist_destroy(spends);
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    as_arraylist_append(multi_args, (as_val*)spends);
    as_arraylist_append(multi_args, (as_val*)as_boolean_new(ignore_conflicting));
    as_arraylist_append(multi_args, (as_val*)as_boolean_new(ignore_locked));
    as_arraylist_append_int64(multi_args, current_block_height);
    as_arraylist_append_int64(multi_args, block_height_retention);

    as_val* result = teranode_spend_multi(rec, (as_list*)multi_args);
    as_val_destroy((as_val*)multi_args);
    return result;
}

as_val*
teranode_spend_multi(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    as_hashmap* response = as_hashmap_new(8);
    if (response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }

    // Extract arguments
    as_val* spends_val = as_list_get(args, 0);
    if (spends_val == NULL || as_val_type(spends_val) != AS_LIST) {
        as_val_destroy((as_val*)response);
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Invalid spends list");
    }
    as_list* spends = as_list_fromval(spends_val);
    bool ignore_conflicting = get_list_bool(args, 1);
    bool ignore_locked = get_list_bool(args, 2);
    int64_t current_block_height = get_list_int64(args, 3);
    int64_t block_height_retention = get_list_int64(args, 4);

    // Check creating flag
    as_val* creating_val = as_rec_get(rec, BIN_CREATING);
    if (creating_val != NULL && as_val_type(creating_val) == AS_BOOLEAN) {
        if (as_boolean_get(as_boolean_fromval(creating_val))) {
            as_val_destroy((as_val*)response);
            return (as_val*)utxo_create_error_response(ERROR_CODE_CREATING, MSG_CREATING);
        }
    }

    // Check conflicting
    if (!ignore_conflicting) {
        as_val* conflicting_val = as_rec_get(rec, BIN_CONFLICTING);
        if (conflicting_val != NULL && as_val_type(conflicting_val) == AS_BOOLEAN) {
            if (as_boolean_get(as_boolean_fromval(conflicting_val))) {
                as_val_destroy((as_val*)response);
                return (as_val*)utxo_create_error_response(ERROR_CODE_CONFLICTING, MSG_CONFLICTING);
            }
        }
    }

    // Check locked
    if (!ignore_locked) {
        as_val* locked_val = as_rec_get(rec, BIN_LOCKED);
        cf_info(AS_UDF, "LOCKED_DEBUG: ignore_locked=%d locked_val=%p", ignore_locked, (void*)locked_val);
        if (locked_val != NULL) {
            int val_type = as_val_type(locked_val);
            cf_info(AS_UDF, "LOCKED_DEBUG: val_type=%d (AS_BOOLEAN=%d)", val_type, AS_BOOLEAN);
            if (val_type == AS_BOOLEAN) {
                as_boolean* bool_obj = as_boolean_fromval(locked_val);
                cf_info(AS_UDF, "LOCKED_DEBUG: bool_obj=%p", (void*)bool_obj);
                if (bool_obj != NULL) {
                    bool bool_value = as_boolean_get(bool_obj);
                    cf_info(AS_UDF, "LOCKED_DEBUG: bool_value=%d - WILL%s RETURN ERROR", bool_value, bool_value ? "" : " NOT");
                    if (bool_value) {
                        as_val_destroy((as_val*)response);
                        return (as_val*)utxo_create_error_response(ERROR_CODE_LOCKED, MSG_LOCKED);
                    }
                }
            }
        }
    }

    // Check coinbase spending height
    as_val* spending_height_val = as_rec_get(rec, BIN_SPENDING_HEIGHT);
    if (spending_height_val != NULL && as_val_type(spending_height_val) == AS_INTEGER) {
        int64_t coinbase_spending_height = as_integer_get(as_integer_fromval(spending_height_val));
        if (coinbase_spending_height > 0 && coinbase_spending_height > current_block_height) {
            char msg[256];
            snprintf(msg, sizeof(msg), "%s, spendable in block %lld or greater. Current block height is %lld",
                MSG_COINBASE_IMMATURE, (long long)coinbase_spending_height, (long long)current_block_height);
            as_val_destroy((as_val*)response);
            return (as_val*)utxo_create_error_response(ERROR_CODE_COINBASE_IMMATURE, msg);
        }
    }

    // Get utxos bin
    as_val* utxos_val = as_rec_get(rec, BIN_UTXOS);
    if (utxos_val == NULL || as_val_type(utxos_val) != AS_LIST) {
        as_val_destroy((as_val*)response);
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXOS_NOT_FOUND, ERR_UTXOS_NOT_FOUND);
    }
    as_list* utxos = as_list_fromval(utxos_val);

    // Note: deleted_children, spendable_in, and utxos are fetched inside the loop
    // because as_rec_set calls may invalidate record values

    // Process each spend
    as_hashmap* errors = as_hashmap_new(8);
    if (errors == NULL) {
        as_val_destroy((as_val*)response);
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    uint32_t spends_size = as_list_size(spends);

    for (uint32_t i = 0; i < spends_size; i++) {
        // Re-fetch record values at each iteration since as_rec_set may invalidate them
        utxos_val = as_rec_get(rec, BIN_UTXOS);
        if (utxos_val == NULL || as_val_type(utxos_val) != AS_LIST) {
            // This shouldn't happen, but handle it gracefully
            break;
        }
        utxos = as_list_fromval(utxos_val);

        as_val* deleted_children_val = as_rec_get(rec, BIN_DELETED_CHILDREN);
        as_map* deleted_children = NULL;
        if (deleted_children_val != NULL && as_val_type(deleted_children_val) == AS_MAP) {
            deleted_children = as_map_fromval(deleted_children_val);
        }

        as_val* spendable_in_val = as_rec_get(rec, BIN_UTXO_SPENDABLE_IN);
        as_map* spendable_in = NULL;
        if (spendable_in_val != NULL && as_val_type(spendable_in_val) == AS_MAP) {
            spendable_in = as_map_fromval(spendable_in_val);
        }

        as_map* spend_item = as_map_fromval(as_list_get(spends, i));
        if (spend_item == NULL) continue;

        as_string* k_offset = as_string_new((char*)"offset", false);
        as_val* v_offset = as_map_get(spend_item, (as_val*)k_offset);
        if (v_offset == NULL || as_val_type(v_offset) != AS_INTEGER) {
            as_val_destroy((as_val*)k_offset);
            continue;
        }
        int64_t offset = as_integer_get(as_integer_fromval(v_offset));
        as_val_destroy((as_val*)k_offset);

        as_string* k_utxo = as_string_new((char*)"utxoHash", false);
        as_val* v_utxo = as_map_get(spend_item, (as_val*)k_utxo);
        if (v_utxo == NULL || as_val_type(v_utxo) != AS_BYTES) {
            as_val_destroy((as_val*)k_utxo);
            continue;
        }
        as_bytes* utxo_hash = as_bytes_fromval(v_utxo);
        as_val_destroy((as_val*)k_utxo);

        as_string* k_sp = as_string_new((char*)"spendingData", false);
        as_val* v_sp = as_map_get(spend_item, (as_val*)k_sp);
        if (v_sp == NULL || as_val_type(v_sp) != AS_BYTES) {
            as_val_destroy((as_val*)k_sp);
            continue;
        }
        as_bytes* spending_data = as_bytes_fromval(v_sp);
        as_val_destroy((as_val*)k_sp);

        as_string* k_idx = as_string_new((char*)"idx", false);
        as_val* idx_val = as_map_get(spend_item, (as_val*)k_idx);
        as_val_destroy((as_val*)k_idx);
        int64_t idx = (idx_val != NULL && as_val_type(idx_val) == AS_INTEGER) ? as_integer_get(as_integer_fromval(idx_val)) : i;

        // Validate UTXO
        as_bytes* utxo = NULL;
        as_bytes* existing_spending_data = NULL;
        as_map* error_response = NULL;

        if (utxo_get_and_validate(utxos, offset, utxo_hash, &utxo, &existing_spending_data, &error_response) != 0) {
            as_hashmap_set(errors,
                (as_val*)as_integer_new(idx),
                (as_val*)error_response);
            continue;
        }

        // Check spendable_in
        if (spendable_in != NULL) {
            as_integer* k_off = as_integer_new(offset);
            as_val* spendable_val = as_map_get(spendable_in, (as_val*)k_off);
            as_val_destroy((as_val*)k_off);
            if (spendable_val != NULL && as_val_type(spendable_val) == AS_INTEGER) {
                int64_t spendable_height = as_integer_get(as_integer_fromval(spendable_val));
                if (spendable_height >= current_block_height) {
                    char msg[256];
                    snprintf(msg, sizeof(msg), "%s%lld", MSG_FROZEN_UNTIL, (long long)spendable_height);
                    as_hashmap* err = as_hashmap_new(2);
                    if (err != NULL) {
                        as_string* k_code = as_string_new((char*)FIELD_ERROR_CODE, false);
                        as_string* v_code = as_string_new((char*)ERROR_CODE_FROZEN_UNTIL, false);
                        if (k_code == NULL || v_code == NULL) {
                            if (k_code) as_string_destroy(k_code);
                            if (v_code) as_string_destroy(v_code);
                            as_hashmap_destroy(err);
                        } else {
                            as_hashmap_set(err, (as_val*)k_code, (as_val*)v_code);

                            // Duplicate message safely; fall back to a static message on OOM
                            char* dyn = strdup(msg);
                            as_val* msg_val = NULL;
                            if (dyn != NULL) {
                                as_string* s = as_string_new(dyn, true);
                                if (s != NULL) {
                                    msg_val = (as_val*)s;
                                } else {
                                    free(dyn);
                                }
                            }
                            if (msg_val == NULL) {
                                msg_val = (as_val*)as_string_new((char*)MSG_FROZEN, false);
                            }

                            as_string* k_msg = as_string_new((char*)FIELD_MESSAGE, false);
                            if (k_msg != NULL && msg_val != NULL) {
                                as_hashmap_set(err, (as_val*)k_msg, msg_val);
                            } else {
                                if (k_msg) as_string_destroy(k_msg);
                                if (msg_val) as_val_destroy(msg_val);
                            }

                            as_integer* idx_key = as_integer_new(idx);
                            if (idx_key != NULL) {
                                as_hashmap_set(errors, (as_val*)idx_key, (as_val*)err);
                            } else {
                                as_hashmap_destroy(err);
                            }
                        }
                    }

                    if (existing_spending_data) as_bytes_destroy(existing_spending_data);
                    continue;
                }
            }
        }

        // Handle already spent UTXO
        if (existing_spending_data != NULL) {
            if (utxo_bytes_equal(existing_spending_data, spending_data)) {
                // Already spent with same data - check if child tx was deleted
                if (deleted_children != NULL) {
                    // Check whether this child tx (by txid) exists in the deletedChildren map
                    as_string* child_txid = utxo_spending_data_to_txid_hex(existing_spending_data);
                    if (child_txid != NULL) {
                        as_val* deleted_val = as_map_get(deleted_children, (as_val*)child_txid);
                        if (deleted_val != NULL && as_val_type(deleted_val) != AS_NIL) {
                            // Child tx was deleted - this is an invalid spend
                            as_hashmap* err = as_hashmap_new(3);
                            if (err != NULL) {
                                as_string* k_code = as_string_new((char*)FIELD_ERROR_CODE, false);
                                as_string* v_code = as_string_new((char*)ERROR_CODE_INVALID_SPEND, false);
                                as_string* k_msg = as_string_new((char*)FIELD_MESSAGE, false);
                                as_string* v_msg = as_string_new((char*)MSG_INVALID_SPEND, false);
                                if (k_code == NULL || v_code == NULL || k_msg == NULL || v_msg == NULL) {
                                    if (k_code) as_string_destroy(k_code);
                                    if (v_code) as_string_destroy(v_code);
                                    if (k_msg) as_string_destroy(k_msg);
                                    if (v_msg) as_string_destroy(v_msg);
                                    as_hashmap_destroy(err);
                                } else {
                                    as_hashmap_set(err, (as_val*)k_code, (as_val*)v_code);
                                    as_hashmap_set(err, (as_val*)k_msg, (as_val*)v_msg);

                                    as_string* hexstr = utxo_spending_data_to_hex(existing_spending_data);
                                    if (hexstr != NULL) {
                                        as_string* k_sd = as_string_new((char*)FIELD_SPENDING_DATA, false);
                                        if (k_sd != NULL) {
                                            as_hashmap_set(err, (as_val*)k_sd, (as_val*)hexstr);
                                        } else {
                                            as_string_destroy(hexstr);
                                        }
                                    }

                                    as_integer* idx_key = as_integer_new(idx);
                                    if (idx_key != NULL) {
                                        as_hashmap_set(errors, (as_val*)idx_key, (as_val*)err);
                                    } else {
                                        as_hashmap_destroy(err);
                                    }
                                }
                            }
                            as_string_destroy(child_txid);
                            as_bytes_destroy(existing_spending_data);
                            continue;
                        }
                        as_string_destroy(child_txid);
                    }
                }
                as_bytes_destroy(existing_spending_data);
                continue;
            } else if (utxo_is_frozen(existing_spending_data)) {
                as_hashmap* err = as_hashmap_new(2);
                if (err != NULL) {
                    as_string* k_code = as_string_new((char*)FIELD_ERROR_CODE, false);
                    as_string* v_code = as_string_new((char*)ERROR_CODE_FROZEN, false);
                    as_string* k_msg = as_string_new((char*)FIELD_MESSAGE, false);
                    as_string* v_msg = as_string_new((char*)MSG_FROZEN, false);
                    if (k_code == NULL || v_code == NULL || k_msg == NULL || v_msg == NULL) {
                        if (k_code) as_string_destroy(k_code);
                        if (v_code) as_string_destroy(v_code);
                        if (k_msg) as_string_destroy(k_msg);
                        if (v_msg) as_string_destroy(v_msg);
                        as_hashmap_destroy(err);
                    } else {
                        as_hashmap_set(err, (as_val*)k_code, (as_val*)v_code);
                        as_hashmap_set(err, (as_val*)k_msg, (as_val*)v_msg);
                        as_integer* idx_key = as_integer_new(idx);
                        if (idx_key != NULL) {
                            as_hashmap_set(errors, (as_val*)idx_key, (as_val*)err);
                        } else {
                            as_hashmap_destroy(err);
                        }
                    }
                }
                as_bytes_destroy(existing_spending_data);
                continue;
            } else {
                // Spent by different transaction
                as_hashmap* err = as_hashmap_new(3);
                if (err != NULL) {
                    as_string* k_code = as_string_new((char*)FIELD_ERROR_CODE, false);
                    as_string* v_code = as_string_new((char*)ERROR_CODE_SPENT, false);
                    as_string* k_msg = as_string_new((char*)FIELD_MESSAGE, false);
                    as_string* v_msg = as_string_new((char*)MSG_SPENT, false);
                    if (k_code == NULL || v_code == NULL || k_msg == NULL || v_msg == NULL) {
                        if (k_code) as_string_destroy(k_code);
                        if (v_code) as_string_destroy(v_code);
                        if (k_msg) as_string_destroy(k_msg);
                        if (v_msg) as_string_destroy(v_msg);
                        as_hashmap_destroy(err);
                    } else {
                        as_hashmap_set(err, (as_val*)k_code, (as_val*)v_code);
                        as_hashmap_set(err, (as_val*)k_msg, (as_val*)v_msg);

                        as_string* hexstr = utxo_spending_data_to_hex(existing_spending_data);
                        if (hexstr != NULL) {
                            as_string* k_sd = as_string_new((char*)FIELD_SPENDING_DATA, false);
                            if (k_sd != NULL) {
                                as_hashmap_set(err, (as_val*)k_sd, (as_val*)hexstr);
                            } else {
                                as_string_destroy(hexstr);
                            }
                        }

                        as_integer* idx_key = as_integer_new(idx);
                        if (idx_key != NULL) {
                            as_hashmap_set(errors, (as_val*)idx_key, (as_val*)err);
                        } else {
                            as_hashmap_destroy(err);
                        }
                    }
                }
                as_bytes_destroy(existing_spending_data);
                continue;
            }
        }

        // Create new UTXO with spending data
        as_bytes* new_utxo = utxo_create_with_spending_data(utxo_hash, spending_data);
        if (new_utxo == NULL) {
            as_hashmap* err = as_hashmap_new(2);
            if (err != NULL) {
                as_hashmap_set(err,
                    (as_val*)as_string_new((char*)FIELD_ERROR_CODE, false),
                    (as_val*)as_string_new((char*)ERROR_CODE_INVALID_PARAMETER, false));
                as_hashmap_set(err,
                    (as_val*)as_string_new((char*)FIELD_MESSAGE, false),
                    (as_val*)as_string_new((char*)"Memory allocation failed", false));
                as_hashmap_set(errors, (as_val*)as_integer_new(idx), (as_val*)err);
            }
            if (existing_spending_data) as_bytes_destroy(existing_spending_data);
            continue;
        }
        as_list_set(utxos, (uint32_t)offset, (as_val*)new_utxo);

        // Increment spent count
        as_val* spent_count_val = as_rec_get(rec, BIN_SPENT_UTXOS);
        int64_t spent_count = 0;
        if (spent_count_val != NULL && as_val_type(spent_count_val) == AS_INTEGER) {
            spent_count = as_integer_get(as_integer_fromval(spent_count_val));
        }
        as_rec_set(rec, BIN_SPENT_UTXOS, (as_val*)as_integer_new(spent_count + 1));

        if (existing_spending_data) as_bytes_destroy(existing_spending_data);
    }

    // Mark utxos bin as dirty to persist changes
    // Re-fetch utxos since as_rec_set calls may have invalidated it
    utxos_val = as_rec_get(rec, BIN_UTXOS);
    if (utxos_val != NULL && as_val_type(utxos_val) == AS_LIST) {
        utxos = as_list_fromval(utxos_val);
        as_rec_set(rec, BIN_UTXOS, as_val_reserve((as_val*)utxos));
    }

    // Call setDeleteAtHeight
    int64_t child_count = 0;
    const char* signal = utxo_set_delete_at_height_impl(rec, current_block_height, block_height_retention, &child_count);

    // Re-fetch block_ids after as_rec_set calls (both above and inside setDeleteAtHeight)
    as_val* response_block_ids_val = as_rec_get(rec, BIN_BLOCK_IDS);

    // Build response
    if (as_hashmap_size(errors) > 0) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_STATUS, false),
            (as_val*)as_string_new((char*)STATUS_ERROR, false));
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_ERRORS, false),
            (as_val*)errors);
    } else {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_STATUS, false),
            (as_val*)as_string_new((char*)STATUS_OK, false));
        as_hashmap_destroy(errors);
    }

    if (response_block_ids_val != NULL && as_val_type(response_block_ids_val) == AS_LIST) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_BLOCK_IDS, false),
            as_val_reserve(response_block_ids_val));
    }

    if (signal != NULL && strlen(signal) > 0) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_SIGNAL, false),
            (as_val*)as_string_new((char*)signal, false));
        if (child_count > 0) {
            as_hashmap_set(response,
                (as_val*)as_string_new((char*)FIELD_CHILD_COUNT, false),
                (as_val*)as_integer_new(child_count));
        }
    }

    return (as_val*)response;
}

as_val*
teranode_unspend(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    int64_t offset = get_list_int64(args, 0);
    as_bytes* utxo_hash = get_list_bytes(args, 1);
    int64_t current_block_height = get_list_int64(args, 2);
    int64_t block_height_retention = get_list_int64(args, 3);

    // Validate required arguments
    if (utxo_hash == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Missing utxo_hash");
    }

    // Get utxos bin
    as_val* utxos_val = as_rec_get(rec, BIN_UTXOS);
    if (utxos_val == NULL || as_val_type(utxos_val) != AS_LIST) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXOS_NOT_FOUND, ERR_UTXOS_NOT_FOUND);
    }
    as_list* utxos = as_list_fromval(utxos_val);

    // Validate UTXO
    as_bytes* utxo = NULL;
    as_bytes* existing_spending_data = NULL;
    as_map* error_response = NULL;

    if (utxo_get_and_validate(utxos, offset, utxo_hash, &utxo, &existing_spending_data, &error_response) != 0) {
        return (as_val*)error_response;
    }

    // Only unspend if spent and not frozen
    if (as_bytes_size(utxo) == FULL_UTXO_SIZE) {
        if (utxo_is_frozen(existing_spending_data)) {
            if (existing_spending_data) as_bytes_destroy(existing_spending_data);
            return (as_val*)utxo_create_error_response(ERROR_CODE_FROZEN, ERR_UTXO_IS_FROZEN);
        }

        // Create unspent UTXO
        as_bytes* new_utxo = utxo_create_with_spending_data(utxo_hash, NULL);
        if (new_utxo == NULL) {
            if (existing_spending_data) as_bytes_destroy(existing_spending_data);
            return (as_val*)utxo_create_error_response(
                ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
        }
        as_list_set(utxos, (uint32_t)offset, (as_val*)new_utxo);

        // Mark utxos bin as dirty to persist changes
        as_rec_set(rec, BIN_UTXOS, as_val_reserve((as_val*)utxos));

        // Decrement spent count
        as_val* spent_count_val = as_rec_get(rec, BIN_SPENT_UTXOS);
        int64_t spent_count = 0;
        if (spent_count_val != NULL && as_val_type(spent_count_val) == AS_INTEGER) {
            spent_count = as_integer_get(as_integer_fromval(spent_count_val));
        }
        as_rec_set(rec, BIN_SPENT_UTXOS, (as_val*)as_integer_new(spent_count - 1));
    }

    if (existing_spending_data) as_bytes_destroy(existing_spending_data);

    // Call setDeleteAtHeight
    int64_t child_count = 0;
    const char* signal = utxo_set_delete_at_height_impl(rec, current_block_height, block_height_retention, &child_count);

    // Build response
    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();
    if (response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }

    if (signal != NULL && strlen(signal) > 0) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_SIGNAL, false),
            (as_val*)as_string_new((char*)signal, false));
        if (child_count > 0) {
            as_hashmap_set(response,
                (as_val*)as_string_new((char*)FIELD_CHILD_COUNT, false),
                (as_val*)as_integer_new(child_count));
        }
    }

    return (as_val*)response;
}

as_val*
teranode_set_mined(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments - all numeric args use the same helper for consistency
    int64_t block_id = get_list_int64(args, 0);
    int64_t block_height = get_list_int64(args, 1);
    int64_t subtree_idx = get_list_int64(args, 2);
    int64_t current_block_height = get_list_int64(args, 3);
    int64_t block_height_retention = get_list_int64(args, 4);
    bool on_longest_chain = get_list_bool(args, 5);
    bool unset_mined = get_list_bool(args, 6);

    // Get or create lists for block tracking
    // Track whether each list is new (created here) vs existing (from record)
    // For new lists: set on record ONCE after modifications, without as_val_reserve
    // For existing lists: use as_val_reserve when re-setting to mark dirty
    as_val* block_ids_val = as_rec_get(rec, BIN_BLOCK_IDS);
    as_list* block_ids = NULL;
    bool block_ids_is_new = false;
    if (block_ids_val == NULL || as_val_type(block_ids_val) == AS_NIL) {
        block_ids = (as_list*)as_arraylist_new(10, 10);
        if (block_ids == NULL) {
            return (as_val*)utxo_create_error_response(
                ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
        }
        block_ids_is_new = true;
    } else {
        if (as_val_type(block_ids_val) != AS_LIST) {
            return (as_val*)utxo_create_error_response(
                ERROR_CODE_INVALID_PARAMETER, "Invalid block_ids bin type");
        }
        block_ids = as_list_fromval(block_ids_val);
    }

    as_val* block_heights_val = as_rec_get(rec, BIN_BLOCK_HEIGHTS);
    as_list* block_heights = NULL;
    bool block_heights_is_new = false;
    if (block_heights_val == NULL || as_val_type(block_heights_val) == AS_NIL) {
        block_heights = (as_list*)as_arraylist_new(10, 10);
        if (block_heights == NULL) {
            if (block_ids_is_new) as_list_destroy(block_ids);
            return (as_val*)utxo_create_error_response(
                ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
        }
        block_heights_is_new = true;
    } else {
        if (as_val_type(block_heights_val) != AS_LIST) {
            if (block_ids_is_new) as_list_destroy(block_ids);
            return (as_val*)utxo_create_error_response(
                ERROR_CODE_INVALID_PARAMETER, "Invalid block_heights bin type");
        }
        block_heights = as_list_fromval(block_heights_val);
    }

    as_val* subtree_idxs_val = as_rec_get(rec, BIN_SUBTREE_IDXS);
    as_list* subtree_idxs = NULL;
    bool subtree_idxs_is_new = false;
    if (subtree_idxs_val == NULL || as_val_type(subtree_idxs_val) == AS_NIL) {
        subtree_idxs = (as_list*)as_arraylist_new(10, 10);
        if (subtree_idxs == NULL) {
            if (block_ids_is_new) as_list_destroy(block_ids);
            if (block_heights_is_new) as_list_destroy(block_heights);
            return (as_val*)utxo_create_error_response(
                ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
        }
        subtree_idxs_is_new = true;
    } else {
        if (as_val_type(subtree_idxs_val) != AS_LIST) {
            if (block_ids_is_new) as_list_destroy(block_ids);
            if (block_heights_is_new) as_list_destroy(block_heights);
            return (as_val*)utxo_create_error_response(
                ERROR_CODE_INVALID_PARAMETER, "Invalid subtree_idxs bin type");
        }
        subtree_idxs = as_list_fromval(subtree_idxs_val);
    }

    if (unset_mined) {
        // Remove blockID and corresponding entries
        uint32_t block_count = as_list_size(block_ids);
        int32_t found_idx = -1;

        for (uint32_t i = 0; i < block_count; i++) {
            as_val* val = as_list_get(block_ids, i);
            if (val == NULL || as_val_type(val) != AS_INTEGER) continue;
            as_integer* existing_int = as_integer_fromval(val);
            if (existing_int && as_integer_get(existing_int) == block_id) {
                found_idx = (int32_t)i;
                break;
            }
        }

        if (found_idx >= 0) {
            as_list_remove(block_ids, (uint32_t)found_idx);
            as_list_remove(block_heights, (uint32_t)found_idx);
            as_list_remove(subtree_idxs, (uint32_t)found_idx);
        }

        // Set bins on record to persist changes
        // For new lists: don't use as_val_reserve (record takes ownership)
        // For existing lists: use as_val_reserve to mark dirty without double-free
        if (block_ids_is_new) {
            as_rec_set(rec, BIN_BLOCK_IDS, (as_val*)block_ids);
        } else if (found_idx >= 0) {
            as_rec_set(rec, BIN_BLOCK_IDS, as_val_reserve((as_val*)block_ids));
        }
        if (block_heights_is_new) {
            as_rec_set(rec, BIN_BLOCK_HEIGHTS, (as_val*)block_heights);
        } else if (found_idx >= 0) {
            as_rec_set(rec, BIN_BLOCK_HEIGHTS, as_val_reserve((as_val*)block_heights));
        }
        if (subtree_idxs_is_new) {
            as_rec_set(rec, BIN_SUBTREE_IDXS, (as_val*)subtree_idxs);
        } else if (found_idx >= 0) {
            as_rec_set(rec, BIN_SUBTREE_IDXS, as_val_reserve((as_val*)subtree_idxs));
        }
    } else {
        // Add blockID if not already exists
        uint32_t block_count = as_list_size(block_ids);
        bool block_exists = false;

        for (uint32_t i = 0; i < block_count; i++) {
            as_val* val = as_list_get(block_ids, i);
            if (val == NULL || as_val_type(val) != AS_INTEGER) continue;
            as_integer* existing_int = as_integer_fromval(val);
            if (existing_int && as_integer_get(existing_int) == block_id) {
                block_exists = true;
                break;
            }
        }

        if (!block_exists) {
            as_list_append(block_ids, (as_val*)as_integer_new(block_id));
            as_list_append(block_heights, (as_val*)as_integer_new(block_height));
            as_list_append(subtree_idxs, (as_val*)as_integer_new(subtree_idx));
        }

        // Set bins on record to persist changes
        // For new lists: don't use as_val_reserve (record takes ownership)
        // For existing lists: use as_val_reserve to mark dirty without double-free
        if (block_ids_is_new) {
            as_rec_set(rec, BIN_BLOCK_IDS, (as_val*)block_ids);
        } else if (!block_exists) {
            as_rec_set(rec, BIN_BLOCK_IDS, as_val_reserve((as_val*)block_ids));
        }
        if (block_heights_is_new) {
            as_rec_set(rec, BIN_BLOCK_HEIGHTS, (as_val*)block_heights);
        } else if (!block_exists) {
            as_rec_set(rec, BIN_BLOCK_HEIGHTS, as_val_reserve((as_val*)block_heights));
        }
        if (subtree_idxs_is_new) {
            as_rec_set(rec, BIN_SUBTREE_IDXS, (as_val*)subtree_idxs);
        } else if (!block_exists) {
            as_rec_set(rec, BIN_SUBTREE_IDXS, as_val_reserve((as_val*)subtree_idxs));
        }
    }

    // Handle unminedSince based on block count
    // Re-fetch block_ids since as_rec_set may have invalidated the pointer
    as_val* current_block_ids_val = as_rec_get(rec, BIN_BLOCK_IDS);
    uint32_t has_blocks = 0;
    if (current_block_ids_val != NULL && as_val_type(current_block_ids_val) == AS_LIST) {
        has_blocks = as_list_size(as_list_fromval(current_block_ids_val));
    }
    if (has_blocks > 0) {
        if (on_longest_chain) {
            as_rec_set(rec, BIN_UNMINED_SINCE, (as_val*)&as_nil);
        }
    } else {
        as_rec_set(rec, BIN_UNMINED_SINCE, (as_val*)as_integer_new(current_block_height));
    }

    // Clear locked flag if set
    as_val* locked_val = as_rec_get(rec, BIN_LOCKED);
    if (locked_val != NULL && as_val_type(locked_val) != AS_NIL) {
        as_rec_set(rec, BIN_LOCKED, (as_val*)as_boolean_new(false));
    }

    // Clear creating flag if set (remove from record)
    as_val* creating_val = as_rec_get(rec, BIN_CREATING);
    if (creating_val != NULL && as_val_type(creating_val) != AS_NIL) {
        as_rec_set(rec, BIN_CREATING, (as_val*)&as_nil);
    }

    // Call setDeleteAtHeight
    int64_t child_count = 0;
    const char* signal = utxo_set_delete_at_height_impl(rec, current_block_height, block_height_retention, &child_count);

    // Re-fetch block_ids from record since as_rec_set calls above may have invalidated the pointer
    as_val* response_block_ids_val = as_rec_get(rec, BIN_BLOCK_IDS);

    // Build response
    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();
    if (response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }

    if (response_block_ids_val != NULL && as_val_type(response_block_ids_val) == AS_LIST) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_BLOCK_IDS, false),
            as_val_reserve(response_block_ids_val));
    }

    if (signal != NULL && strlen(signal) > 0) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_SIGNAL, false),
            (as_val*)as_string_new((char*)signal, false));
        if (child_count > 0) {
            as_hashmap_set(response,
                (as_val*)as_string_new((char*)FIELD_CHILD_COUNT, false),
                (as_val*)as_integer_new(child_count));
        }
    }

    return (as_val*)response;
}

as_val*
teranode_freeze(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    int64_t offset = get_list_int64(args, 0);
    as_bytes* utxo_hash = get_list_bytes(args, 1);

    // Validate required arguments
    if (utxo_hash == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Missing utxo_hash");
    }

    // Get utxos bin
    as_val* utxos_val = as_rec_get(rec, BIN_UTXOS);
    if (utxos_val == NULL || as_val_type(utxos_val) != AS_LIST) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXOS_NOT_FOUND, ERR_UTXOS_NOT_FOUND);
    }
    as_list* utxos = as_list_fromval(utxos_val);

    // Validate UTXO
    as_bytes* utxo = NULL;
    as_bytes* existing_spending_data = NULL;
    as_map* error_response = NULL;

    if (utxo_get_and_validate(utxos, offset, utxo_hash, &utxo, &existing_spending_data, &error_response) != 0) {
        return (as_val*)error_response;
    }

    // If UTXO has spending data, check if already frozen
    if (existing_spending_data != NULL) {
        if (utxo_is_frozen(existing_spending_data)) {
            as_bytes_destroy(existing_spending_data);
            return (as_val*)utxo_create_error_response(ERROR_CODE_ALREADY_FROZEN, MSG_ALREADY_FROZEN);
        } else {
            // Already spent by something else
            as_map* err = utxo_create_error_response(ERROR_CODE_SPENT, MSG_SPENT);
            as_hashmap_set((as_hashmap*)err,
                (as_val*)as_string_new((char*)FIELD_SPENDING_DATA, false),
                (as_val*)utxo_spending_data_to_hex(existing_spending_data));
            as_bytes_destroy(existing_spending_data);
            return (as_val*)err;
        }
    }

    // Check UTXO is unspent (size should be 32)
    if (as_bytes_size(utxo) != UTXO_HASH_SIZE) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXO_INVALID_SIZE, ERR_UTXO_INVALID_SIZE);
    }

    // Create frozen spending data (all bytes = 255)
    uint8_t frozen_buf[SPENDING_DATA_SIZE];
    memset(frozen_buf, FROZEN_BYTE, SPENDING_DATA_SIZE);

    as_bytes* frozen_data = as_bytes_new(SPENDING_DATA_SIZE);
    if (frozen_data == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    as_bytes_set(frozen_data, 0, frozen_buf, SPENDING_DATA_SIZE);

    // Create new UTXO with frozen data
    as_bytes* new_utxo = utxo_create_with_spending_data(utxo_hash, frozen_data);
    if (new_utxo == NULL) {
        as_bytes_destroy(frozen_data);
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    as_list_set(utxos, (uint32_t)offset, (as_val*)new_utxo);

    // Mark utxos bin as dirty to persist changes
    as_rec_set(rec, BIN_UTXOS, as_val_reserve((as_val*)utxos));

    as_bytes_destroy(frozen_data);

    as_map* ok_response = utxo_create_ok_response();
    if (ok_response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    return (as_val*)ok_response;
}

as_val*
teranode_unfreeze(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    int64_t offset = get_list_int64(args, 0);
    as_bytes* utxo_hash = get_list_bytes(args, 1);

    // Validate required arguments
    if (utxo_hash == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Missing utxo_hash");
    }

    // Get utxos bin
    as_val* utxos_val = as_rec_get(rec, BIN_UTXOS);
    if (utxos_val == NULL || as_val_type(utxos_val) != AS_LIST) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXOS_NOT_FOUND, ERR_UTXOS_NOT_FOUND);
    }
    as_list* utxos = as_list_fromval(utxos_val);

    // Validate UTXO
    as_bytes* utxo = NULL;
    as_bytes* existing_spending_data = NULL;
    as_map* error_response = NULL;

    if (utxo_get_and_validate(utxos, offset, utxo_hash, &utxo, &existing_spending_data, &error_response) != 0) {
        return (as_val*)error_response;
    }

    // Check that UTXO is actually frozen (size=68 with frozen pattern)
    // Unspent UTXOs (size=32) or spent UTXOs (size=68 with non-frozen data) are not frozen
    if (as_bytes_size(utxo) != FULL_UTXO_SIZE || existing_spending_data == NULL || !utxo_is_frozen(existing_spending_data)) {
        if (existing_spending_data) as_bytes_destroy(existing_spending_data);
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXO_NOT_FROZEN, ERR_UTXO_NOT_FROZEN);
    }

    // Create unspent UTXO (remove spending data)
    as_bytes* new_utxo = utxo_create_with_spending_data(utxo_hash, NULL);
    if (new_utxo == NULL) {
        as_bytes_destroy(existing_spending_data);
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    as_list_set(utxos, (uint32_t)offset, (as_val*)new_utxo);

    // Mark utxos bin as dirty to persist changes
    as_rec_set(rec, BIN_UTXOS, as_val_reserve((as_val*)utxos));

    as_bytes_destroy(existing_spending_data);

    as_map* ok_response = utxo_create_ok_response();
    if (ok_response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    return (as_val*)ok_response;
}

as_val*
teranode_reassign(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    int64_t offset = get_list_int64(args, 0);
    as_bytes* utxo_hash = get_list_bytes(args, 1);
    as_bytes* new_utxo_hash = get_list_bytes(args, 2);
    int64_t block_height = get_list_int64(args, 3);
    int64_t spendable_after = get_list_int64(args, 4);

    // Validate required arguments
    if (utxo_hash == NULL || new_utxo_hash == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Missing utxo_hash or new_utxo_hash");
    }

    // Get utxos bin
    as_val* utxos_val = as_rec_get(rec, BIN_UTXOS);
    if (utxos_val == NULL || as_val_type(utxos_val) != AS_LIST) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXOS_NOT_FOUND, ERR_UTXOS_NOT_FOUND);
    }
    as_list* utxos = as_list_fromval(utxos_val);

    // Validate UTXO
    as_bytes* utxo = NULL;
    as_bytes* existing_spending_data = NULL;
    as_map* error_response = NULL;

    if (utxo_get_and_validate(utxos, offset, utxo_hash, &utxo, &existing_spending_data, &error_response) != 0) {
        return (as_val*)error_response;
    }

    // Check that UTXO is frozen (required for reassignment)
    // Unspent UTXOs (size=32) or spent UTXOs (size=68 with non-frozen data) are not frozen
    if (as_bytes_size(utxo) != FULL_UTXO_SIZE || existing_spending_data == NULL || !utxo_is_frozen(existing_spending_data)) {
        if (existing_spending_data) as_bytes_destroy(existing_spending_data);
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXO_NOT_FROZEN, ERR_UTXO_NOT_FROZEN);
    }

    // Create new UTXO with new hash (unspent)
    as_bytes* new_utxo = utxo_create_with_spending_data(new_utxo_hash, NULL);
    if (new_utxo == NULL) {
        as_bytes_destroy(existing_spending_data);
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    as_list_set(utxos, (uint32_t)offset, (as_val*)new_utxo);

    // Mark utxos bin as dirty to persist changes
    as_rec_set(rec, BIN_UTXOS, as_val_reserve((as_val*)utxos));

    // Initialize reassignments list if needed
    // Track whether we created the list (vs retrieved from record) for proper ref counting
    as_val* reassignments_val = as_rec_get(rec, BIN_REASSIGNMENTS);
    as_list* reassignments = NULL;
    bool reassignments_is_new = false;
    if (reassignments_val == NULL || as_val_type(reassignments_val) == AS_NIL) {
        reassignments = (as_list*)as_arraylist_new(10, 10);
        if (reassignments == NULL) {
            as_bytes_destroy(existing_spending_data);
            return (as_val*)utxo_create_error_response(
                ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
        }
        reassignments_is_new = true;
    } else {
        if (as_val_type(reassignments_val) != AS_LIST) {
            as_bytes_destroy(existing_spending_data);
            return (as_val*)utxo_create_error_response(
                ERROR_CODE_INVALID_PARAMETER, "Invalid reassignments list");
        }
        reassignments = as_list_fromval(reassignments_val);
    }

    // Initialize utxoSpendableIn map if needed
    as_val* spendable_in_val = as_rec_get(rec, BIN_UTXO_SPENDABLE_IN);
    as_map* spendable_in = NULL;
    bool spendable_in_is_new = false;
    if (spendable_in_val == NULL || as_val_type(spendable_in_val) == AS_NIL) {
        spendable_in = (as_map*)as_hashmap_new(10);
        if (spendable_in == NULL) {
            if (reassignments_is_new) as_list_destroy(reassignments);
            as_bytes_destroy(existing_spending_data);
            return (as_val*)utxo_create_error_response(
                ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
        }
        spendable_in_is_new = true;
    } else {
        if (as_val_type(spendable_in_val) != AS_MAP) {
            if (reassignments_is_new) as_list_destroy(reassignments);
            as_bytes_destroy(existing_spending_data);
            return (as_val*)utxo_create_error_response(
                ERROR_CODE_INVALID_PARAMETER, "Invalid spendable_in map");
        }
        spendable_in = as_map_fromval(spendable_in_val);
    }

    // Record reassignment details
    as_hashmap* reassignment_entry = as_hashmap_new(4);
    if (reassignment_entry == NULL) {
        if (reassignments_is_new) as_list_destroy(reassignments);
        if (spendable_in_is_new) as_map_destroy(spendable_in);
        as_bytes_destroy(existing_spending_data);
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    as_hashmap_set(reassignment_entry,
        (as_val*)as_string_new((char*)"offset", false),
        (as_val*)as_integer_new(offset));
    as_hashmap_set(reassignment_entry,
        (as_val*)as_string_new((char*)"utxoHash", false),
        as_val_reserve((as_val*)utxo_hash));
    as_hashmap_set(reassignment_entry,
        (as_val*)as_string_new((char*)"newUtxoHash", false),
        as_val_reserve((as_val*)new_utxo_hash));
    as_hashmap_set(reassignment_entry,
        (as_val*)as_string_new((char*)"blockHeight", false),
        (as_val*)as_integer_new(block_height));

    as_list_append(reassignments, (as_val*)reassignment_entry);
    // For new lists, don't use as_val_reserve (record takes ownership)
    // For existing lists, use as_val_reserve to mark dirty without double-free
    if (reassignments_is_new) {
        as_rec_set(rec, BIN_REASSIGNMENTS, (as_val*)reassignments);
    } else {
        as_rec_set(rec, BIN_REASSIGNMENTS, as_val_reserve((as_val*)reassignments));
    }

    // Set spendable height
    as_map_set(spendable_in,
        (as_val*)as_integer_new(offset),
        (as_val*)as_integer_new(block_height + spendable_after));
    // For new maps, don't use as_val_reserve (record takes ownership)
    // For existing maps, use as_val_reserve to mark dirty without double-free
    if (spendable_in_is_new) {
        as_rec_set(rec, BIN_UTXO_SPENDABLE_IN, (as_val*)spendable_in);
    } else {
        as_rec_set(rec, BIN_UTXO_SPENDABLE_IN, as_val_reserve((as_val*)spendable_in));
    }


    // Increment recordUtxos to prevent deletion
    as_val* record_utxos_val = as_rec_get(rec, BIN_RECORD_UTXOS);
    int64_t record_utxos = 0;
    if (record_utxos_val != NULL && as_val_type(record_utxos_val) == AS_INTEGER) {
        record_utxos = as_integer_get(as_integer_fromval(record_utxos_val));
    }
    as_rec_set(rec, BIN_RECORD_UTXOS, (as_val*)as_integer_new(record_utxos + 1));

    as_bytes_destroy(existing_spending_data);

    as_map* ok_response = utxo_create_ok_response();
    if (ok_response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    return (as_val*)ok_response;
}

as_val*
teranode_set_conflicting(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    bool set_value = get_list_bool(args, 0);
    int64_t current_block_height = get_list_int64(args, 1);
    int64_t block_height_retention = get_list_int64(args, 2);

    // Set conflicting flag
    as_rec_set(rec, BIN_CONFLICTING, (as_val*)as_boolean_new(set_value));

    // Call setDeleteAtHeight
    int64_t child_count = 0;
    const char* signal = utxo_set_delete_at_height_impl(rec, current_block_height, block_height_retention, &child_count);

    // Build response
    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();
    if (response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }

    if (signal != NULL && strlen(signal) > 0) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_SIGNAL, false),
            (as_val*)as_string_new((char*)signal, false));
        if (child_count > 0) {
            as_hashmap_set(response,
                (as_val*)as_string_new((char*)FIELD_CHILD_COUNT, false),
                (as_val*)as_integer_new(child_count));
        }
    }

    return (as_val*)response;
}

as_val*
teranode_preserve_until(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    int64_t block_height = get_list_int64(args, 0);

    // Remove deleteAtHeight if it exists
    as_rec_set(rec, BIN_DELETE_AT_HEIGHT, (as_val*)&as_nil);

    // Set preserveUntil
    as_rec_set(rec, BIN_PRESERVE_UNTIL, (as_val*)as_integer_new(block_height));

    // Build response
    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();
    if (response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }

    // Check if we need to signal external file handling
    as_val* external_val = as_rec_get(rec, BIN_EXTERNAL);
    if (external_val != NULL && as_val_type(external_val) != AS_NIL) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_SIGNAL, false),
            (as_val*)as_string_new((char*)SIGNAL_PRESERVE, false));
    }

    return (as_val*)response;
}

as_val*
teranode_set_locked(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    bool set_value = get_list_bool(args, 0);

    // Get totalExtraRecs (default to 0)
    as_val* total_extra_recs_val = as_rec_get(rec, BIN_TOTAL_EXTRA_RECS);
    int64_t total_extra_recs = 0;
    if (total_extra_recs_val != NULL && as_val_type(total_extra_recs_val) == AS_INTEGER) {
        total_extra_recs = as_integer_get(as_integer_fromval(total_extra_recs_val));
    }

    // Set locked flag
    as_rec_set(rec, BIN_LOCKED, (as_val*)as_boolean_new(set_value));

    // Remove deleteAtHeight when locking
    if (set_value) {
        as_val* existing_dah_val = as_rec_get(rec, BIN_DELETE_AT_HEIGHT);
        if (existing_dah_val != NULL && as_val_type(existing_dah_val) != AS_NIL) {
            as_rec_set(rec, BIN_DELETE_AT_HEIGHT, (as_val*)&as_nil);
        }
    }

    // Build response
    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();
    if (response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    as_hashmap_set(response,
        (as_val*)as_string_new((char*)FIELD_CHILD_COUNT, false),
        (as_val*)as_integer_new(total_extra_recs));

    return (as_val*)response;
}

as_val*
teranode_increment_spent_extra_recs(as_rec* rec, as_list* args)
{
    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    int64_t inc = get_list_int64(args, 0);
    int64_t current_block_height = get_list_int64(args, 1);
    int64_t block_height_retention = get_list_int64(args, 2);

    // Check totalExtraRecs exists
    as_val* total_extra_recs_val = as_rec_get(rec, BIN_TOTAL_EXTRA_RECS);
    if (total_extra_recs_val == NULL || as_val_type(total_extra_recs_val) != AS_INTEGER) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_INVALID_PARAMETER, ERR_TOTAL_EXTRA_RECS);
    }
    int64_t total_extra_recs = as_integer_get(as_integer_fromval(total_extra_recs_val));

    // Get spentExtraRecs (default to 0)
    as_val* spent_extra_recs_val = as_rec_get(rec, BIN_SPENT_EXTRA_RECS);
    int64_t spent_extra_recs = 0;
    if (spent_extra_recs_val != NULL && as_val_type(spent_extra_recs_val) == AS_INTEGER) {
        spent_extra_recs = as_integer_get(as_integer_fromval(spent_extra_recs_val));
    }

    // Increment
    spent_extra_recs += inc;

    // Validate bounds
    if (spent_extra_recs < 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_INVALID_PARAMETER, ERR_SPENT_EXTRA_RECS_NEGATIVE);
    }

    if (spent_extra_recs > total_extra_recs) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_INVALID_PARAMETER, ERR_SPENT_EXTRA_RECS_EXCEED);
    }

    // Update bin
    as_rec_set(rec, BIN_SPENT_EXTRA_RECS, (as_val*)as_integer_new(spent_extra_recs));

    // Call setDeleteAtHeight
    int64_t child_count = 0;
    const char* signal = utxo_set_delete_at_height_impl(rec, current_block_height, block_height_retention, &child_count);

    // Build response
    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();
    if (response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }

    if (signal != NULL && strlen(signal) > 0) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_SIGNAL, false),
            (as_val*)as_string_new((char*)signal, false));
        if (child_count > 0) {
            as_hashmap_set(response,
                (as_val*)as_string_new((char*)FIELD_CHILD_COUNT, false),
                (as_val*)as_integer_new(child_count));
        }
    }

    return (as_val*)response;
}

as_val*
teranode_set_delete_at_height(as_rec* rec, as_list* args)
{
    int64_t current_block_height = get_list_int64(args, 0);
    int64_t block_height_retention = get_list_int64(args, 1);

    int64_t child_count = 0;
    const char* signal = utxo_set_delete_at_height_impl(rec, current_block_height, block_height_retention, &child_count);

    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();
    if (response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }

    if (signal != NULL && strlen(signal) > 0) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_SIGNAL, false),
            (as_val*)as_string_new((char*)signal, false));
        if (child_count > 0) {
            as_hashmap_set(response,
                (as_val*)as_string_new((char*)FIELD_CHILD_COUNT, false),
                (as_val*)as_integer_new(child_count));
        }
    }

    return (as_val*)response;
}
