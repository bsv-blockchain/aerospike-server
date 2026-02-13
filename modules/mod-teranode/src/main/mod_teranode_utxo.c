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

// Fast hex conversion lookup table (used by byte_to_hex).
static const char hex_digits[16] = "0123456789abcdef";

/**
 * Convert a single byte to two hex characters.
 * Uses a lookup table instead of sprintf for ~10x faster conversion.
 *
 * @param byte  The byte value to convert.
 * @param out   Output buffer; must have room for at least 2 chars (no NUL).
 */
static inline void
byte_to_hex(uint8_t byte, char* out)
{
    out[0] = hex_digits[byte >> 4];
    out[1] = hex_digits[byte & 0x0F];
}

/**
 * Lazy-initialize an errors hashmap and insert a key-value pair.
 *
 * Allocates the hashmap on first call. If the hashmap or the insert
 * allocation fails, the key and value are destroyed and false is returned.
 *
 * @param errors  Pointer to the hashmap pointer (initially NULL).
 * @param key     Key to insert (ownership transferred to map on success).
 * @param value   Value to insert (ownership transferred to map on success).
 * @return true on success, false on allocation failure.
 */
static inline bool
errors_set(as_hashmap** errors, as_val* key, as_val* value)
{
    if (*errors == NULL) {
        *errors = as_hashmap_new(8);
        if (*errors == NULL) {
            as_val_destroy(key);
            as_val_destroy(value);
            return false;
        }
    }
    as_hashmap_set(*errors, key, value);
    return true;
}

/**
 * Safely extract a boolean value from an as_list at the given index.
 * Returns false if the list is NULL, the index is out of range, or
 * the value at that index is not AS_BOOLEAN.
 */
static inline bool get_list_bool(as_list* list, uint32_t index) {
    if (list == NULL) return false;
    as_val* val = as_list_get(list, index);
    if (val == NULL) return false;
    if (as_val_type(val) != AS_BOOLEAN) return false;
    as_boolean* b = as_boolean_fromval(val);
    if (b == NULL) return false;
    return as_boolean_get(b);
}

/**
 * Safely extract an int64 value from an as_list at the given index.
 * Returns 0 if the list is NULL, the index is out of range, or
 * the value at that index is not AS_INTEGER.
 */
static inline int64_t get_list_int64(as_list* list, uint32_t index) {
    if (list == NULL) return 0;
    as_val* val = as_list_get(list, index);
    if (val == NULL) return 0;
    if (as_val_type(val) != AS_INTEGER) return 0;
    as_integer* i = as_integer_fromval(val);
    if (i == NULL) return 0;
    return as_integer_get(i);
}

/**
 * Fast-path: create a 68-byte spent UTXO from raw hash and spending data
 * pointers. Bypasses the validation in utxo_create_with_spending_data since
 * callers have already validated sizes via utxo_get_and_validate.
 *
 * @param hash_data     Pointer to UTXO_HASH_SIZE (32) bytes.
 * @param spending_ptr  Pointer to SPENDING_DATA_SIZE (36) bytes.
 * @return Newly allocated 68-byte as_bytes, or NULL on failure.
 */
static inline as_bytes*
utxo_create_spent(const uint8_t* hash_data, const uint8_t* spending_ptr)
{
    as_bytes* new_utxo = as_bytes_new(FULL_UTXO_SIZE);
    if (new_utxo == NULL) {
        return NULL;
    }

    uint8_t* dst = new_utxo->value;
    memcpy(dst, hash_data, UTXO_HASH_SIZE);
    memcpy(dst + UTXO_HASH_SIZE, spending_ptr, SPENDING_DATA_SIZE);
    new_utxo->size = FULL_UTXO_SIZE;
    return new_utxo;
}

/**
 * Safely extract an as_bytes pointer from an as_list at the given index.
 * Returns NULL if the list is NULL, the index is out of range, or
 * the value at that index is not AS_BYTES.
 */
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

/**
 * Compare two as_bytes objects for byte-level equality.
 * Two NULLs are considered equal. A NULL and non-NULL are not equal.
 * Returns false if either bytes object has NULL internal data.
 */
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

/**
 * Check whether a UTXO's spending data represents the "frozen" state.
 * A frozen UTXO has all SPENDING_DATA_SIZE (36) bytes set to 0xFF.
 *
 * Uses integer comparisons (4x uint64_t + 1x uint32_t) for the full
 * 36 bytes. Short-circuit evaluation rejects non-frozen data on the
 * first mismatch. The memcpy calls are optimized to register loads
 * by the compiler at -O2.
 *
 * @param spending_data  Pointer to SPENDING_DATA_SIZE bytes, or NULL.
 * @return true if all 36 bytes are 0xFF, false otherwise.
 */
bool
utxo_is_frozen(const uint8_t* spending_data)
{
    if (spending_data == NULL) {
        return false;
    }

    uint64_t a, b, c, d;
    uint32_t e;
    memcpy(&a, spending_data,      8);
    memcpy(&b, spending_data + 8,  8);
    memcpy(&c, spending_data + 16, 8);
    memcpy(&d, spending_data + 24, 8);
    memcpy(&e, spending_data + 32, 4);

    return a == UINT64_MAX && b == UINT64_MAX &&
           c == UINT64_MAX && d == UINT64_MAX && e == UINT32_MAX;
}

/**
 * Allocate a new UTXO bytes object, optionally appending spending data.
 *
 * If spending_data is NULL, creates a 32-byte "unspent" UTXO containing
 * only the hash. If spending_data is provided, creates a 68-byte "spent"
 * UTXO (hash + spending data). The caller owns the returned object.
 *
 * @param utxo_hash      32-byte UTXO hash. Must not be NULL.
 * @param spending_data   36-byte spending data, or NULL for unspent.
 * @return Newly allocated as_bytes, or NULL on failure.
 */
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

/**
 * Look up a UTXO in the utxos list at the given offset and validate that
 * its hash matches expected_hash.
 *
 * On success (return 0):
 *   - *out_utxo points to the UTXO bytes object in the list (not a copy).
 *   - *out_spending_data points into the UTXO's internal buffer at the
 *     spending data region (bytes 32..67), or NULL if the UTXO is unspent.
 *
 * On failure (return -1):
 *   - *out_error_response is set to a newly allocated error map.
 *
 * @param utxos             The utxos list from the record.
 * @param offset            0-based index into the list.
 * @param expected_hash     32-byte hash to verify against.
 * @param out_utxo          [out] The UTXO bytes object.
 * @param out_spending_data [out] Pointer to spending data, or NULL.
 * @param out_error_response [out] Error map on failure.
 * @return 0 on success, -1 on failure.
 */
int
utxo_get_and_validate(
    as_list* utxos,
    int64_t offset,
    as_bytes* expected_hash,
    as_bytes** out_utxo,
    const uint8_t** out_spending_data,
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

    as_bytes* utxo = as_bytes_fromval(val);

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

    // Return pointer to spending data within existing UTXO bytes (no allocation)
    if (utxo_size == FULL_UTXO_SIZE) {
        *out_spending_data = utxo_data + UTXO_HASH_SIZE;
    }

    return 0;
}

/**
 * Convert 36-byte spending data to a 72-character hex string.
 *
 * Format: the first 32 bytes (txID) are byte-reversed to big-endian,
 * followed by the 4-byte vin index in original little-endian order.
 * The result is a heap-allocated as_string (caller owns it).
 *
 * Allocates the hex buffer directly — avoids the extra memcpy of
 * building on stack then strdup-ing.
 *
 * @param spending_data  Pointer to SPENDING_DATA_SIZE bytes, or NULL.
 * @return Newly allocated as_string, or NULL on failure.
 */
as_string*
utxo_spending_data_to_hex(const uint8_t* spending_data)
{
    if (spending_data == NULL) {
        return NULL;
    }

    // Allocate directly — 64 + 8 + NUL = 73 bytes
    char* hex = malloc(73);
    if (hex == NULL) {
        return NULL;
    }

    // First 32 bytes reversed (txID)
    int pos = 0;
    for (int i = 31; i >= 0; i--) {
        byte_to_hex(spending_data[i], &hex[pos]);
        pos += 2;
    }

    // Next 4 bytes as-is (vin in little-endian)
    for (int i = 32; i < 36; i++) {
        byte_to_hex(spending_data[i], &hex[pos]);
        pos += 2;
    }

    hex[72] = '\0';
    as_string* s = as_string_new(hex, true);
    if (s == NULL) {
        free(hex);
        return NULL;
    }
    return s;
}

/**
 * Convert the txID portion (first 32 bytes) of spending data to a
 * 64-character hex string with bytes reversed to big-endian order.
 * Used for looking up child transactions in the deletedChildren map.
 *
 * @param spending_data  Pointer to at least 32 bytes, or NULL.
 * @return Newly allocated as_string, or NULL on failure.
 */
static as_string*
utxo_spending_data_to_txid_hex(const uint8_t* spending_data)
{
    if (spending_data == NULL) {
        return NULL;
    }

    // Allocate directly — 64 + NUL = 65 bytes
    char* hex = malloc(65);
    if (hex == NULL) {
        return NULL;
    }

    // First 32 bytes reversed (txID)
    int pos = 0;
    for (int i = 31; i >= 0; i--) {
        byte_to_hex(spending_data[i], &hex[pos]);
        pos += 2;
    }

    hex[64] = '\0';
    as_string* s = as_string_new(hex, true);
    if (s == NULL) {
        free(hex);
        return NULL;
    }
    return s;
}

/**
 * Create a standard error response map with status="ERROR", the given
 * error code, and a human-readable message. The message is strdup'd so
 * the caller may pass a stack buffer.
 *
 * @param error_code  Error code string (e.g. "UTXO_NOT_FOUND").
 * @param message     Human-readable error message.
 * @return Newly allocated as_map, or NULL on allocation failure.
 */
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

/**
 * Append a "spendingData" hex field to an existing error response map.
 * No-op if err or spending_data is NULL.
 */
static inline void
error_response_add_spending_data(as_map* err, const uint8_t* spending_data)
{
    if (err == NULL || spending_data == NULL) {
        return;
    }
    as_string* hexstr = utxo_spending_data_to_hex(spending_data);
    if (hexstr != NULL) {
        as_string* k_sd = as_string_new((char*)FIELD_SPENDING_DATA, false);
        if (k_sd != NULL) {
            as_hashmap_set((as_hashmap*)err, (as_val*)k_sd, (as_val*)hexstr);
        } else {
            as_string_destroy(hexstr);
        }
    }
}

/**
 * Create a standard success response map with status="OK".
 *
 * @return Newly allocated as_map, or NULL on allocation failure.
 */
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

/**
 * Evaluate whether a record should be scheduled for deletion at a future
 * block height. This is the shared logic called by spend, unspend,
 * setMined, setConflicting, and incrementSpentExtraRecs after they
 * modify record state.
 *
 * Decision logic:
 *   - If block_height_retention == 0 or preserveUntil is set, no-op.
 *   - If the transaction is conflicting, set deleteAtHeight immediately.
 *   - For child records (no totalExtraRecs): signal ALLSPENT/NOTALLSPENT
 *     when the spent state changes.
 *   - For master records: set deleteAtHeight when all UTXOs and child
 *     records are fully spent, the transaction is mined, and it's on
 *     the longest chain. Clear deleteAtHeight if conditions are no longer
 *     met.
 *
 * Returns a signal string (DAHSET, DAHUNSET, ALLSPENT, NOTALLSPENT)
 * or "" if no signal. Sets *out_child_count for external records.
 *
 * Note: teranode_set_mined uses an inlined version of this logic for
 * performance, passing pre-fetched values to avoid redundant bin reads.
 * Any changes here must be mirrored there.
 */
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
// Per-UTXO spend helper.
//

typedef enum {
    SPEND_OK,     // UTXO was successfully spent
    SPEND_SKIP,   // UTXO was already spent with same data (idempotent)
    SPEND_ERROR   // Error occurred, out_error is set
} spend_result_t;

/**
 * Core per-UTXO spend logic extracted for direct use by both
 * teranode_spend (single) and teranode_spend_multi (batch).
 *
 * On SPEND_ERROR, *out_error is set to a newly allocated error map.
 * On SPEND_OK/SPEND_SKIP, *out_error is NULL.
 */
static spend_result_t
spend_single_utxo(
    as_list* utxos,
    as_map* deleted_children,
    as_map* spendable_in,
    int64_t offset,
    as_bytes* utxo_hash,
    as_bytes* spending_data,
    int64_t current_block_height,
    as_map** out_error)
{
    *out_error = NULL;

    // Validate UTXO
    as_bytes* utxo = NULL;
    const uint8_t* existing_spending_data = NULL;
    as_map* error_response = NULL;

    if (utxo_get_and_validate(utxos, offset, utxo_hash, &utxo, &existing_spending_data, &error_response) != 0) {
        *out_error = error_response;
        return SPEND_ERROR;
    }

    // Check spendable_in
    if (spendable_in != NULL) {
        as_integer k_off;
        as_integer_init(&k_off, offset);
        as_val* spendable_val = as_map_get(spendable_in, (as_val*)&k_off);
        if (spendable_val != NULL && as_val_type(spendable_val) == AS_INTEGER) {
            int64_t spendable_height = as_integer_get(as_integer_fromval(spendable_val));
            if (spendable_height >= current_block_height) {
                char msg[256];
                snprintf(msg, sizeof(msg), "%s%lld", MSG_FROZEN_UNTIL, (long long)spendable_height);
                *out_error = utxo_create_error_response(ERROR_CODE_FROZEN_UNTIL, msg);
                return SPEND_ERROR;
            }
        }
    }

    // Handle already spent UTXO
    if (existing_spending_data != NULL) {
        const uint8_t* new_spending_ptr = as_bytes_get(spending_data);
        if (new_spending_ptr != NULL &&
                memcmp(existing_spending_data, new_spending_ptr, SPENDING_DATA_SIZE) == 0) {
            // Already spent with same data - check if child tx was deleted
            if (deleted_children != NULL) {
                as_string* child_txid = utxo_spending_data_to_txid_hex(existing_spending_data);
                if (child_txid != NULL) {
                    as_val* deleted_val = as_map_get(deleted_children, (as_val*)child_txid);
                    if (deleted_val != NULL && as_val_type(deleted_val) != AS_NIL) {
                        as_string_destroy(child_txid);
                        *out_error = utxo_create_error_response(ERROR_CODE_INVALID_SPEND, MSG_INVALID_SPEND);
                        error_response_add_spending_data(*out_error, existing_spending_data);
                        return SPEND_ERROR;
                    }
                    as_string_destroy(child_txid);
                }
            }
            return SPEND_SKIP;
        } else if (utxo_is_frozen(existing_spending_data)) {
            *out_error = utxo_create_error_response(ERROR_CODE_FROZEN, MSG_FROZEN);
            return SPEND_ERROR;
        } else {
            // Spent by different transaction
            *out_error = utxo_create_error_response(ERROR_CODE_SPENT, MSG_SPENT);
            error_response_add_spending_data(*out_error, existing_spending_data);
            return SPEND_ERROR;
        }
    }

    // Create new UTXO with spending data — use fast-path since we already
    // validated sizes via utxo_get_and_validate above
    const uint8_t* hash_ptr = as_bytes_get(utxo_hash);
    const uint8_t* spend_ptr = as_bytes_get(spending_data);
    if (hash_ptr == NULL || spend_ptr == NULL) {
        *out_error = utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Failed to get byte data");
        return SPEND_ERROR;
    }
    as_bytes* new_utxo = utxo_create_spent(hash_ptr, spend_ptr);
    if (new_utxo == NULL) {
        *out_error = utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
        return SPEND_ERROR;
    }
    as_list_set(utxos, (uint32_t)offset, (as_val*)new_utxo);

    // Caller is responsible for batching the BIN_SPENT_UTXOS increment
    return SPEND_OK;
}

//==========================================================
// UTXO Function Implementations.
//

/**
 * Mark a single UTXO as spent.
 *
 * Performs all pre-checks (creating, conflicting, locked, coinbase
 * maturity) inline, then delegates to spend_single_utxo() for the
 * per-UTXO logic. This avoids the marshalling overhead of packing
 * arguments into hashmaps/lists that the old teranode_spend_multi
 * delegation path required (~15 heap allocations eliminated).
 *
 * Response map always contains "status" ("OK" or "ERROR").
 * On error, contains "errors" map keyed by UTXO index (always 0).
 * On success, may contain "blockIDs", "signal", and "childCount".
 */
as_val*
teranode_spend(as_rec* rec, as_list* args, as_aerospike* as)
{
    // Check aerospike context
    if (as == NULL) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_INVALID_PARAMETER, "aerospike context is NULL");
    }

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

    // Pre-checks (same as spend_multi but return directly)
    as_val* creating_val = as_rec_get(rec, BIN_CREATING);
    if (creating_val != NULL && as_val_type(creating_val) == AS_BOOLEAN) {
        if (as_boolean_get(as_boolean_fromval(creating_val))) {
            return (as_val*)utxo_create_error_response(ERROR_CODE_CREATING, MSG_CREATING);
        }
    }

    if (!ignore_conflicting) {
        as_val* conflicting_val = as_rec_get(rec, BIN_CONFLICTING);
        if (conflicting_val != NULL && as_val_type(conflicting_val) == AS_BOOLEAN) {
            if (as_boolean_get(as_boolean_fromval(conflicting_val))) {
                return (as_val*)utxo_create_error_response(ERROR_CODE_CONFLICTING, MSG_CONFLICTING);
            }
        }
    }

    if (!ignore_locked) {
        as_val* locked_val = as_rec_get(rec, BIN_LOCKED);
        if (locked_val != NULL && as_val_type(locked_val) == AS_BOOLEAN) {
            if (as_boolean_get(as_boolean_fromval(locked_val))) {
                return (as_val*)utxo_create_error_response(ERROR_CODE_LOCKED, MSG_LOCKED);
            }
        }
    }

    as_val* spending_height_val = as_rec_get(rec, BIN_SPENDING_HEIGHT);
    if (spending_height_val != NULL && as_val_type(spending_height_val) == AS_INTEGER) {
        int64_t coinbase_spending_height = as_integer_get(as_integer_fromval(spending_height_val));
        if (coinbase_spending_height > 0 && coinbase_spending_height > current_block_height) {
            char msg[256];
            snprintf(msg, sizeof(msg), "%s, spendable in block %lld or greater. Current block height is %lld",
                MSG_COINBASE_IMMATURE, (long long)coinbase_spending_height, (long long)current_block_height);
            return (as_val*)utxo_create_error_response(ERROR_CODE_COINBASE_IMMATURE, msg);
        }
    }

    // Get utxos bin
    as_val* utxos_val = as_rec_get(rec, BIN_UTXOS);
    if (utxos_val == NULL || as_val_type(utxos_val) != AS_LIST) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXOS_NOT_FOUND, ERR_UTXOS_NOT_FOUND);
    }
    as_list* utxos = as_list_fromval(utxos_val);

    // Fetch deleted_children and spendable_in
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

    // Call spend_single_utxo directly — no hashmap/list marshalling
    as_map* spend_error = NULL;
    spend_result_t result = spend_single_utxo(utxos, deleted_children,
        spendable_in, offset, utxo_hash, spending_data, current_block_height,
        &spend_error);

    // Batch the spent counter increment (single read+write instead of per-UTXO)
    if (result == SPEND_OK) {
        as_val* spent_count_val = as_rec_get(rec, BIN_SPENT_UTXOS);
        int64_t spent_count = 0;
        if (spent_count_val != NULL && as_val_type(spent_count_val) == AS_INTEGER) {
            spent_count = as_integer_get(as_integer_fromval(spent_count_val));
        }
        as_rec_set(rec, BIN_SPENT_UTXOS, (as_val*)as_integer_new(spent_count + 1));
    }

    // Mark utxos bin as dirty to persist changes
    as_rec_set(rec, BIN_UTXOS, as_val_reserve((as_val*)utxos));

    // Call setDeleteAtHeight
    int64_t child_count = 0;
    const char* signal = utxo_set_delete_at_height_impl(rec, current_block_height, block_height_retention, &child_count);

    // Commit record changes
    int update_rv = as_aerospike_rec_update(as, rec);
    if (update_rv != 0) {
        if (spend_error != NULL) {
            as_val_destroy((as_val*)spend_error);
        }
        return (as_val*)utxo_create_error_response(ERROR_CODE_UPDATE_FAILED, ERR_UPDATE_FAILED);
    }

    // Build response — max 5 entries: status, errors, blockIDs, signal, childCount
    as_hashmap* response = as_hashmap_new(5);
    if (response == NULL) {
        if (spend_error != NULL) {
            as_val_destroy((as_val*)spend_error);
        }
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }

    if (result == SPEND_ERROR && spend_error != NULL) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_STATUS, false),
            (as_val*)as_string_new((char*)STATUS_ERROR, false));
        // Wrap single error in errors map keyed by index 0
        as_hashmap* errors_map = as_hashmap_new(1);
        if (errors_map != NULL) {
            as_hashmap_set(errors_map,
                (as_val*)as_integer_new(0),
                (as_val*)spend_error);
            as_hashmap_set(response,
                (as_val*)as_string_new((char*)FIELD_ERRORS, false),
                (as_val*)errors_map);
        } else {
            as_val_destroy((as_val*)spend_error);
        }
    } else {
        if (spend_error != NULL) {
            as_val_destroy((as_val*)spend_error);
        }
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_STATUS, false),
            (as_val*)as_string_new((char*)STATUS_OK, false));
    }

    as_val* response_block_ids_val = as_rec_get(rec, BIN_BLOCK_IDS);
    if (response_block_ids_val != NULL && as_val_type(response_block_ids_val) == AS_LIST) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_BLOCK_IDS, false),
            as_val_reserve(response_block_ids_val));
    }

    if (signal != NULL && signal[0] != '\0') {
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

/**
 * Mark multiple UTXOs as spent in a single operation.
 *
 * Args[0] is a list of spend-item maps, each containing:
 *   "offset" (int), "utxoHash" (bytes[32]), "spendingData" (bytes[36]),
 *   and optionally "idx" (int) for error reporting.
 *
 * Pre-checks (creating, conflicting, locked, coinbase) are evaluated
 * once and abort the entire batch. Per-UTXO errors (hash mismatch,
 * already spent, frozen) are accumulated in a lazy-allocated errors map
 * keyed by the item's index, while successful spends proceed.
 *
 * Response map contains "status" ("OK" if no per-UTXO errors, "ERROR"
 * otherwise). On error, contains "errors" map. May also contain
 * "blockIDs", "signal", and "childCount".
 */
as_val*
teranode_spend_multi(as_rec* rec, as_list* args, as_aerospike* as)
{
    // Check aerospike context
    if (as == NULL) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_INVALID_PARAMETER, "aerospike context is NULL");
    }

    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Max 5 entries: status, errors, blockIDs, signal, childCount
    as_hashmap* response = as_hashmap_new(5);
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
        if (locked_val != NULL && as_val_type(locked_val) == AS_BOOLEAN) {
            if (as_boolean_get(as_boolean_fromval(locked_val))) {
                as_val_destroy((as_val*)response);
                return (as_val*)utxo_create_error_response(ERROR_CODE_LOCKED, MSG_LOCKED);
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

    // Fetch deleted_children and spendable_in once before the loop.
    // These bins are never modified inside the loop (only BIN_SPENT_UTXOS is set),
    // so the pointers remain valid throughout iteration.
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

    // Process each spend — errors hashmap is lazy-allocated on first error
    as_hashmap* errors = NULL;
    uint32_t spends_size = as_list_size(spends);
    int64_t success_count = 0;

    for (uint32_t i = 0; i < spends_size; i++) {
        as_map* spend_item = as_map_fromval(as_list_get(spends, i));
        if (spend_item == NULL) continue;

        // Use stack-allocated as_string keys to avoid heap alloc churn
        as_string k_offset;
        as_string_init(&k_offset, (char*)"offset", false);
        as_val* v_offset = as_map_get(spend_item, (as_val*)&k_offset);
        if (v_offset == NULL || as_val_type(v_offset) != AS_INTEGER) {
            continue;
        }
        int64_t offset = as_integer_get(as_integer_fromval(v_offset));

        as_string k_utxo;
        as_string_init(&k_utxo, (char*)"utxoHash", false);
        as_val* v_utxo = as_map_get(spend_item, (as_val*)&k_utxo);
        if (v_utxo == NULL || as_val_type(v_utxo) != AS_BYTES) {
            continue;
        }
        as_bytes* utxo_hash = as_bytes_fromval(v_utxo);

        as_string k_sp;
        as_string_init(&k_sp, (char*)"spendingData", false);
        as_val* v_sp = as_map_get(spend_item, (as_val*)&k_sp);
        if (v_sp == NULL || as_val_type(v_sp) != AS_BYTES) {
            continue;
        }
        as_bytes* spending_data = as_bytes_fromval(v_sp);

        as_string k_idx;
        as_string_init(&k_idx, (char*)"idx", false);
        as_val* idx_val = as_map_get(spend_item, (as_val*)&k_idx);
        int64_t idx = (idx_val != NULL && as_val_type(idx_val) == AS_INTEGER) ? as_integer_get(as_integer_fromval(idx_val)) : i;

        as_map* spend_error = NULL;
        spend_result_t sr = spend_single_utxo(utxos, deleted_children,
            spendable_in, offset, utxo_hash, spending_data,
            current_block_height, &spend_error);

        if (sr == SPEND_OK) {
            success_count++;
        } else if (sr == SPEND_ERROR && spend_error != NULL) {
            errors_set(&errors,
                (as_val*)as_integer_new(idx),
                (as_val*)spend_error);
        }
    }

    // Batch the spent counter increment — single read+write for all successful spends
    if (success_count > 0) {
        as_val* spent_count_val = as_rec_get(rec, BIN_SPENT_UTXOS);
        int64_t spent_count = 0;
        if (spent_count_val != NULL && as_val_type(spent_count_val) == AS_INTEGER) {
            spent_count = as_integer_get(as_integer_fromval(spent_count_val));
        }
        as_rec_set(rec, BIN_SPENT_UTXOS, (as_val*)as_integer_new(spent_count + success_count));
    }

    // Mark utxos bin as dirty to persist changes
    as_rec_set(rec, BIN_UTXOS, as_val_reserve((as_val*)utxos));

    // Call setDeleteAtHeight
    int64_t child_count = 0;
    const char* signal = utxo_set_delete_at_height_impl(rec, current_block_height, block_height_retention, &child_count);

    // Commit record changes - equivalent to Lua's aerospike:update(rec)
    int update_rv = as_aerospike_rec_update(as, rec);
    if (update_rv != 0) {
        as_hashmap_destroy(response);
        if (errors != NULL) {
            as_hashmap_destroy(errors);
        }
        return (as_val*)utxo_create_error_response(ERROR_CODE_UPDATE_FAILED, ERR_UPDATE_FAILED);
    }

    // Re-fetch block_ids after as_rec_set calls (both above and inside setDeleteAtHeight)
    as_val* response_block_ids_val = as_rec_get(rec, BIN_BLOCK_IDS);

    // Build response
    if (errors != NULL && as_hashmap_size(errors) > 0) {
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
    }

    if (response_block_ids_val != NULL && as_val_type(response_block_ids_val) == AS_LIST) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_BLOCK_IDS, false),
            as_val_reserve(response_block_ids_val));
    }

    if (signal != NULL && signal[0] != '\0') {
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

/**
 * Reverse a spend operation — mark a UTXO as unspent.
 *
 * Replaces the 68-byte spent UTXO with a 32-byte unspent version
 * (hash only) and decrements the spentUtxos counter. Refuses to
 * unspend frozen UTXOs.
 *
 * No-op if the UTXO is already unspent (32 bytes).
 */
as_val*
teranode_unspend(as_rec* rec, as_list* args, as_aerospike* as)
{
    // Check aerospike context
    if (as == NULL) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_INVALID_PARAMETER, "aerospike context is NULL");
    }

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
    const uint8_t* existing_spending_data = NULL;
    as_map* error_response = NULL;

    if (utxo_get_and_validate(utxos, offset, utxo_hash, &utxo, &existing_spending_data, &error_response) != 0) {
        return (as_val*)error_response;
    }

    // Only unspend if spent and not frozen
    if (as_bytes_size(utxo) == FULL_UTXO_SIZE) {
        if (utxo_is_frozen(existing_spending_data)) {
            return (as_val*)utxo_create_error_response(ERROR_CODE_FROZEN, ERR_UTXO_IS_FROZEN);
        }

        // Create unspent UTXO
        as_bytes* new_utxo = utxo_create_with_spending_data(utxo_hash, NULL);
        if (new_utxo == NULL) {
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

    // Call setDeleteAtHeight
    int64_t child_count = 0;
    const char* signal = utxo_set_delete_at_height_impl(rec, current_block_height, block_height_retention, &child_count);

    // Commit record changes - equivalent to Lua's aerospike:update(rec)
    int update_rv = as_aerospike_rec_update(as, rec);
    if (update_rv != 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UPDATE_FAILED, ERR_UPDATE_FAILED);
    }

    // Build response
    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();
    if (response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }

    if (signal != NULL && signal[0] != '\0') {
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

/**
 * Track which block(s) a transaction is mined in, or remove a block
 * on reorg (unsetMined).
 *
 * Maintains three parallel lists (blockIDs, blockHeights, subtreeIdxs).
 * Adding a block is idempotent — duplicate blockIDs are ignored.
 * Also clears the locked and creating flags, manages unminedSince,
 * and evaluates deleteAtHeight eligibility.
 *
 * Performance: the setDeleteAtHeight logic is inlined here (rather than
 * calling utxo_set_delete_at_height_impl) to avoid ~10 redundant
 * as_rec_get calls by reusing already-known values (block_count,
 * is_on_longest_chain). Any changes to the DAH logic must be kept in
 * sync with utxo_set_delete_at_height_impl.
 */
as_val*
teranode_set_mined(as_rec* rec, as_list* args, as_aerospike* as)
{
    // Check aerospike context
    if (as == NULL) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_INVALID_PARAMETER, "aerospike context is NULL");
    }

    // Check record exists
    if (rec == NULL || as_rec_numbins(rec) == 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_TX_NOT_FOUND, ERR_TX_NOT_FOUND);
    }

    // Extract arguments
    int64_t block_id = get_list_int64(args, 0);
    int64_t block_height = get_list_int64(args, 1);
    int64_t subtree_idx = get_list_int64(args, 2);
    int64_t current_block_height = get_list_int64(args, 3);
    int64_t block_height_retention = get_list_int64(args, 4);
    bool on_longest_chain = get_list_bool(args, 5);
    bool unset_mined = get_list_bool(args, 6);

    // Pre-read locked and creating while the udf_record cache is still small.
    // These are checked/cleared later but reading them now avoids scanning a
    // larger cache after block list as_rec_set calls grow it.
    as_val* locked_val = as_rec_get(rec, BIN_LOCKED);
    as_val* creating_val = as_rec_get(rec, BIN_CREATING);

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

    // Track block count locally to avoid re-fetching block_ids from record
    uint32_t block_count = as_list_size(block_ids);

    if (unset_mined) {
        // Remove blockID and corresponding entries
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
            block_count--;
        }

        // Set bins on record to persist changes
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
            block_count++;
        }

        // Set bins on record to persist changes
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

    // Handle unminedSince — use locally tracked block_count, no re-fetch needed
    bool is_on_longest_chain;
    if (block_count > 0) {
        if (on_longest_chain) {
            as_rec_set(rec, BIN_UNMINED_SINCE, (as_val*)&as_nil);
        }
        is_on_longest_chain = on_longest_chain;
    } else {
        as_rec_set(rec, BIN_UNMINED_SINCE, (as_val*)as_integer_new(current_block_height));
        is_on_longest_chain = false;
    }

    // Clear locked flag if set — uses pre-read value, avoids cache scan.
    // as_false is a global const with count=0 (immortal): as_val_destroy is
    // a no-op, so it's safe to pass to as_rec_set without allocation.
    if (locked_val != NULL && as_val_type(locked_val) != AS_NIL) {
        as_rec_set(rec, BIN_LOCKED, (as_val*)&as_false);
    }

    // Clear creating flag if set — uses pre-read value
    if (creating_val != NULL && as_val_type(creating_val) != AS_NIL) {
        as_rec_set(rec, BIN_CREATING, (as_val*)&as_nil);
    }

    // Inline setDeleteAtHeight logic — avoids 10 as_rec_get calls by using
    // already-known values (block_count, is_on_longest_chain) and only reading
    // the bins that setMined hasn't already fetched.
    int64_t child_count = 0;
    const char* signal = "";

    if (block_height_retention != 0) {
        as_val* preserve_until_val = as_rec_get(rec, BIN_PRESERVE_UNTIL);

        if (preserve_until_val == NULL || as_val_type(preserve_until_val) == AS_NIL) {
            // Read bins needed for DAH logic that setMined doesn't already know
            as_val* total_extra_recs_val = as_rec_get(rec, BIN_TOTAL_EXTRA_RECS);
            as_val* existing_dah_val = as_rec_get(rec, BIN_DELETE_AT_HEIGHT);
            as_val* conflicting_val = as_rec_get(rec, BIN_CONFLICTING);
            as_val* external_val = as_rec_get(rec, BIN_EXTERNAL);

            int64_t new_delete_height = current_block_height + block_height_retention;
            bool has_external = (external_val != NULL && as_val_type(external_val) != AS_NIL);

            // Handle conflicting transactions
            if (conflicting_val != NULL && as_val_type(conflicting_val) == AS_BOOLEAN) {
                if (as_boolean_get(as_boolean_fromval(conflicting_val))) {
                    if (existing_dah_val == NULL || as_val_type(existing_dah_val) == AS_NIL) {
                        bool should_signal = false;
                        int64_t total_extra_recs = 0;
                        if (has_external && total_extra_recs_val != NULL && as_val_type(total_extra_recs_val) == AS_INTEGER) {
                            total_extra_recs = as_integer_get(as_integer_fromval(total_extra_recs_val));
                            should_signal = true;
                        }

                        as_rec_set(rec, BIN_DELETE_AT_HEIGHT, (as_val*)as_integer_new(new_delete_height));

                        if (should_signal) {
                            child_count = total_extra_recs;
                            signal = SIGNAL_DELETE_AT_HEIGHT_SET;
                        }
                    }
                    goto dah_done;
                }
            }

            // Handle pagination records (no totalExtraRecs = child record)
            if (total_extra_recs_val == NULL || as_val_type(total_extra_recs_val) == AS_NIL) {
                as_val* last_spent_state_val = as_rec_get(rec, BIN_LAST_SPENT_STATE);
                as_val* spent_utxos_val = as_rec_get(rec, BIN_SPENT_UTXOS);
                as_val* record_utxos_val = as_rec_get(rec, BIN_RECORD_UTXOS);

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
                    signal = current_state;
                }
                goto dah_done;
            }

            // Master record: check all conditions for deletion
            if (as_val_type(total_extra_recs_val) == AS_INTEGER) {
                int64_t total_extra_recs = as_integer_get(as_integer_fromval(total_extra_recs_val));
                child_count = total_extra_recs;

                as_val* spent_extra_recs_val = as_rec_get(rec, BIN_SPENT_EXTRA_RECS);
                as_val* spent_utxos_val = as_rec_get(rec, BIN_SPENT_UTXOS);
                as_val* record_utxos_val = as_rec_get(rec, BIN_RECORD_UTXOS);

                int64_t spent_extra_recs = 0;
                if (spent_extra_recs_val != NULL && as_val_type(spent_extra_recs_val) == AS_INTEGER) {
                    spent_extra_recs = as_integer_get(as_integer_fromval(spent_extra_recs_val));
                }

                int64_t spent_utxos = 0;
                int64_t record_utxos = 0;
                if (spent_utxos_val != NULL && as_val_type(spent_utxos_val) == AS_INTEGER) {
                    spent_utxos = as_integer_get(as_integer_fromval(spent_utxos_val));
                }
                if (record_utxos_val != NULL && as_val_type(record_utxos_val) == AS_INTEGER) {
                    record_utxos = as_integer_get(as_integer_fromval(record_utxos_val));
                }

                bool all_spent = (total_extra_recs == spent_extra_recs) && (spent_utxos == record_utxos);
                // Use locally-tracked block_count and is_on_longest_chain
                // instead of re-reading BIN_BLOCK_IDS and BIN_UNMINED_SINCE
                bool has_block_ids = (block_count > 0);

                if (all_spent && has_block_ids && is_on_longest_chain) {
                    int64_t existing_dah = 0;
                    if (existing_dah_val != NULL && as_val_type(existing_dah_val) == AS_INTEGER) {
                        existing_dah = as_integer_get(as_integer_fromval(existing_dah_val));
                    }

                    if (existing_dah == 0 || existing_dah < new_delete_height) {
                        as_rec_set(rec, BIN_DELETE_AT_HEIGHT, (as_val*)as_integer_new(new_delete_height));
                        if (has_external) {
                            signal = SIGNAL_DELETE_AT_HEIGHT_SET;
                        }
                    }
                } else if (existing_dah_val != NULL && as_val_type(existing_dah_val) != AS_NIL) {
                    as_rec_set(rec, BIN_DELETE_AT_HEIGHT, (as_val*)&as_nil);
                    if (has_external) {
                        signal = SIGNAL_DELETE_AT_HEIGHT_UNSET;
                    }
                }
            }
        }
    }
dah_done: ;

    // Commit record changes
    int update_rv = as_aerospike_rec_update(as, rec);
    if (update_rv != 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UPDATE_FAILED, ERR_UPDATE_FAILED);
    }

    // Build response — use block_ids we already hold (no re-fetch needed;
    // rec_update committed the cache, the list pointer is still valid)
    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();
    if (response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }

    if (block_count > 0) {
        as_hashmap_set(response,
            (as_val*)as_string_new((char*)FIELD_BLOCK_IDS, false),
            as_val_reserve((as_val*)block_ids));
    }

    if (signal[0] != '\0') {
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

/**
 * Freeze a UTXO to prevent spending.
 *
 * Sets the UTXO's spending data to all 0xFF bytes (the frozen pattern).
 * Only unspent UTXOs (32 bytes) can be frozen. Already-frozen UTXOs
 * return ALREADY_FROZEN; already-spent UTXOs return SPENT with the
 * existing spending data hex.
 */
as_val*
teranode_freeze(as_rec* rec, as_list* args, as_aerospike* as)
{
    // Check aerospike context
    if (as == NULL) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_INVALID_PARAMETER, "aerospike context is NULL");
    }

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
    const uint8_t* existing_spending_data = NULL;
    as_map* error_response = NULL;

    if (utxo_get_and_validate(utxos, offset, utxo_hash, &utxo, &existing_spending_data, &error_response) != 0) {
        return (as_val*)error_response;
    }

    // If UTXO has spending data, check if already frozen
    if (existing_spending_data != NULL) {
        if (utxo_is_frozen(existing_spending_data)) {
            return (as_val*)utxo_create_error_response(ERROR_CODE_ALREADY_FROZEN, MSG_ALREADY_FROZEN);
        } else {
            // Already spent by something else
            as_map* err = utxo_create_error_response(ERROR_CODE_SPENT, MSG_SPENT);
            as_string* hexstr = utxo_spending_data_to_hex(existing_spending_data);
            if (hexstr != NULL) {
                as_string* k_sd = as_string_new((char*)FIELD_SPENDING_DATA, false);
                if (k_sd != NULL) {
                    as_hashmap_set((as_hashmap*)err, (as_val*)k_sd, (as_val*)hexstr);
                } else {
                    as_string_destroy(hexstr);
                }
            }
            return (as_val*)err;
        }
    }

    // Check UTXO is unspent (size should be 32)
    if (as_bytes_size(utxo) != UTXO_HASH_SIZE) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXO_INVALID_SIZE, ERR_UTXO_INVALID_SIZE);
    }

    // Build frozen UTXO directly — avoids intermediate as_bytes allocation + double copy
    const uint8_t* hash_data = as_bytes_get(utxo_hash);
    if (hash_data == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Failed to get UTXO hash data");
    }

    as_bytes* new_utxo = as_bytes_new(FULL_UTXO_SIZE);
    if (new_utxo == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    as_bytes_set(new_utxo, 0, hash_data, UTXO_HASH_SIZE);
    memset(new_utxo->value + UTXO_HASH_SIZE, FROZEN_BYTE, SPENDING_DATA_SIZE);
    new_utxo->size = FULL_UTXO_SIZE;

    as_list_set(utxos, (uint32_t)offset, (as_val*)new_utxo);

    // Mark utxos bin as dirty to persist changes
    as_rec_set(rec, BIN_UTXOS, as_val_reserve((as_val*)utxos));

    // Commit record changes - equivalent to Lua's aerospike:update(rec)
    int update_rv = as_aerospike_rec_update(as, rec);
    if (update_rv != 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UPDATE_FAILED, ERR_UPDATE_FAILED);
    }

    as_map* ok_response = utxo_create_ok_response();
    if (ok_response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    return (as_val*)ok_response;
}

/**
 * Unfreeze a frozen UTXO, restoring it to the unspent state.
 *
 * Replaces the 68-byte frozen UTXO with a 32-byte unspent version.
 * Returns UTXO_NOT_FROZEN if the UTXO is not currently frozen.
 */
as_val*
teranode_unfreeze(as_rec* rec, as_list* args, as_aerospike* as)
{
    // Check aerospike context
    if (as == NULL) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_INVALID_PARAMETER, "aerospike context is NULL");
    }

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
    const uint8_t* existing_spending_data = NULL;
    as_map* error_response = NULL;

    if (utxo_get_and_validate(utxos, offset, utxo_hash, &utxo, &existing_spending_data, &error_response) != 0) {
        return (as_val*)error_response;
    }

    // Check that UTXO is actually frozen (size=68 with frozen pattern)
    // Unspent UTXOs (size=32) or spent UTXOs (size=68 with non-frozen data) are not frozen
    if (as_bytes_size(utxo) != FULL_UTXO_SIZE || existing_spending_data == NULL || !utxo_is_frozen(existing_spending_data)) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXO_NOT_FROZEN, ERR_UTXO_NOT_FROZEN);
    }

    // Create unspent UTXO (remove spending data)
    as_bytes* new_utxo = utxo_create_with_spending_data(utxo_hash, NULL);
    if (new_utxo == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    as_list_set(utxos, (uint32_t)offset, (as_val*)new_utxo);

    // Mark utxos bin as dirty to persist changes
    as_rec_set(rec, BIN_UTXOS, as_val_reserve((as_val*)utxos));

    // Commit record changes - equivalent to Lua's aerospike:update(rec)
    int update_rv = as_aerospike_rec_update(as, rec);
    if (update_rv != 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UPDATE_FAILED, ERR_UPDATE_FAILED);
    }

    as_map* ok_response = utxo_create_ok_response();
    if (ok_response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    return (as_val*)ok_response;
}

/**
 * Reassign a frozen UTXO to a new hash.
 *
 * Replaces the frozen UTXO's hash with newUtxoHash and returns it to
 * the unspent state. Records the reassignment in the "reassignments"
 * list and sets a "utxoSpendableIn" entry so the new UTXO cannot be
 * spent until blockHeight + spendableAfter. Also increments
 * recordUtxos to prevent premature record deletion.
 *
 * Returns UTXO_NOT_FROZEN if the UTXO is not currently frozen.
 */
as_val*
teranode_reassign(as_rec* rec, as_list* args, as_aerospike* as)
{
    // Check aerospike context
    if (as == NULL) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_INVALID_PARAMETER, "aerospike context is NULL");
    }

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
    const uint8_t* existing_spending_data = NULL;
    as_map* error_response = NULL;

    if (utxo_get_and_validate(utxos, offset, utxo_hash, &utxo, &existing_spending_data, &error_response) != 0) {
        return (as_val*)error_response;
    }

    // Check that UTXO is frozen (required for reassignment)
    // Unspent UTXOs (size=32) or spent UTXOs (size=68 with non-frozen data) are not frozen
    if (as_bytes_size(utxo) != FULL_UTXO_SIZE || existing_spending_data == NULL || !utxo_is_frozen(existing_spending_data)) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UTXO_NOT_FROZEN, ERR_UTXO_NOT_FROZEN);
    }

    // Create new UTXO with new hash (unspent)
    as_bytes* new_utxo = utxo_create_with_spending_data(new_utxo_hash, NULL);
    if (new_utxo == NULL) {
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
            return (as_val*)utxo_create_error_response(
                ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
        }
        reassignments_is_new = true;
    } else {
        if (as_val_type(reassignments_val) != AS_LIST) {
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
            return (as_val*)utxo_create_error_response(
                ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
        }
        spendable_in_is_new = true;
    } else {
        if (as_val_type(spendable_in_val) != AS_MAP) {
            if (reassignments_is_new) as_list_destroy(reassignments);
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

    // Commit record changes - equivalent to Lua's aerospike:update(rec)
    int update_rv = as_aerospike_rec_update(as, rec);
    if (update_rv != 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UPDATE_FAILED, ERR_UPDATE_FAILED);
    }

    as_map* ok_response = utxo_create_ok_response();
    if (ok_response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }
    return (as_val*)ok_response;
}

/**
 * Mark or unmark a transaction as conflicting.
 *
 * Sets the "conflicting" bin to the given boolean value, then
 * re-evaluates deleteAtHeight eligibility. Conflicting transactions
 * get deleteAtHeight set immediately (if not already set).
 */
as_val*
teranode_set_conflicting(as_rec* rec, as_list* args, as_aerospike* as)
{
    // Check aerospike context
    if (as == NULL) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_INVALID_PARAMETER, "aerospike context is NULL");
    }

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

    // Commit record changes - equivalent to Lua's aerospike:update(rec)
    int update_rv = as_aerospike_rec_update(as, rec);
    if (update_rv != 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UPDATE_FAILED, ERR_UPDATE_FAILED);
    }

    // Build response
    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();
    if (response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }

    if (signal != NULL && signal[0] != '\0') {
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

/**
 * Prevent a transaction's record from being deleted until a given
 * block height. Clears any existing deleteAtHeight and sets
 * preserveUntil. Emits the PRESERVE signal for external records.
 */
as_val*
teranode_preserve_until(as_rec* rec, as_list* args, as_aerospike* as)
{
    // Check aerospike context
    if (as == NULL) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_INVALID_PARAMETER, "aerospike context is NULL");
    }

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

    // Commit record changes - equivalent to Lua's aerospike:update(rec)
    int update_rv = as_aerospike_rec_update(as, rec);
    if (update_rv != 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UPDATE_FAILED, ERR_UPDATE_FAILED);
    }

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

/**
 * Lock or unlock a transaction from being spent.
 *
 * When locking (setValue=true), also clears any existing deleteAtHeight.
 * Always returns the totalExtraRecs count as "childCount" in the
 * response so the caller can propagate the lock to child records.
 */
as_val*
teranode_set_locked(as_rec* rec, as_list* args, as_aerospike* as)
{
    // Check aerospike context
    if (as == NULL) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_INVALID_PARAMETER, "aerospike context is NULL");
    }

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

    // Commit record changes - equivalent to Lua's aerospike:update(rec)
    int update_rv = as_aerospike_rec_update(as, rec);
    if (update_rv != 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UPDATE_FAILED, ERR_UPDATE_FAILED);
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

/**
 * Increment (or decrement) the spent extra records counter.
 *
 * The spentExtraRecs counter tracks how many child (pagination) records
 * have had all their UTXOs spent. It must stay within [0, totalExtraRecs].
 * After updating, re-evaluates deleteAtHeight eligibility.
 */
as_val*
teranode_increment_spent_extra_recs(as_rec* rec, as_list* args, as_aerospike* as)
{
    // Check aerospike context
    if (as == NULL) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_INVALID_PARAMETER, "aerospike context is NULL");
    }

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

    // Commit record changes - equivalent to Lua's aerospike:update(rec)
    int update_rv = as_aerospike_rec_update(as, rec);
    if (update_rv != 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UPDATE_FAILED, ERR_UPDATE_FAILED);
    }

    // Build response
    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();
    if (response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }

    if (signal != NULL && signal[0] != '\0') {
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

/**
 * Public entry point for setDeleteAtHeight — wraps the internal
 * utxo_set_delete_at_height_impl with argument extraction, record
 * commit, and response building. Typically called directly via the
 * module dispatch table rather than by other UTXO functions (which
 * call the _impl variant directly).
 */
as_val*
teranode_set_delete_at_height(as_rec* rec, as_list* args, as_aerospike* as)
{
    // Check aerospike context
    if (as == NULL) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_INVALID_PARAMETER, "aerospike context is NULL");
    }

    int64_t current_block_height = get_list_int64(args, 0);
    int64_t block_height_retention = get_list_int64(args, 1);

    int64_t child_count = 0;
    const char* signal = utxo_set_delete_at_height_impl(rec, current_block_height, block_height_retention, &child_count);

    // Commit record changes - equivalent to Lua's aerospike:update(rec)
    // Note: When called via dispatch table, this persists the changes
    int update_rv = as_aerospike_rec_update(as, rec);
    if (update_rv != 0) {
        return (as_val*)utxo_create_error_response(ERROR_CODE_UPDATE_FAILED, ERR_UPDATE_FAILED);
    }

    as_hashmap* response = (as_hashmap*)utxo_create_ok_response();
    if (response == NULL) {
        return (as_val*)utxo_create_error_response(
            ERROR_CODE_INVALID_PARAMETER, "Memory allocation failed");
    }

    if (signal != NULL && signal[0] != '\0') {
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
