/*
 * mock_record.c
 *
 * Copyright (C) 2024 Aerospike, Inc.
 *
 * Mock implementation of as_rec for testing.
 */

#include "mock_record.h"
#include <aerospike/as_arraylist.h>
#include <aerospike/as_bytes.h>
#include <aerospike/as_integer.h>
#include <aerospike/as_boolean.h>
#include <aerospike/as_nil.h>
#include <stdlib.h>
#include <string.h>

//==========================================================
// Mock record implementation.
//

typedef struct mock_rec_data_s {
    as_map* bins;  // Map of bin name -> as_val
} mock_rec_data;

static as_val*
mock_rec_get(const as_rec* rec, const char* name)
{
    mock_rec_data* data = (mock_rec_data*)rec->data;
    if (data == NULL || data->bins == NULL) {
        return NULL;
    }
    return as_map_get(data->bins, (as_val*)as_string_new((char*)name, false));
}

static int
mock_rec_set(const as_rec* rec, const char* name, const as_val* value)
{
    mock_rec_data* data = (mock_rec_data*)rec->data;
    if (data == NULL || data->bins == NULL) {
        return 1;
    }

    // Check if we're setting the same value that's already there
    // This avoids the double-free when as_map_set destroys the "old" value
    as_string* key = as_string_new((char*)name, false);
    as_val* existing = as_map_get(data->bins, (as_val*)key);
    if (existing == value) {
        // Same object - nothing to do, changes were made in-place
        as_string_destroy(key);
        return 0;
    }

    as_map_set(data->bins, (as_val*)key, (as_val*)value);
    return 0;
}

static int
mock_rec_remove(const as_rec* rec, const char* name)
{
    mock_rec_data* data = (mock_rec_data*)rec->data;
    if (data == NULL || data->bins == NULL) {
        return 1;
    }
    as_map_remove(data->bins, (as_val*)as_string_new((char*)name, false));
    return 0;
}

static uint32_t
mock_rec_ttl(const as_rec* rec)
{
    (void)rec;
    return 0;
}

static uint16_t
mock_rec_gen(const as_rec* rec)
{
    (void)rec;
    return 1;
}

static as_bytes*
mock_rec_digest(const as_rec* rec)
{
    (void)rec;
    return NULL;
}

static uint16_t
mock_rec_numbins(const as_rec* rec)
{
    mock_rec_data* data = (mock_rec_data*)rec->data;
    if (data == NULL || data->bins == NULL) {
        return 0;
    }
    return (uint16_t)as_map_size(data->bins);
}

static int
mock_rec_set_ttl(const as_rec* rec, uint32_t ttl)
{
    (void)rec;
    (void)ttl;
    return 0;
}

static int
mock_rec_drop_key(const as_rec* rec)
{
    (void)rec;
    return 0;
}

static const char*
mock_rec_setname(const as_rec* rec)
{
    (void)rec;
    return "test_set";
}

static as_val*
mock_rec_key(const as_rec* rec)
{
    (void)rec;
    return NULL;
}

static uint64_t
mock_rec_last_update_time(const as_rec* rec)
{
    (void)rec;
    return 0;
}

static const as_rec_hooks mock_rec_hooks = {
    .get = mock_rec_get,
    .set = mock_rec_set,
    .remove = mock_rec_remove,
    .ttl = mock_rec_ttl,
    .gen = mock_rec_gen,
    .digest = mock_rec_digest,
    .set_ttl = mock_rec_set_ttl,
    .drop_key = mock_rec_drop_key,
    .numbins = mock_rec_numbins,
    .setname = mock_rec_setname,
    .key = mock_rec_key,
    .last_update_time = mock_rec_last_update_time
};

as_rec*
mock_rec_new(void)
{
    as_rec* rec = (as_rec*)malloc(sizeof(as_rec));
    if (rec == NULL) {
        return NULL;
    }

    mock_rec_data* data = (mock_rec_data*)malloc(sizeof(mock_rec_data));
    if (data == NULL) {
        free(rec);
        return NULL;
    }

    data->bins = (as_map*)as_hashmap_new(32);

    rec->data = data;
    rec->hooks = &mock_rec_hooks;

    return rec;
}

void
mock_rec_destroy(as_rec* rec)
{
    if (rec == NULL) {
        return;
    }

    mock_rec_data* data = (mock_rec_data*)rec->data;
    if (data != NULL) {
        if (data->bins != NULL) {
            as_map_destroy(data->bins);
        }
        free(data);
    }

    free(rec);
}

void
mock_rec_init_utxos(as_rec* rec, uint32_t num_utxos)
{
    as_arraylist* utxos = as_arraylist_new(num_utxos, 0);

    // Create unspent UTXOs
    for (uint32_t i = 0; i < num_utxos; i++) {
        // Allocate heap memory for UTXO data
        uint8_t* data = (uint8_t*)malloc(32);
        for (uint32_t j = 0; j < 32; j++) {
            data[j] = (uint8_t)(i + j);
        }

        // Use as_bytes_new_wrap with free=true so it owns the memory
        as_bytes* utxo = as_bytes_new_wrap(data, 32, true);
        as_arraylist_append(utxos, (as_val*)utxo);
    }

    as_rec_set(rec, "utxos", (as_val*)utxos);
    as_rec_set(rec, "spentUtxos", (as_val*)as_integer_new(0));
    as_rec_set(rec, "recordUtxos", (as_val*)as_integer_new(num_utxos));
}

#ifdef __cplusplus
}
#endif
