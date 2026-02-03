/*
 * mod_teranode.c
 *
 * Copyright (C) 2024 Aerospike, Inc.
 *
 * Native TERANODE module implementation.
 * Implements as_module_hooks interface for UTXO management.
 */

#include <aerospike/mod_teranode.h>
#include <aerospike/mod_teranode_config.h>
#include <aerospike/mod_teranode_utxo.h>
#include <aerospike/as_aerospike.h>
#include <aerospike/as_module.h>
#include <aerospike/as_result.h>
#include <aerospike/as_udf_context.h>
#include <aerospike/as_val.h>
#include <aerospike/as_list.h>
#include <aerospike/as_string.h>

#include <pthread.h>
#include <string.h>
#include <stdbool.h>

#include "base/udf_record.h"
#include "internal.h"

//==========================================================
// Typedefs.
//

// Function pointer type for UTXO functions
typedef as_val* (*teranode_fn)(as_rec*, as_list*);

// Function table entry
typedef struct fn_entry_s {
    const char* name;
    teranode_fn fn;
} fn_entry;

//==========================================================
// Forward declarations.
//

static int teranode_update(as_module* m, as_module_event* e);
static int teranode_validate(as_module* m, as_aerospike* as,
    const char* filename, const char* content, uint32_t size,
    as_module_error* err);
static int teranode_apply_record(as_module* m, as_udf_context* ctx,
    const char* filename, const char* function,
    as_rec* rec, as_list* args, as_result* res);

//==========================================================
// Globals.
//

// Module configuration
static mod_teranode_config g_config;

// Read-write lock for thread safety
static pthread_rwlock_t g_lock = PTHREAD_RWLOCK_INITIALIZER;

// Function dispatch table
static fn_entry g_function_table[] = {
    { "spend",                  teranode_spend },
    { "spendMulti",             teranode_spend_multi },
    { "unspend",                teranode_unspend },
    { "setMined",               teranode_set_mined },
    { "freeze",                 teranode_freeze },
    { "unfreeze",               teranode_unfreeze },
    { "reassign",               teranode_reassign },
    { "setConflicting",         teranode_set_conflicting },
    { "preserveUntil",          teranode_preserve_until },
    { "setLocked",              teranode_set_locked },
    { "incrementSpentExtraRecs", teranode_increment_spent_extra_recs },
    { "setDeleteAtHeight",      teranode_set_delete_at_height },
    { NULL, NULL }  // Sentinel
};

// Module hooks
static const as_module_hooks mod_teranode_hooks = {
    .destroy        = NULL,
    .update         = teranode_update,
    .validate       = teranode_validate,
    .apply_record   = teranode_apply_record,
    .apply_stream   = NULL  // Stream operations not supported
};

// Global module instance
as_module mod_teranode = {
    .source = NULL,
    .hooks  = &mod_teranode_hooks
};

//==========================================================
// Public API - Locking.
//

void
mod_teranode_rdlock(void)
{
    pthread_rwlock_rdlock(&g_lock);
}

void
mod_teranode_wrlock(void)
{
    pthread_rwlock_wrlock(&g_lock);
}

void
mod_teranode_unlock(void)
{
    pthread_rwlock_unlock(&g_lock);
}

//==========================================================
// Module hooks implementation.
//

/**
 * Handle module lifecycle events.
 */
static int
teranode_update(as_module* m, as_module_event* e)
{
    (void)m;

    switch (e->type) {
    case AS_MODULE_EVENT_CONFIGURE:
        if (e->data.config != NULL) {
            memcpy(&g_config, e->data.config, sizeof(mod_teranode_config));
            LOG_INFO("mod-teranode configured: enabled=%d", g_config.enabled);
        }
        break;

    case AS_MODULE_EVENT_FILE_SCAN:
    case AS_MODULE_EVENT_FILE_ADD:
    case AS_MODULE_EVENT_FILE_REMOVE:
    case AS_MODULE_EVENT_CLEAR_CACHE:
        // No-op for native module - no files to manage
        break;

    default:
        LOG_WARN("mod-teranode: unknown event type %d", e->type);
        break;
    }

    return 0;
}

/**
 * Validate module content.
 * For native modules, always succeeds (no code to validate).
 */
static int
teranode_validate(as_module* m, as_aerospike* as,
    const char* filename, const char* content, uint32_t size,
    as_module_error* err)
{
    (void)m;
    (void)as;
    (void)filename;
    (void)content;
    (void)size;
    (void)err;

    // Native module - nothing to validate
    return 0;
}

/**
 * Apply a function to a record.
 * This is the main entry point for UDF execution.
 */
static int
teranode_apply_record(as_module* m, as_udf_context* ctx,
    const char* filename, const char* function,
    as_rec* rec, as_list* args, as_result* res)
{
    (void)m;
    (void)filename;

    if (function == NULL) {
        LOG_ERROR("mod-teranode: function name is NULL");
        as_result_setfailure(res, (as_val*)as_string_new("function name required", false));
        return -1;
    }

    // Find function in dispatch table
    teranode_fn fn = NULL;

    for (fn_entry* entry = g_function_table; entry->name != NULL; entry++) {
        if (strcmp(entry->name, function) == 0) {
            fn = entry->fn;
            break;
        }
    }

    if (fn == NULL) {
        LOG_ERROR("mod-teranode: unknown function '%s'", function);
        char errmsg[256];
        snprintf(errmsg, sizeof(errmsg), "unknown function: %s", function);
        // Use strdup since errmsg is on stack
        as_result_setfailure(res, (as_val*)as_string_new(strdup(errmsg), true));
        return -1;
    }

    // Execute the function
    as_val* result = fn(rec, args);

    if (result == NULL) {
        LOG_ERROR("mod-teranode: function '%s' returned NULL", function);
        as_result_setfailure(res, (as_val*)as_string_new("function returned NULL", false));
        return -1;
    }

    // Check if the result indicates an error - don't commit changes on error
    // The result is a map with a "status" field that is either "OK" or "ERROR"
    bool is_error = false;
    if (as_val_type(result) == AS_MAP) {
        as_map* result_map = as_map_fromval(result);
        if (result_map != NULL) {
            as_string status_key;
            as_string_init(&status_key, "status", false);
            as_val* status_val = as_map_get(result_map, (as_val*)&status_key);
            if (status_val != NULL && as_val_type(status_val) == AS_STRING) {
                const char* status_str = as_string_get(as_string_fromval(status_val));
                if (status_str != NULL && strcmp(status_str, "ERROR") == 0) {
                    is_error = true;
                }
            }
        }
    }

    // Commit record changes - equivalent to Lua's aerospike:update(rec)
    // This is required to persist any bin modifications made by the function
    // Only call update if:
    // 1. The function did not return an error - don't persist changes on error
    // 2. The record exists (has bins) - otherwise there's nothing to update
    // 3. There are dirty (modified) cached values to write
    if (!is_error && ctx != NULL && ctx->as != NULL && rec != NULL && as_rec_numbins(rec) > 0) {
        // Check if any cached values are dirty (modified)
        udf_record* urecord = (udf_record*)as_rec_source(rec);
        bool has_dirty = false;
        if (urecord != NULL) {
            for (uint32_t i = 0; i < urecord->n_updates; i++) {
                if (urecord->updates[i].dirty) {
                    has_dirty = true;
                    break;
                }
            }
        }

        // Only call update if there are dirty changes to persist
        if (has_dirty) {
            int update_rc = as_aerospike_rec_update(ctx->as, rec);
            if (update_rc != 0) {
                LOG_ERROR("mod-teranode: as_aerospike_rec_update failed with rc=%d", update_rc);
                // Don't fail the whole operation - the function result may still be valid
                // (e.g., returning an error response about the record state)
            }
        }
    }

    // Set success result
    as_result_setsuccess(res, result);
    return 0;
}
