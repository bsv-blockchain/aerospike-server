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

#include "internal.h"

//==========================================================
// Typedefs.
//

// Function pointer type for UTXO functions
typedef as_val* (*teranode_fn)(as_rec*, as_list*, as_aerospike*);

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

    int rc = -1;

    if (function == NULL) {
        LOG_ERROR("mod-teranode: function name is NULL");
        as_result_setfailure(res, (as_val*)as_string_new("function name required", false));
        goto done;
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
        char* msg_copy = strdup(errmsg);
        if (msg_copy != NULL) {
            as_string* s = as_string_new(msg_copy, true);
            if (s != NULL) {
                as_result_setfailure(res, (as_val*)s);
            } else {
                free(msg_copy);
                as_result_setfailure(res, (as_val*)as_string_new("unknown function", false));
            }
        } else {
            as_result_setfailure(res, (as_val*)as_string_new("unknown function", false));
        }
        goto done;
    }

    // Execute the function - pass ctx->as so functions can call aerospike:update(rec)
    // like the Lua script does, committing changes inside each function on success
    as_aerospike* as_ctx = (ctx != NULL) ? ctx->as : NULL;
    as_val* result = fn(rec, args, as_ctx);

    if (result == NULL) {
        LOG_ERROR("mod-teranode: function '%s' returned NULL", function);
        as_result_setfailure(res, (as_val*)as_string_new("function returned NULL", false));
        goto done;
    }

    // Set success result
    // Note: Record updates are now handled inside each function (like Lua script)
    as_result_setsuccess(res, result);
    rc = 0;

done:
    // Compensate for the unconditional as_val_reserve(urec) in udf.c
    // (transaction/udf.c:900). That reserve exists for Lua's garbage collector
    // which calls as_val_destroy via __gc when it collects the record userdata.
    // Native modules have no GC, so without this the as_rec leaks on every call.
    //
    // The proper fix is to make the reserve in udf.c conditional on the module
    // type. This workaround is safe: it decrements the refcount from 2 to 1 here,
    // then udf_master_done/failed decrements from 1 to 0 and frees.
    as_val_destroy((as_val*)rec);

    return rc;
}
