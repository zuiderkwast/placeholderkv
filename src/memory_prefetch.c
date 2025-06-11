/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */
/*
 * This file utilizes prefetching keys and data for multiple commands in a batch,
 * to improve performance by amortizing memory access costs across multiple operations.
 */

#include "memory_prefetch.h"
#include "server.h"

typedef enum {
    PREFETCH_ENTRY, /* Initial state, prefetch entries associated with the given key's hash */
    PREFETCH_VALUE, /* prefetch the value object of the entry found in the previous step */
    PREFETCH_DONE   /* Indicates that prefetching for this key is complete */
} PrefetchState;

typedef struct KeyPrefetchInfo {
    PrefetchState state; /* Current state of the prefetch operation */
    hashtableIncrementalFindState hashtab_state;
} KeyPrefetchInfo;

/* PrefetchCommandsBatch structure holds the state of the current batch of client commands being processed. */
typedef struct PrefetchCommandsBatch {
    size_t cur_idx;                 /* Index of the current key being processed */
    size_t keys_done;               /* Number of keys that have been prefetched */
    size_t key_count;               /* Number of keys in the current batch */
    size_t client_count;            /* Number of clients in the current batch */
    size_t command_count;           /* Number of commands in the current batch */
    size_t max_prefetch_size;       /* Maximum number of keys to prefetch in a batch */
    void **keys;                    /* Array of keys to prefetch in the current batch */
    client **clients;               /* Array of clients in the current batch */
    hashtable **keys_tables;        /* Main table for each key */
    KeyPrefetchInfo *prefetch_info; /* Prefetch info for each key */
} PrefetchCommandsBatch;

static PrefetchCommandsBatch *batch = NULL;

static void freePrefetchCommandsBatch(void) {
    if (batch == NULL) {
        return;
    }

    zfree(batch->clients);
    zfree(batch->keys);
    zfree(batch->keys_tables);
    zfree(batch->prefetch_info);
    zfree(batch);
    batch = NULL;
}

void prefetchCommandsBatchInit(void) {
    if (batch) return;
    size_t max_prefetch_size = server.prefetch_batch_max_size;

    if (max_prefetch_size == 0) {
        return;
    }

    batch = zcalloc(sizeof(PrefetchCommandsBatch));
    batch->max_prefetch_size = max_prefetch_size;
    batch->clients = zcalloc(max_prefetch_size * sizeof(client *));
    batch->keys = zcalloc(max_prefetch_size * sizeof(void *));
    batch->keys_tables = zcalloc(max_prefetch_size * sizeof(hashtable *));
    batch->prefetch_info = zcalloc(max_prefetch_size * sizeof(KeyPrefetchInfo));
}

static void onMaxBatchSizeChange(void) {
    serverAssert(!batch || batch->client_count == 0);
    freePrefetchCommandsBatch();
    prefetchCommandsBatchInit();
}

/* Move to the next key in the batch. */
static void moveToNextKey(void) {
    batch->cur_idx = (batch->cur_idx + 1) % batch->key_count;
}

static void markKeyAsdone(KeyPrefetchInfo *info) {
    info->state = PREFETCH_DONE;
    server.stat_total_prefetch_entries++;
    batch->keys_done++;
}

/* Returns the next KeyPrefetchInfo structure that needs to be processed. */
static KeyPrefetchInfo *getNextPrefetchInfo(void) {
    size_t start_idx = batch->cur_idx;
    do {
        KeyPrefetchInfo *info = &batch->prefetch_info[batch->cur_idx];
        if (info->state != PREFETCH_DONE) return info;
        batch->cur_idx = (batch->cur_idx + 1) % batch->key_count;
    } while (batch->cur_idx != start_idx);
    return NULL;
}

static void initBatchInfo(hashtable **tables) {
    /* Initialize the prefetch info */
    for (size_t i = 0; i < batch->key_count; i++) {
        KeyPrefetchInfo *info = &batch->prefetch_info[i];
        if (!tables[i] || hashtableSize(tables[i]) == 0) {
            info->state = PREFETCH_DONE;
            batch->keys_done++;
            continue;
        }
        info->state = PREFETCH_ENTRY;
        hashtableIncrementalFindInit(&info->hashtab_state, tables[i], batch->keys[i]);
    }
}

static void prefetchEntry(KeyPrefetchInfo *info) {
    if (hashtableIncrementalFindStep(&info->hashtab_state) == 1) {
        /* Not done yet */
        moveToNextKey();
    } else {
        info->state = PREFETCH_VALUE;
    }
}

/* Prefetch the entry's value. If the value is found.*/
static void prefetchValue(KeyPrefetchInfo *info) {
    void *entry;
    if (hashtableIncrementalFindGetResult(&info->hashtab_state, &entry)) {
        robj *val = entry;
        if (val->encoding == OBJ_ENCODING_RAW && val->type == OBJ_STRING) {
            valkey_prefetch(val->ptr);
        }
    }

    markKeyAsdone(info);
}

/* Prefetch hashtable data for an array of keys.
 *
 * This function takes an array of tables and keys, attempting to bring
 * data closer to the L1 cache that might be needed for hashtable operations
 * on those keys.
 *
 * tables - An array of hashtables to prefetch data from.
 * prefetch_value - If true, we prefetch the value data for each key.
 * to bring the key's value data closer to the L1 cache as well.
 */
static void hashtablePrefetch(hashtable **tables) {
    initBatchInfo(tables);
    KeyPrefetchInfo *info;
    while ((info = getNextPrefetchInfo())) {
        switch (info->state) {
        case PREFETCH_ENTRY: prefetchEntry(info); break;
        case PREFETCH_VALUE: prefetchValue(info); break;
        default: serverPanic("Unknown prefetch state %d", info->state);
        }
    }
}

static void resetCommandsBatch(void) {
    batch->cur_idx = 0;
    batch->keys_done = 0;
    batch->key_count = 0;
    batch->client_count = 0;
    batch->command_count = 0;
}

/* Prefetch command-related data:
 * 1. Prefetch the command arguments allocated by the I/O thread to bring them closer to the L1 cache.
 * 2. Prefetch the keys and values for all commands in the current batch from the main hashtable. */
static void prefetchCommands(void) {
    /* Prefetch argv[j] in step 0 and argv[j]->ptr in step 1 for all commands.*/
    for (int step = 0; step <= 1; step++) {
        for (size_t i = 0; i < batch->client_count; i++) {
            client *c = batch->clients[i];
            if (c->argc > 1) {
                /* Skip prefetching first argv (cmd name) it was already looked up by the I/O thread. */
                for (int j = 1; j < c->argc; j++) {
                    if (step == 0) {
                        valkey_prefetch(c->argv[j]);
                    } else if (c->argv[j]->encoding == OBJ_ENCODING_RAW) {
                        valkey_prefetch(c->argv[j]->ptr);
                    }
                }
            }
            cmdQueue *queue = &c->cmd_queue;
            for (int k = queue->off; k < queue->len; k++) {
                commandParserState *st = &queue->cmds[k];
                if (!(st->read_flags & READ_FLAGS_PREFETCHED)) {
                    break; /* Command not included in this batch. */
                }
                if (!st->cmd || st->read_flags & (READ_FLAGS_BAD_ARITY | READ_FLAGS_NO_KEYS | READ_FLAGS_CROSSSLOT)) {
                    continue; /* Error or incomplete command. */
                }
                for (int j = 1; j < st->argc; j++) {
                    if (step == 0) {
                        valkey_prefetch(st->argv[j]);
                    } else if (st->argv[j]->encoding == OBJ_ENCODING_RAW) {
                        valkey_prefetch(st->argv[j]->ptr);
                    }
                }
            }
        }
    }

    /* Get the keys ptrs - we do it here after the key obj was prefetched. */
    for (size_t i = 0; i < batch->key_count; i++) {
        batch->keys[i] = ((robj *)batch->keys[i])->ptr;
    }

    /* Prefetch hashtable keys for all commands. Prefetching is beneficial only if there are more than one key. */
    if (batch->key_count > 1) {
        server.stat_total_prefetch_batches++;
        /* Prefetch keys from the main hashtable */
        hashtablePrefetch(batch->keys_tables);
    }
}

/* Processes all the prefetched commands in the current batch. */
static void processClientsCommandsBatch(void) {
    if (batch->client_count == 0) return;

    prefetchCommands();
    resetCommandsBatch();

    /* Handle the case where the max prefetch size has been changed. */
    if (batch->max_prefetch_size != (size_t)server.prefetch_batch_max_size) {
        onMaxBatchSizeChange();
    }
}

/* Get a command's keys to the current prefetching batch. Returns true if added
 * and false if the batch is full. */
static int addCommandToBatch(struct serverCommand *cmd, robj **argv, int argc,
                             int read_flags, serverDb *db, int slot) {
    if (!cmd || read_flags & (READ_FLAGS_BAD_ARITY | READ_FLAGS_NO_KEYS | READ_FLAGS_CROSSSLOT)) {
        /* Nothing to prefetch. Trivially added. */
        return C_OK;
    }
    if (batch->key_count >= batch->max_prefetch_size || batch->command_count >= batch->max_prefetch_size) {
        /* Batch is full. We also limit the command count to handle cases where
         * the commands have no keys. */
        return C_ERR;
    }
    getKeysResult result;
    initGetKeysResult(&result);
    int num_keys = getKeysFromCommand(cmd, argv, argc, &result);
    if (batch->key_count > 0 && num_keys + batch->key_count > batch->max_prefetch_size) {
        /* The keys of this command don't fit in the current non-empty batch.
         * The caller needs to process the current batch first. */
        getKeysFreeResult(&result);
        return C_ERR;
    }
    for (int i = 0; i < num_keys && batch->key_count < batch->max_prefetch_size; i++) {
        batch->keys[batch->key_count] = argv[result.keys[i].pos];
        int kv_idx = slot >= 0 ? slot : 0;
        batch->keys_tables[batch->key_count] = kvstoreGetHashtable(db->keys, kv_idx);
        batch->key_count++;
    }
    getKeysFreeResult(&result);
    batch->command_count++;
    return C_OK;
}

/* Adds the as many of the client's commands to the current batch that can fit
 * before the batch is full.
 *
 * Returns C_OK if any commands were added successfully, C_ERR otherwise. */
static int addClientQueuedCommandsToBatch(client *c) {
    if (!batch) return C_ERR;

    batch->clients[batch->client_count++] = c;

    /* Client's current command. */
    if (addCommandToBatch(c->parsed_cmd, c->argv, c->argc, c->read_flags, c->db, c->slot) == C_OK) {
        c->read_flags |= READ_FLAGS_PREFETCHED;
    } else {
        /* Batch is full. Undo adding the client to the batch. */
        batch->clients[--batch->client_count] = NULL;
        return C_ERR;
    }

    /* Commands in the queue. */
    for (int j = c->cmd_queue.off; j < c->cmd_queue.len; j++) {
        commandParserState *st = &c->cmd_queue.cmds[j];
        if (addCommandToBatch(st->cmd, st->argv, st->argc, st->read_flags, c->db, st->slot) == C_OK) {
            st->read_flags |= READ_FLAGS_PREFETCHED;
        } else {
            /* Batch is full. */
            break;
        }
    }

    return C_OK;
}

/* Prefetches a batch of commands for the next clients in the list. If the first
 * client's first command is already prefetched, do nothing. Let the caller
 * executed all prefetched commands before we prefetch another batch. */
void prefetchSomeCommandsForSomeClients(list *clients) {
    if (!batch) return;
    serverAssert(batch->client_count == 0);

    for (listNode *ln = listFirst(clients); ln != NULL; ln = listNextNode(ln)) {
        client *c = listNodeValue(ln);
        valkey_prefetch(listNextNode(ln)); /* Speed up list iteration */

        /* memory barrier acquire to get the updated client state */ //DEBUG
        atomic_thread_fence(memory_order_acquire); //DEBUG

        /* Skip client if it's not ready to execute commands. */
        if (c->io_write_state == CLIENT_PENDING_IO || c->io_read_state == CLIENT_PENDING_IO) continue;
        if (c->flag.close_asap) continue;

        /* Abort if the first command is already prefetched. The caller needs to
         * execute all prefetched commands before we prefetch another batch.*/
        if (c->read_flags & READ_FLAGS_PREFETCHED) {
            serverAssert(batch->client_count == 0);
            return;
        }

        if (addClientQueuedCommandsToBatch(c) != C_OK) break;
    }
    processClientsCommandsBatch();
}

/* Prefetches the next commands for one client. Aborts if the client's first
 * command is already prefetched. */
void prefetchSomeCommandsForOneClient(client *c) {
    if (!batch) return;
    serverAssert(batch->client_count == 0);

    /* Abort if the first command is already prefetched. */
    if (c->read_flags & READ_FLAGS_PREFETCHED) return;
    if (addClientQueuedCommandsToBatch(c) != C_OK) return;
    processClientsCommandsBatch();
}
