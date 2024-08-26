/* Copyright (c) 2024-present, Valkey contributors
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    * Redistributions of source code must retain the above copyright notice,
 *      this list of conditions and the following disclaimer.
 *    * Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *    * Neither the name of the copyright holder nor the names of its
 *      contributors may be used to endorse or promote products derived from
 *      this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/* Hashtab
 * =======
 *
 * This is an implementation of an open addressing hash table with cache-line
 * sized buckets. It's designed for speed and low memory overhead. It provides
 * lookups using a single memory access in most cases. It provides the following
 * features:
 *
 * - Incremental rehashing using two tables.
 *
 * - Stateless interation using 'scan'.
 *
 * - A hash table contains pointer-sized elements rather than key-value entries.
 *   Using it as a set is strait-forward. Using it as a key-value store requires
 *   combining key and value in an object and inserting this object into the
 *   hash table. A callback for fetching the key from within the element is
 *   provided by the caller when creating the hash table.
 *
 * - The element type, key type, hash function and other properties are
 *   configurable as callbacks in a 'type' structure provided when creating a
 *   hash table.
 *
 * Conventions
 * -----------
 *
 * Functions and types are prefixed by "hashtab", macros by "HASHTAB". Internal
 * names don't use the prefix. Internal functions are 'static'.
 *
 * Credits
 * -------
 *
 * - The design of the cache-line aware open addressing scheme is inspired by
 *   tricks used in 'Swiss tables' (Sam Benzaquen, Alkis Evlogimenos, Matt
 *   Kulukundis, and Roman Perepelitsa et. al.).
 *
 * - The incremental rehashing using two tables, though for a chaining hash
 *   table, was designed by Salvatore Sanfilippo.
 *
 * - The original scan algorithm (for a chained hash table) was designed by
 *   Pieter Noordhuis.
 *
 * - The incremental rehashing and the scan algorithm were adapted for the open
 *   addressing scheme by Viktor Söderqvist.
 */

#include "hashtab.h"
#include "serverassert.h"
#include "zmalloc.h"

#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* The default hashing function uses the SipHash implementation in siphash.c. */

uint64_t siphash(const uint8_t *in, const size_t inlen, const uint8_t *k);
uint64_t siphash_nocase(const uint8_t *in, const size_t inlen, const uint8_t *k);

/* --- Global variables --- */

static uint8_t hash_function_seed[16];
static hashtabResizePolicy resize_policy = HASHTAB_RESIZE_ALLOW;

/* --- Fill factor --- */

/* We use a soft and a hard limit for the minumum and maximum fill factor. The
 * hard limits are used when resizing should be avoided, according to the resize
 * policy. Resizing is typically to be avoided when we have forked child process
 * running. Then, we don't want to move too much memory around, since the fork
 * is using copy-on-write.
 *
 * With open addressing, the physical fill factor limit is 100% (probes the
 * whole table) so we may need to expand even if when it's preferred to avoid
 * it. Even if we resize and start inserting new elements in the new table, we
 * can avoid actively moving elements from the old table to the new table. When
 * the resize policy is AVOID, we perform a step of incremental rehashing only
 * on insertions and not on lookups. */

#define MAX_FILL_PERCENT_SOFT 77
#define MAX_FILL_PERCENT_HARD 90

#define MIN_FILL_PERCENT_SOFT 13
#define MIN_FILL_PERCENT_HARD 3

/* --- Hash function API --- */

/* The seed needs to be 16 bytes. */
void hashtabSetHashFunctionSeed(uint8_t *seed) {
    memcpy(hash_function_seed, seed, sizeof(hash_function_seed));
}

uint8_t *hashtabGetHashFunctionSeed(void) {
    return hash_function_seed;
}

uint64_t hashtabGenHashFunction(const char *buf, size_t len) {
    return siphash((const uint8_t *)buf, len, hash_function_seed);
}

uint64_t hashtabGenCaseHashFunction(const char *buf, size_t len) {
    return siphash_nocase((const uint8_t *)buf, len, hash_function_seed);
}

/* --- Global resize policy API --- */

/* The global resize policy is one of
 *
 *   - HASHTAB_RESIZE_ALLOW: Rehash as required for optimal performance.
 *
 *   - HASHTAB_RESIZE_AVOID: Don't rehash and move memory if it can be avoided;
 *     used when there is a fork running and we want to avoid affecting
 *     copy-on-write memory.
 *
 *   - HASHTAB_RESIZE_FORBID: Don't rehash at all. Used in a child process which
 *     doesn't add any keys. */
void hashtabSetResizePolicy(hashtabResizePolicy policy) {
    resize_policy = policy;
}

/* --- Hash table layout --- */

#if SIZE_MAX == UINT64_MAX /* 64-bit version */

#define ELEMENTS_PER_BUCKET 7

/* Selecting the number of buckets.
 *
 * When resizing the table, we want to select an appropriate number of buckets
 * without an expensive division. Division by a power of two is cheap, but any
 * other division is expensive. We pick a fill factor to make division cheap for
 * our choice of ELEMENTS_PER_BUCKET.
 *
 * The number of buckets we want is NUM_ELEMENTS / (ELEMENTS_PER_BUCKET * FILL_FACTOR),
 * rounded up. The fill is the number of elements we have, or want to put, in
 * the table.
 *
 * Instead of the above fraction, we multiply by an integer BUCKET_FACTOR and
 * divide by a power-of-two BUCKET_DIVISOR. This gives us a fill factor of at
 * most MAX_FILL_PERCENT_SOFT, the soft limit for expanding.
 *
 *     NUM_BUCKETS = ceil(NUM_ELEMENTS * BUCKET_FACTOR / BUCKET_DIVISOR)
 *
 * This gives us
 *
 *     FILL_FACTOR = NUM_ELEMENTS / (NUM_BUCKETS * ELEMENTS_PER_BUCKET)
 *                 = 1 / (BUCKET_FACTOR / BUCKET_DIVISOR) / ELEMENTS_PER_BUCKET
 *                 = BUCKET_DIVISOR / BUCKET_FACTOR / ELEMENTS_PER_BUCKET
 */

#define BUCKET_FACTOR 3
#define BUCKET_DIVISOR 16
/* When resizing, we get a fill of at most 76.19% (16 / 3 / 7). */

#elif SIZE_MAX == UINT32_MAX /* 32-bit version */

#define ELEMENTS_PER_BUCKET 12
#define BUCKET_FACTOR 7
#define BUCKET_DIVISOR 64
/* When resizing, we get a fill of at most 76.19% (64 / 7 / 12). */

#else
#error "Only 64-bit or 32-bit architectures are supported"
#endif /* 64-bit vs 32-bit version */

#ifndef static_assert
#define static_assert _Static_assert
#endif

static_assert(100 * BUCKET_DIVISOR / BUCKET_FACTOR / ELEMENTS_PER_BUCKET <= MAX_FILL_PERCENT_SOFT,
              "Expand must result in a fill below the soft max fill factor");
static_assert(MAX_FILL_PERCENT_SOFT <= MAX_FILL_PERCENT_HARD, "Soft vs hard fill factor");
static_assert(MAX_FILL_PERCENT_HARD < 100, "Hard fill factor must be below 100%");

/* Incremental rehashing
 * ---------------------
 *
 * When rehashing, we allocate a new table and incrementally move elements from
 * the old to the new table.
 *
 * To avoid affecting CoW when there is a fork, the dict avoids resizing in this
 * case. With an open addressing scheme, it is impossible to add more elements
 * than the number of slots, so we need to allow resizing even in this case. To
 * avoid affecting CoW, we resize with incremental rehashing paused, so only new
 * elements are added to the new table until the fork is done.
 *
 * This also means that we need to allow resizing even if rehashing is already
 * in progress. In the worst case, we need to resizing multiple times while a
 * fork is running. We can to fast-forward the rehashing in this case.
 */

/* --- Types --- */

/* Open addressing scheme
 * ----------------------
 *
 * It uses an open addressing scheme, with buckets of 64 bytes (one cache line).
 * Each bucket contains metadata and element slots for a fixed number of
 * elements. In a 64-bit system, there are up to 7 elements per bucket. These
 * are unordered slots and an element can be inserted in any of the free slots.
 * Additionally, the bucket contains metadata for the elements. This includes a
 * few bits of the hash of the key of each element, which are used to rule out
 * false negatives when looking up elements.
 *
 * The bucket metadata contains a bit that is set if the bucket has ever been
 * full. This bit acts as a tombstone for the bucket and it's what we need to
 * know if probing the next bucket is necessary.
 *
 * Bucket layout, 64-bit version, 7 elements per bucket:
 *
 *     1 bit     7 bits    [1 byte] x 7  [8 bytes] x 7 = 64 bytes
 *     everfull  presence  hashes        elements
 *
 *     everfull: a shared tombstone; set if the bucket has ever been full
 *     presence: an bit per element slot indicating if an element present or not
 *     hashes: some bits of hash of each element to rule out false positives
 *     elements: the actual elements, typically pointers (pointer-sized)
 *
 * The 32-bit version has 12 elements and 19 unused bits per bucket:
 *
 *     1 bit     12 bits   3 bits  [1 byte] x 12  2 bytes  [4 bytes] x 12
 *     everfull  presence  unused  hashes         unused   elements
 */

typedef struct {
    uint8_t everfull : 1;
    uint8_t presence : ELEMENTS_PER_BUCKET;
    uint8_t hashes[ELEMENTS_PER_BUCKET];
    void *elements[ELEMENTS_PER_BUCKET];
} hashtabBucket;

struct hashtab {
    hashtabType *type;
    ssize_t rehashIdx;        /* -1 = rehashing not in progress. */
    hashtabBucket *tables[2]; /* 0 = main table, 1 = rehashing target.  */
    size_t used[2];           /* Number of elements in each table. */
    int8_t bucketExp[2];      /* Exponent for num buckets (num = 1 << exp). */
    int16_t pauseRehash;      /* Non-zero = rehashing is paused */
    int16_t pauseAutoShrink;  /* Non-zero = automatic resizing disallowed. */
    void *metadata[];
};

/* --- Internal functions --- */

static hashtabBucket *hashtabFindBucketForInsert(hashtab *s, uint64_t hash, int *pos_in_bucket);

static inline void freeElement(hashtab *s, void *elem) {
    if (s->type->elementDestructor) s->type->elementDestructor(s, elem);
}

static inline int compareKeys(hashtab *s, const void *key1, const void *key2) {
    return s->type->keyCompare ? s->type->keyCompare(s, key1, key2)
                               : key1 != key2;
}

static inline const void *elementGetKey(hashtab *s, const void *elem) {
    return s->type->elementGetKey ? s->type->elementGetKey(elem) : elem;
}

static inline uint64_t hashKey(hashtab *s, const void *key) {
    return s->type->hashFunction ? s->type->hashFunction(key)
        : hashtabGenHashFunction((const char *)&key, sizeof(key));
}

static inline uint64_t hashElement(hashtab *s, const void *elem) {
    return hashKey(s, elementGetKey(s, elem));
}

/* For the hash bits stored in the bucket, we use the highest bits of the hash
 * value, since these are not used for selecting the bucket. */
static inline uint8_t highBits(uint64_t hash) {
    return hash >> (CHAR_BIT * 7);
}

static inline int bucketIsFull(hashtabBucket *b) {
    return b->presence == (1 << ELEMENTS_PER_BUCKET) - 1;
}

static void resetTable(hashtab *s, int table_idx) {
    s->tables[table_idx] = NULL;
    s->used[table_idx] = 0;
    s->bucketExp[table_idx] = -1;
}

static inline size_t numBuckets(int exp) {
    return exp == -1 ? 0 : (size_t)1 << exp;
}

/* Bitmask for masking the hash value to get bucket index. */
static inline size_t expToMask(int exp) {
    return exp == -1 ? 0 : numBuckets(exp) - 1;
}

/* Returns the 'exp', where num_buckets = 1 << exp. The number of
 * buckets is a power of two. */
static signed char nextBucketExp(size_t min_capacity) {
    if (min_capacity == 0) return -1;
    /* ceil(x / y) = floor((x - 1) / y) + 1 */
    size_t min_buckets = (min_capacity * BUCKET_FACTOR - 1) / BUCKET_DIVISOR + 1;
    if (min_buckets >= SIZE_MAX / 2) return CHAR_BIT * sizeof(size_t) - 1;
    return CHAR_BIT * sizeof(size_t) - __builtin_clzl(min_buckets - 1);
}

/* Swaps the tables and frees the old table. */
static void rehashingCompleted(hashtab *s) {
    if (s->type->rehashingCompleted) s->type->rehashingCompleted(s);
    if (s->tables[0]) zfree(s->tables[0]);
    s->bucketExp[0] = s->bucketExp[1];
    s->tables[0] = s->tables[1];
    s->used[0] = s->used[1];
    resetTable(s, 1);
    s->rehashIdx = -1;
}

/* Reverse bits, adapted to use bswap, from
 * https://graphics.stanford.edu/~seander/bithacks.html#ReverseParallel */
static size_t rev(size_t v) {
#if SIZE_MAX == UINT64_MAX
    /* Swap odd and even bits. */
    v = ((v >> 1) & 0x5555555555555555) | ((v & 0x5555555555555555) << 1);
    /* Swap consecutive pairs. */
    v = ((v >> 2) & 0x3333333333333333) | ((v & 0x3333333333333333) << 2);
    /* Swap nibbles. */
    v = ((v >> 4) & 0x0F0F0F0F0F0F0F0F) | ((v & 0x0F0F0F0F0F0F0F0F) << 4);
    /* Reverse bytes. */
    v = __builtin_bswap64(v);
#else
    /* 32-bit version. */
    v = ((v >> 1) & 0x55555555) | ((v & 0x55555555) << 1);
    v = ((v >> 2) & 0x33333333) | ((v & 0x33333333) << 2);
    v = ((v >> 4) & 0x0F0F0F0F) | ((v & 0x0F0F0F0F) << 4);
    v = __builtin_bswap32(v);
#endif
    return v;
}

/* Advances a scan cursor to the next value. It increments the reverse bit
 * representation of the masked bits of v. This algorithm was invented by Pieter
 * Noordhuis. */
size_t nextCursor(size_t v, size_t mask) {
    v |= ~mask; /* Set the unmasked (high) bits. */
    v = rev(v); /* Reverse. The unmasked bits are now the low bits. */
    v++;        /* Increment the reversed cursor, flipping the unmasked bits to
                 * 0 and increments the masked bits. */
    v = rev(v); /* Reverse the bits back to normal. */
    return v;
}

/* The reverse of nextCursor. */
static size_t prevCursor(size_t v, size_t mask) {
    v = rev(v);
    v--;
    v = rev(v);
    v = v & mask;
    return v;
}

/* Rehashes one bucket. */
static void rehashStep(hashtab *s) {
    assert(hashtabIsRehashing(s));
    size_t idx = s->rehashIdx;
    hashtabBucket *b = &s->tables[0][idx];
    int pos;
    for (pos = 0; pos < ELEMENTS_PER_BUCKET; pos++) {
        if (!(b->presence & (1 << pos))) continue; /* empty */
        void *elem = b->elements[pos];
        uint8_t h2 = b->hashes[pos];
        /* Insert into table 1. */
        uint64_t hash;
        /* When shrinking, it's possible to avoid computing the hash. We can
         * just use idx has the hash, but only if we know that probing didn't
         * push this element away from its primary bucket, so only if the
         * bucket before the current one hasn't ever been full. */
        if (s->bucketExp[1] < s->bucketExp[0] &&
            !s->tables[0][prevCursor(idx, expToMask(s->bucketExp[0]))].everfull) {
            hash = idx;
        } else {
            hash = hashElement(s, elem);
        }
        int pos_in_dst_bucket;
        hashtabBucket *dst = hashtabFindBucketForInsert(s, hash, &pos_in_dst_bucket);
        dst->elements[pos_in_dst_bucket] = elem;
        dst->hashes[pos_in_dst_bucket] = h2;
        dst->presence |= (1 << pos_in_dst_bucket);
        dst->everfull |= bucketIsFull(dst);
        s->used[0]--;
        s->used[1]++;
    }
    /* Mark the source bucket as empty. */
    b->presence = 0;
    /* Done. */
    s->rehashIdx = nextCursor(s->rehashIdx, expToMask(s->bucketExp[0]));
    //s->rehashIdx++;
    //if ((size_t)s->rehashIdx == numBuckets(s->bucketExp[0])) {
    if (s->rehashIdx == 0) {
        rehashingCompleted(s);
    }
}

/* Allocates a new table and initiates incremental rehashing if necessary.
 * Returns 1 on resize (success), 0 on no resize (failure). If 0 is returned and
 * 'malloc_failed' is provided, it is set to 1 if allocation failed. If
 * 'malloc_failed' is not provided, an allocation failure triggers a panic. */
static int resize(hashtab *s, size_t min_capacity, int *malloc_failed) {
    if (malloc_failed) *malloc_failed = 0;

    /* Size of new table. */
    signed char exp = nextBucketExp(min_capacity);
    size_t num_buckets = numBuckets(exp);
    size_t new_capacity = num_buckets * ELEMENTS_PER_BUCKET;
    if (new_capacity < min_capacity || num_buckets * sizeof(hashtabBucket) < num_buckets) {
        /* Overflow */
        return 0;
    }
    signed char old_exp = s->bucketExp[hashtabIsRehashing(s) ? 1 : 0];
    if (exp == old_exp) {
        /* Can't resize to the same size. */
        return 0;
    }

    /* We can't resize if rehashing is already ongoing. Fast-forward ongoing
     * rehashing before we continue. */
    while (hashtabIsRehashing(s)) {
        rehashStep(s);
    }

    /* Allocate the new hash table. */
    hashtabBucket *new_table;
    if (malloc_failed) {
        new_table = ztrycalloc(num_buckets * sizeof(hashtabBucket));
        if (new_table == NULL) {
            *malloc_failed = 1;
            return 0;
        }
    } else {
        new_table = zcalloc(num_buckets * sizeof(hashtabBucket));
    }
    s->bucketExp[1] = exp;
    s->tables[1] = new_table;
    s->used[1] = 0;
    s->rehashIdx = 0;
    if (s->type->rehashingStarted) s->type->rehashingStarted(s);

    /* If the old table was empty, the rehashing is completed immediately. */
    if (s->tables[0] == NULL || s->used[0] == 0) {
        rehashingCompleted(s);
    }
    return 1;
}

/* Returns 1 if the table is expanded, 0 if not expanded. If 0 is returned and
 * 'malloc_failed' is proveded, it is set to 1 if malloc failed and 0
 * otherwise. */
static int expand(hashtab *t, size_t size, int *malloc_failed) {
    if (size < hashtabSize(t)) {
        return 0;
    }
    return resize(t, size, malloc_failed);
}

/* Return 1 if expand was performed; 0 otherwise. */
int hashtabExpand(hashtab *t, size_t size) {
    return expand(t, size, NULL);
}

/* Returns 1 if expand was performed or if expand is not needed. Returns 0 if
 * expand failed due to memory allocation failure. */
int hashtabTryExpand(hashtab *s, size_t size) {
    int malloc_failed = 0;
    return expand(s, size, &malloc_failed) || !malloc_failed;
}

/* Expanding is done automatically on insertion, but less eagerly if resize
 * policy is set to AVOID or FORBID. After restoring resize policy to ALLOW, you
 * may want to call hashtabExpandIfNeeded. Returns 1 if expanding, 0 if not
 * expanding. */
int hashtabExpandIfNeeded(hashtab *s) {
    size_t min_capacity = s->used[0] + s->used[1] + 1;
    size_t num_buckets = numBuckets(s->bucketExp[hashtabIsRehashing(s) ? 1 : 0]);
    size_t current_capacity = num_buckets * ELEMENTS_PER_BUCKET;
    unsigned max_fill_percent = resize_policy == HASHTAB_RESIZE_AVOID ? MAX_FILL_PERCENT_HARD : MAX_FILL_PERCENT_SOFT;
    if (min_capacity * 100 <= current_capacity * max_fill_percent) {
        return 0;
    }
    return resize(s, min_capacity, NULL);
}

/* Shrinking is done automatically on deletion, but less eagerly if resize
 * policy is set to AVOID and not at all if set to FORBID. After restoring
 * resize policy to ALLOW, you may want to call hashtabShrinkIfNeeded. */
int hashtabShrinkIfNeeded(hashtab *s) {
    /* Don't shrink if rehashing is already in progress. */
    if (hashtabIsRehashing(s) || resize_policy == HASHTAB_RESIZE_FORBID) {
        return 0;
    }
    size_t current_capacity = numBuckets(s->bucketExp[0]) * ELEMENTS_PER_BUCKET;
    unsigned min_fill_percent = resize_policy == HASHTAB_RESIZE_AVOID ? MIN_FILL_PERCENT_HARD : MIN_FILL_PERCENT_SOFT;
    if (s->used[0] * 100 > current_capacity * min_fill_percent) {
        return 0;
    }
    return resize(s, s->used[0], NULL);
}

/* Finds an element matching the key. If a match is found, returns a pointer to
 * the bucket containing the matching element and points 'pos_in_bucket' to the
 * index within the bucket. Returns NULL if no matching element was found. */
static hashtabBucket *hashtabFindBucket(hashtab *s, uint64_t hash, const void *key, int *pos_in_bucket) {
    if (hashtabSize(s) == 0) return 0;
    uint8_t h2 = highBits(hash);
    int table;

    /* Do some incremental rehashing. */
    if (hashtabIsRehashing(s) && resize_policy == HASHTAB_RESIZE_ALLOW) {
        rehashStep(s);
    }

    /* Check rehashing destination table first, since it is newer and typically
     * has less 'everfull' flagged buckets. Therefore it needs less probing for
     * lookup. */
    for (table = 1; table >= 0; table--) {
        if (s->used[table] == 0) continue;
        size_t mask = expToMask(s->bucketExp[table]);
        size_t bucket_idx = hash & mask;
        while (1) {
            hashtabBucket *b = &s->tables[table][bucket_idx];
            /* Find candidate elements with presence flag set and matching h2 hash. */
            for (int pos = 0; pos < ELEMENTS_PER_BUCKET; pos++) {
                if ((b->presence & (1 << pos)) && b->hashes[pos] == h2) {
                    /* It's a candidate. */
                    void *elem = b->elements[pos];
                    const void *elem_key = elementGetKey(s, elem);
                    if (compareKeys(s, key, elem_key) == 0) {
                        /* It's a match. */
                        if (pos_in_bucket) *pos_in_bucket = pos;
                        return b;
                    }
                }
            }

            /* Probe the next bucket? */
            if (!b->everfull) break;
            bucket_idx = nextCursor(bucket_idx, mask);
        }
    }
    return NULL;
}

/* Find an empty position in the table for inserting an element with the given hash. */
static hashtabBucket *hashtabFindBucketForInsert(hashtab *s, uint64_t hash, int *pos_in_bucket) {
    int table = hashtabIsRehashing(s) ? 1 : 0;
    assert(s->tables[table]);
    size_t mask = expToMask(s->bucketExp[table]);
    size_t bucket_idx = hash & mask;
    while (1) {
        hashtabBucket *b = &s->tables[table][bucket_idx];
        for (int pos = 0; pos < ELEMENTS_PER_BUCKET; pos++) {
            if (b->presence & (1 << pos)) continue; /* busy */
            if (pos_in_bucket) *pos_in_bucket = pos;
            return b;
        }
        bucket_idx = nextCursor(bucket_idx, mask);
    }
}

/* Helper to insert an element. Doesn't check if an element with a matching key
 * already exists. This must be ensured by the caller. */
static void hashtabInsert(hashtab *s, uint64_t hash, void *elem) {
    hashtabExpandIfNeeded(s);
    /* If resize policy is AVOID, do some incremental rehashing here, because in
     * this case we don't do it when looking up existing elements. The reason
     * for doing it on insert is to ensure that we finish rehashing before we
     * need to resize the table again. */
    if (hashtabIsRehashing(s) && resize_policy == HASHTAB_RESIZE_AVOID) {
        rehashStep(s);
    }
    int i;
    hashtabBucket *b = hashtabFindBucketForInsert(s, hash, &i);
    b->elements[i] = elem;
    b->presence |= (1 << i);
    b->hashes[i] = highBits(hash);;
    b->everfull |= bucketIsFull(b);
    s->used[hashtabIsRehashing(s) ? 1 : 0]++;
}

/* --- API functions --- */

/* Allocates and initializes a new hashtable specified by the given type. */
hashtab *hashtabCreate(hashtabType *type) {
    size_t metasize = type->getMetadataSize ? type->getMetadataSize() : 0;
    hashtab *s = zmalloc(sizeof(*s) + metasize);
    if (metasize > 0) {
        memset(&s->metadata, 0, metasize);
    }
    s->type = type;
    s->rehashIdx = -1;
    s->pauseRehash = 0;
    s->pauseAutoShrink = 0;
    resetTable(s, 0);
    resetTable(s, 1);
    return s;
}

/* Returns the type of the hashtable. */
hashtabType *hashtabGetType(hashtab *s) {
    return s->type;
}

/* Returns a pointer to the table's metadata (userdata) section. */
void *hashtabMetadata(hashtab *s) {
    return &s->metadata;
}

/* Returns the number of elements stored. */
size_t hashtabSize(hashtab *s) {
    return s->used[0] + s->used[1];
}

/* Pauses automatic shrinking. This can be called before deleting a lot of
 * elements, to prevent automatic shrinking from being triggered multiple times.
 * Call hashtableResumeAutoShrink afterwards to restore automatic shrinking. */
void hashtabPauseAutoShrink(hashtab *s) {
    s->pauseAutoShrink++;
}

/* Re-enables automatic shrinking, after it has been paused. If you have deleted
 * many elements while automatic shrinking was paused, you may want to call
 * hashtabShrinkIfNeeded. */
void hashtabResumeAutoShrink(hashtab *s) {
    s->pauseAutoShrink--;
    if (s->pauseAutoShrink == 0) {
        hashtabShrinkIfNeeded(s);
    }
}

/* Pauses incremental rehashing. */
void hashtabPauseRehashing(hashtab *s) {
    s->pauseRehash++;
}

/* Resumes incremental rehashing, after pausing it. */
void hashtabResumeRehashing(hashtab *s) {
    s->pauseRehash--;
}

/* Returns 1 if incremental rehashing is paused, 0 if it isn't. */
int hashtabIsRehashingPaused(hashtab *s) {
    return s->pauseRehash > 0;
}

/* Returns 1 if incremental rehashing is in progress, 0 otherwise. */
int hashtabIsRehashing(hashtab *s) {
    return s->rehashIdx != -1;
}

/* Returns 1 if an element was found matching the key. Also points *found to it,
 * if found is provided. Returns 0 if no matching element was found. */
int hashtabFind(hashtab *s, const void *key, void **found) {
    if (hashtabSize(s) == 0) return 0;
    uint64_t hash = hashKey(s, key);
    int pos_in_bucket = 0;
    hashtabBucket *b = hashtabFindBucket(s, hash, key, &pos_in_bucket);
    if (b) {
        if (found) *found = b->elements[pos_in_bucket];
        return 1;
    } else {
        return 0;
    }
}

/* Adds an element. Returns 1 on success. Returns 0 if there was already an element
 * with the same key. */
int hashtabAdd(hashtab *s, void *elem) {
    return hashtabAddRaw(s, elem, NULL);
}

/* Adds an element and returns 1 on success. Returns 0 if there was already an
 * element with the same key and, if an 'existing' pointer is provided, it is
 * pointed to the existing element. */
int hashtabAddRaw(hashtab *s, void *elem, void **existing) {
    const void *key = elementGetKey(s, elem);
    uint64_t hash = hashKey(s, key);
    int pos_in_bucket = 0;
    hashtabBucket *b = hashtabFindBucket(s, hash, key, &pos_in_bucket);
    if (b != NULL) {
        if (existing) *existing = b->elements[pos_in_bucket];
        return 0;
    } else {
        hashtabInsert(s, hash, elem);
        return 1;
    }
}

/* Add or overwrite. Returns 1 if an new element was inserted, 0 if an existing
 * element was overwritten. */
int hashtabReplace(hashtab *s, void *elem) {
    const void *key = elementGetKey(s, elem);
    int pos_in_bucket = 0;
    uint64_t hash = hashKey(s, key);
    hashtabBucket *b = hashtabFindBucket(s, hash, key, &pos_in_bucket);
    if (b != NULL) {
        if (s->type->elementDestructor) {
            s->type->elementDestructor(s, b->elements[pos_in_bucket]);
        }
        b->elements[pos_in_bucket] = elem;
        return 0;
    } else {
        hashtabInsert(s, hash, elem);
        return 1;
    }
}

/* --- Scan ---
 *
 * We need to use a scan-increment-probing variant of linear probing. When we
 * scan, we need to continue scanning as long a bucket in either of the tables
 * is tombstoned (has ever been full).
 *
 * A full scan is performed like this: Start with a cursor of 0. The scan
 * callback is invoked for each element scanned and a new cursor is returned.
 * Next time, call this function with the new cursor. Continue until the
 * function returns 0.
 *
 * If emit_ref is non-zero, a pointer to the element's location in the table is
 * passed to the scan function instead of the actual element.
 */
size_t hashtabScan(hashtab *t, size_t cursor, hashtabScanFunction fn, void *privdata, int emit_ref) {

    if (hashtabSize(t) == 0) return 0;

    /* Prevent elements from being moved around as a side-effect of the scan
     * callback. */
    hashtabPauseRehashing(t);

    /* If any element that hashes to the current bucket may have been inserted
     * in another bucket due to probing, we need to continue to cover the whole
     * probe sequence in the same scan cycle. Otherwise we may miss those
     * elements if they are rehashed before the next scan call. */
    int in_probe_sequence = 1;
    while (in_probe_sequence) {
        in_probe_sequence = 0; /* Set to 1 if an ever-full bucket is scanned. */
        if (!hashtabIsRehashing(t)) {
            size_t mask = expToMask(t->bucketExp[0]);
            hashtabBucket *b = &t->tables[0][cursor & mask];

            /* Emit entries at cursor */
            int pos;
            for (pos = 0; pos < ELEMENTS_PER_BUCKET; pos++) {
                if (b->presence & (1 << pos)) {
                    void *emit = emit_ref ? &b->elements[pos] : b->elements[pos];
                    fn(privdata, emit);
                }
            }

            in_probe_sequence |= b->everfull;

            /* Advance cursor */
            cursor = nextCursor(cursor, mask);
        } else {
            /* Let table0 be the the smaller table and table1 the bigger one. */
            int table0, table1;
            if (t->bucketExp[0] <= t->bucketExp[1]) {
                table0 = 0;
                table1 = 1;
            } else {
                table0 = 1;
                table1 = 0;
            }

            size_t mask0 = expToMask(t->bucketExp[table0]);
            size_t mask1 = expToMask(t->bucketExp[table1]);

            /* Emit elements in table0 at cursor. */
            hashtabBucket *b = &t->tables[0][cursor & mask0];
            for (int pos = 0; pos < ELEMENTS_PER_BUCKET; pos++) {
                if (b->presence & (1 << pos)) {
                    void *emit = emit_ref ? &b->elements[pos] : b->elements[pos];
                    fn(privdata, emit);
                }
            }
            in_probe_sequence |= b->everfull;

            /* Iterate over indices in larger table that are the expansion of
             * the index pointed to by the cursor in the smaller table. */
            do {
                /* Emit elements in table1 at cursor. */
                b = &t->tables[1][cursor & mask1];
                for (int pos = 0; pos < ELEMENTS_PER_BUCKET; pos++) {
                    if (b->presence & (1 << pos)) {
                        void *emit = emit_ref ? &b->elements[pos] : b->elements[pos];
                        fn(privdata, emit);
                    }
                }
                in_probe_sequence |= b->everfull;

                /* Increment the reverse cursor not covered by the smaller mask.*/
                cursor = nextCursor(cursor, mask1);

                /* Continue while bits covered by mask difference is non-zero */
            } while (cursor & (mask0 ^ mask1));
        }
    } while (in_probe_sequence);

    hashtabResumeRehashing(t);

    return cursor;
}


/* --- DEBUG --- */
void hashtabDump(hashtab *s) {
    for (int table = 0; table <= 1; table++) {
        printf("Table %d, used %lu, exp %d\n", table, s->used[table], s->bucketExp[table]);
        for (size_t idx = 0; idx < numBuckets(s->bucketExp[table]); idx++) {
            hashtabBucket *b = &s->tables[table][idx];
            printf("Bucket %d:%lu everfull:%d\n", table, idx, b->everfull);
            for (int pos = 0; pos < ELEMENTS_PER_BUCKET; pos++) {
                printf("  %d ", pos);
                if (b->presence & (1 << pos)) {
                    printf("h2 %02x, key \"%s\"\n", b->hashes[pos], (const char *)elementGetKey(s, b->elements[pos]));
                } else {
                    printf("(empty)\n");
                }
            }
        }
    }
}

void hashtabHistogram(hashtab *s) {
    //const char *symb = ".:-+x*=#";
    //const char *symb = ".123456#";
    for (int table = 0; table <= 1; table++) {
        //printf("Table %d elements per bucket:", table);
        for (size_t idx = 0; idx < numBuckets(s->bucketExp[table]); idx++) {
            hashtabBucket *b = &s->tables[table][idx];
            char c = b->presence == 0 && b->everfull ? 'X' : '0' + __builtin_popcount(b->presence);
            printf("%c", c);
        }
        if (table == 0) printf(" ");
    }
    printf("\n");
}

int hashtabLongestProbingChain(hashtab *t) {
    int maxlen = 0;
    for (int table = 0; table <= 1; table++) {
        if (t->bucketExp[table] < 0) {
            continue; /* table not used */
        }
        size_t cursor = 0;
        size_t mask = expToMask(t->bucketExp[table]);
        int chainlen = 0;
        do {
            assert(cursor <= mask);
            hashtabBucket *b = &t->tables[table][cursor];
            if (b->everfull) {
                if (++chainlen > maxlen) {
                    maxlen = chainlen;
                }
            } else {
                chainlen = 0;
            }
            cursor = nextCursor(cursor, mask);
        } while (cursor != 0);
    }
    return maxlen;
}
