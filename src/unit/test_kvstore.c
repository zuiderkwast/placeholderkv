#include "../kvstore.c"
#include "test_help.h"

uint64_t hashTestCallback(const void *key) {
    return hashsetGenHashFunction((unsigned char *)key, strlen((char *)key));
}

int cmpTestCallback(hashset *t, void *k1, void *k2) {
    UNUSED(t);
    return strcmp(k1, k2);
}

void freeTestCallback(hashset *d, void *val) {
    UNUSED(d);
    zfree(val);
}

hashsetType KvstoreHashsetTestType = {
    .hashFunction = hashTestCallback,
    .keyCompare = cmpTestCallback,
    .elementDestructor = freeTestCallback,
    .rehashingStarted = kvstoreHashsetRehashingStarted,
    .rehashingCompleted = kvstoreHashsetRehashingCompleted,
    .getMetadataSize = kvstoreHashsetMetadataSize,
};

char *stringFromInt(int value) {
    char buf[32];
    int len;
    char *s;

    len = snprintf(buf, sizeof(buf), "%d", value);
    s = zmalloc(len + 1);
    memcpy(s, buf, len);
    s[len] = '\0';
    return s;
}

int test_kvstoreAdd16Keys(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    int i;
    dictEntry *de;

    int didx = 0;
    kvstore *kvs1 = kvstoreCreate(&KvstoreHashsetTestType, 0, KVSTORE_ALLOCATE_HASHSETS_ON_DEMAND);
    kvstore *kvs2 = kvstoreCreate(&KvstoreHashsetTestType, 0, KVSTORE_ALLOCATE_HASHSETS_ON_DEMAND | KVSTORE_FREE_EMPTY_HASHSETS);

    for (i = 0; i < 16; i++) {
        de = kvstoreHashsetAddRaw(kvs1, didx, stringFromInt(i), NULL);
        TEST_ASSERT(de != NULL);
        de = kvstoreHashsetAddRaw(kvs2, didx, stringFromInt(i), NULL);
        TEST_ASSERT(de != NULL);
    }
    TEST_ASSERT(kvstoreHashsetSize(kvs1, didx) == 16);
    TEST_ASSERT(kvstoreSize(kvs1) == 16);
    TEST_ASSERT(kvstoreHashsetSize(kvs2, didx) == 16);
    TEST_ASSERT(kvstoreSize(kvs2) == 16);

    kvstoreRelease(kvs1);
    kvstoreRelease(kvs2);
    return 0;
}

int test_kvstoreIteratorRemoveAllKeysNoDeleteEmptyHashset(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    int i;
    void *key;
    dictEntry *de;
    kvstoreIterator *kvs_it;

    int didx = 0;
    int curr_slot = 0;
    kvstore *kvs1 = kvstoreCreate(&KvstoreHashsetTestType, 0, KVSTORE_ALLOCATE_HASHSETS_ON_DEMAND);

    for (i = 0; i < 16; i++) {
        de = kvstoreHashsetAddRaw(kvs1, didx, stringFromInt(i), NULL);
        TEST_ASSERT(de != NULL);
    }

    kvs_it = kvstoreIteratorInit(kvs1);
    while ((de = kvstoreIteratorNext(kvs_it)) != NULL) {
        curr_slot = kvstoreIteratorGetCurrentHashsetIndex(kvs_it);
        key = dictGetKey(de);
        TEST_ASSERT(kvstoreHashsetDelete(kvs1, curr_slot, key) == DICT_OK);
    }
    kvstoreIteratorRelease(kvs_it);

    hashset *d = kvstoreGetHashset(kvs1, didx);
    TEST_ASSERT(d != NULL);
    TEST_ASSERT(kvstoreHashsetSize(kvs1, didx) == 0);
    TEST_ASSERT(kvstoreSize(kvs1) == 0);

    kvstoreRelease(kvs1);
    return 0;
}

int test_kvstoreIteratorRemoveAllKeysDeleteEmptyHashset(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    int i;
    void *key;
    dictEntry *de;
    kvstoreIterator *kvs_it;

    int didx = 0;
    int curr_slot = 0;
    kvstore *kvs2 = kvstoreCreate(&KvstoreHashsetTestType, 0, KVSTORE_ALLOCATE_HASHSETS_ON_DEMAND | KVSTORE_FREE_EMPTY_HASHSETS);

    for (i = 0; i < 16; i++) {
        de = kvstoreHashsetAddRaw(kvs2, didx, stringFromInt(i), NULL);
        TEST_ASSERT(de != NULL);
    }

    kvs_it = kvstoreIteratorInit(kvs2);
    while ((de = kvstoreIteratorNext(kvs_it)) != NULL) {
        curr_slot = kvstoreIteratorGetCurrentHashsetIndex(kvs_it);
        key = dictGetKey(de);
        TEST_ASSERT(kvstoreHashsetDelete(kvs2, curr_slot, key) == DICT_OK);
    }
    kvstoreIteratorRelease(kvs_it);

    /* Make sure the hashset was removed from the rehashing list. */
    while (kvstoreIncrementallyRehash(kvs2, 1000)) {
    }

    hashset *d = kvstoreGetHashset(kvs2, didx);
    TEST_ASSERT(d == NULL);
    TEST_ASSERT(kvstoreHashsetSize(kvs2, didx) == 0);
    TEST_ASSERT(kvstoreSize(kvs2) == 0);

    kvstoreRelease(kvs2);
    return 0;
}

int test_kvstoreHashsetIteratorRemoveAllKeysNoDeleteEmptyHashset(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    int i;
    void *key;
    dictEntry *de;
    kvstoreHashsetIterator *kvs_di;

    int didx = 0;
    kvstore *kvs1 = kvstoreCreate(&KvstoreHashsetTestType, 0, KVSTORE_ALLOCATE_HASHSETS_ON_DEMAND);

    for (i = 0; i < 16; i++) {
        de = kvstoreHashsetAddRaw(kvs1, didx, stringFromInt(i), NULL);
        TEST_ASSERT(de != NULL);
    }

    kvs_di = kvstoreGetHashsetSafeIterator(kvs1, didx);
    while ((de = kvstoreHashsetIteratorNext(kvs_di)) != NULL) {
        key = dictGetKey(de);
        TEST_ASSERT(kvstoreHashsetDelete(kvs1, didx, key) == DICT_OK);
    }
    kvstoreReleaseHashsetIterator(kvs_di);

    hashset *d = kvstoreGetHashset(kvs1, didx);
    TEST_ASSERT(d != NULL);
    TEST_ASSERT(kvstoreHashsetSize(kvs1, didx) == 0);
    TEST_ASSERT(kvstoreSize(kvs1) == 0);

    kvstoreRelease(kvs1);
    return 0;
}

int test_kvstoreHashsetIteratorRemoveAllKeysDeleteEmptyHashset(int argc, char **argv, int flags) {
    UNUSED(argc);
    UNUSED(argv);
    UNUSED(flags);

    int i;
    void *key;
    dictEntry *de;
    kvstoreHashsetIterator *kvs_di;

    int didx = 0;
    kvstore *kvs2 = kvstoreCreate(&KvstoreHashsetTestType, 0, KVSTORE_ALLOCATE_HASHSETS_ON_DEMAND | KVSTORE_FREE_EMPTY_HASHSETS);

    for (i = 0; i < 16; i++) {
        de = kvstoreHashsetAddRaw(kvs2, didx, stringFromInt(i), NULL);
        TEST_ASSERT(de != NULL);
    }

    kvs_di = kvstoreGetHashsetSafeIterator(kvs2, didx);
    while ((de = kvstoreHashsetIteratorNext(kvs_di)) != NULL) {
        key = hashsetGetKey(de);
        TEST_ASSERT(kvstoreHashsetDelete(kvs2, didx, key) == DICT_OK);
    }
    kvstoreReleaseHashsetIterator(kvs_di);

    hashset *d = kvstoreGetHashset(kvs2, didx);
    TEST_ASSERT(d == NULL);
    TEST_ASSERT(kvstoreHashsetSize(kvs2, didx) == 0);
    TEST_ASSERT(kvstoreSize(kvs2) == 0);

    kvstoreRelease(kvs2);
    return 0;
}
