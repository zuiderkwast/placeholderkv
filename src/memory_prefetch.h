#ifndef MEMORY_PREFETCH_H
#define MEMORY_PREFETCH_H

struct client;
struct list;

typedef enum {
    PREFETCH_DISABLED,
    PREFETCH_ADDED,
    PREFETCH_FULL_ADDED,
    PREFETCH_FULL_NOT_ADDED
} prefetchAddToBatchResult;

void prefetchCommandsBatchInit(void);
//void processClientsCommandsBatch(void);
prefetchAddToBatchResult addToBatchAndProcessIfFull(struct client *c);
//void removeClientFromPendingCommandsBatch(struct client *c);

void prefetchSomeCommandsForSomeClients(struct list *clients);
void prefetchSomeCommandsForOneClient(struct client *c);

#endif /* MEMORY_PREFETCH_H */
