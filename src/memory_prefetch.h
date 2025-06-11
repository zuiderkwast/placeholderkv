#ifndef MEMORY_PREFETCH_H
#define MEMORY_PREFETCH_H

struct client;
struct list;

void prefetchCommandsBatchInit(void);
//void processClientsCommandsBatch(void);
//int addCommandToBatchAndProcessIfFull(struct client *c);
//void removeClientFromPendingCommandsBatch(struct client *c);

void prefetchSomeCommandsForSomeClients(struct list *clients);
void prefetchSomeCommandsForOneClient(struct client *c);

#endif /* MEMORY_PREFETCH_H */
