#include "mapreduce.h"
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>

struct key_value {
    char *key;
    char *value;
    struct key_value *next;
};

// Holds useful information about map threads
struct map_info {
    int file;  // File number
    int more;  // More files than mappers (1=yes, 0=no)
    pthread_t id;
    char *f;  // Holds the file to map to
};

// Holds useful information about the reduce threads
struct reduce_info {
    pthread_t id;
    int partition;
    char *key;
    struct key_value *head;
    int more;  // More partitions than reducers (1=yes, 0=no)
};

void MR_Emit(char *key, char *value);
unsigned long MR_DefaultHashPartition(char *key, int num_partitions);
unsigned long MR_SortedPartition(char *key, int num_partitions);
char* get_next(char *key, int partition);
void MergeSort(struct key_value** headRef);
struct key_value* SortedMerge(struct key_value* a, struct key_value* b);
void FrontBackSplit(struct key_value* source,
                    struct key_value** frontRef, struct key_value** backRef);


int nm;  // Number of partitions
volatile int indexes;  // To keep track of when num_reducers < num_partitions
volatile int ind;  // INdex to keep track of when num_mappers < files
int nr;  // Number of reducers
struct key_value **list;
char **argvv;
int argcc;

struct map_info *mappers;
struct reduce_info *reducers;
struct sort_info *sorters;
pthread_mutex_t *locks;
pthread_mutex_t lick;
pthread_mutex_t lock;

Partitioner part;
Mapper mapp;
Getter get;
Reducer red;

int logg(int l) {
    int count = 0;
    int ll = l;
    while (ll != 1) {
        ll /= 2;
        count++;
    }
    return count;
}

// Helper for mapping threads
void *map_help(void *arg) {
    struct map_info *a = arg;
    if (a->more == 0) {
        mapp(a->f);
    } else {
        pthread_mutex_lock(&lock);
        a->f = argvv[ind];
        a->file = ind;
        ind++;
        pthread_mutex_unlock(&lock);
        while (a->file < argcc) {
            mapp(a->f);
            pthread_mutex_lock(&lock);
            a->f = argvv[ind];
            a->file = ind;
            ind++;
            pthread_mutex_unlock(&lock);
        }
    }
}

// Helper for reducer threads
void *reduce_help(void *arg) {
    struct reduce_info *r = arg;
    if (r->more == 0) {
        struct key_value *h = malloc(sizeof(struct key_value*));
        h = list[r->partition];
        while (h != NULL) {
            red(h->key, get, r->partition);
            h = list[r->partition];
        }
    } else {
        pthread_mutex_lock(&lick);
        r->partition = indexes;
        indexes++;
        pthread_mutex_unlock(&lick);
        while (r->partition < nm) {
            struct key_value *h = malloc(sizeof(struct key_value*));
            h = list[r->partition];
            while (h!= NULL) {
                red(h->key, get, r->partition);
                h = list[r->partition];
            }
            pthread_mutex_lock(&lick);
            r->partition = indexes;
            indexes++;
            pthread_mutex_unlock(&lick);
        }
    }
}

void MR_Run(int argc, char *argv[],
            Mapper map, int num_mappers,
            Reducer reduce, int num_reducers,
            Partitioner partition, int num_partitions) {
    part = partition;
    get = &get_next;
    nm = num_partitions;
    mapp = map;
    red = reduce;
    indexes = 0;
    ind = 1;
    argcc = argc;
    nr = num_reducers;
    mappers = malloc(num_mappers * sizeof(struct map_info));
    reducers = malloc(num_reducers * sizeof(struct reduce_info));
    list = malloc(num_partitions * sizeof(struct key_value*));
    locks = malloc(num_partitions * sizeof(pthread_mutex_t));
    pthread_mutex_init(&lick, NULL);
    pthread_mutex_init(&lock, NULL);
    argvv = argv;

    // Fill the list with NULL pointers to avoid problems later on
    for (int k = 0; k < num_partitions; k++) {
        struct key_value *ke = NULL;
        list[k] = ke;
        pthread_mutex_t l;
        pthread_mutex_init(&l, NULL);
        locks[k] = l;
    }

    // Checks for arguments
    if (!(argc > 1)) {
        printf("Insufficient number of arguments.");
        exit(1);
    }

    int y = 0;  // Keep track of how many mapper threads were created

    if (num_mappers == 1) {
        for (int j = 1; j < argc; j++) {
            map(argv[j]);
        }
    } else if (num_mappers >= argc-1) {
        for (int i = 0; i < argc-1; i++) {
            struct map_info *m = malloc(sizeof(struct map_info));
            m->f = argv[i+1];
            m->more = 0;
            mappers[i] = *m;
            y++;
            pthread_create(&mappers[i].id, NULL, map_help, m);
        }
    } else {
        for (int i = 0; i < num_mappers; i++) {
            struct map_info *m = malloc(sizeof(struct map_info));
            y++;
            m->more = 1;
            mappers[i] = *m;
            pthread_create(&mappers[i].id, NULL, map_help, m);
        }
    }

    // Join mapper threads
    for (int i = 0; i < y; i++) {
        pthread_join(mappers[i].id, NULL);
    }

    // Sort all of the linked lists
    for (int i = 0; i < num_partitions; i++) {
        if (list[i] != NULL) {
            MergeSort(&list[i]);
        }
    }

    if ((num_reducers == 1) || (num_partitions == 1)) {
        for (int p = 0; p < num_partitions; p++) {
            if (list[p] != NULL) {
                struct key_value *z = malloc(sizeof(struct key_value*));
                while (list[p] != NULL) {
                    reduce(list[p]->key, &get_next, p);
                }
            }
        }
    } else if (num_reducers >= num_partitions) {
        for (int i = 0; i < num_partitions; i++) {
            if (list[i] != NULL) {
                struct key_value *z = malloc(sizeof(struct key_value*));
                z = list[i];
                struct reduce_info *re = malloc(sizeof(struct reduce_info*));
                re->partition = i;
                re->key = strdup(z->key);  // Seg faulting around here?
                re->head = z;
                re->more = 0;
                reducers[i] = *re;
                pthread_create(&reducers[i].id, NULL, reduce_help, re);
            }
        }
    } else {  // Num reducers < num_partitions
        for (int i = 0; i < num_reducers; i++) {
            struct reduce_info *re = malloc(sizeof(struct reduce_info*));
            re->more = 1;
            reducers[i] = *re;
            pthread_create(&reducers[i].id, NULL, reduce_help, re);
        }
    }

    int minimum_loop;
    if (num_reducers <= num_partitions) {
        minimum_loop = num_reducers;
    } else {
        minimum_loop = num_partitions;
    }

    if (num_partitions != 1 && num_reducers != 1) {
        for (int i = 0; i < minimum_loop; i++) {
            pthread_join(reducers[i].id, NULL);
        }
    }
}

void MR_Emit(char *key, char *value) {
    int hash = part(key, nm);
    pthread_mutex_lock(&locks[hash]);
    if (list[hash] == NULL) {
        struct key_value *new = malloc(sizeof(struct key_value));
        new->key = strdup(key);
        new->value = strdup(value);
        new->next = NULL;
        list[hash] = new;
    } else {
        struct key_value *new = malloc(sizeof(struct key_value));
        new->key = strdup(key);
        new->value = strdup(value);
        new->next = list[hash];
        list[hash] = new;
    }
    pthread_mutex_unlock(&locks[hash]);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

unsigned long MR_SortedPartition(char *key, int num_partitions) {
    char *end;
    if (num_partitions == 1) {
        return 0;
    } else {
        unsigned long n = strtoul(key, &end, 10);
        n = n & 0x0FFFFFFFF;
        int l = logg(num_partitions);
        int part = n >> (32 - l);
        return part;
    }
}

char* get_next(char *key, int partition) {
    struct key_value *k = malloc(sizeof(struct key_value*));
    struct key_value *prev = malloc(sizeof(struct key_value*));
    k = list[partition];
    if (k == NULL) {
        return NULL;
    }
    if (strcmp(k->key, key) == 0) {
        char* val = k->value;
        list[partition] = k->next;
        free(k);
        return val;
    }
    return 0;
}



/*
 * The following 3 methods (MergeSort, SortedMerge, FrontBackSplit)
 * were taken from Geeks for Geeks website in 
 * order to implement linked list merge sort. Professor said that
 * this is okay.
 * https://www.geeksforgeeks.org/merge-sort-for-linked-list/ 
 */
void MergeSort(struct key_value** headRef) {
    struct key_value* head = *headRef;
    struct key_value* a;
    struct key_value* b;

    /* Base case -- length 0 or 1 */
    if ((head == NULL) || (head->next == NULL)) {
        return;
    }

    /* Split head into 'a' and 'b' sublists */
    FrontBackSplit(head, &a, &b);

    /* Recursively sort the sublists */
    MergeSort(&a);
    MergeSort(&b);

    /* answer = merge the two sorted lists together */
    *headRef = SortedMerge(a, b);
}

/* See https:// www.geeksforgeeks.org/?p=3622 for details of this
function */
struct key_value* SortedMerge(struct key_value* a, struct key_value* b) {
    struct key_value* result = NULL;

    /* Base cases */
    if (a == NULL)
        return (b);
    else if (b == NULL)
        return (a);

    /* Pick either a or b, and recur */
    if (strcmp(a->key, b->key) <= 0) {
        result = a;
        result->next = SortedMerge(a->next, b);
    } else {
        result = b;
        result->next = SortedMerge(a, b->next);
    }
    return (result);
}

/* UTILITY FUNCTIONS */
/* Split the key_values of the given list into front and back halves,
    and return the two lists using the reference parameters.
    If the length is odd, the extra key_value should go in the front list.
    Uses the fast/slow pointer strategy. */
void FrontBackSplit(struct key_value* source,
                    struct key_value** frontRef, struct key_value** backRef) {
    struct key_value* fast;
    struct key_value* slow;
    slow = source;
    fast = source->next;

    /* Advance 'fast' two key_values, and advance 'slow' one key_value */
    while (fast != NULL) {
        fast = fast->next;
        if (fast != NULL) {
            slow = slow->next;
            fast = fast->next;
        }
    }

    /* 'slow' is before the midpoint in the list, so split it in two
    at that point. */
    *frontRef = source;
    *backRef = slow->next;
    slow->next = NULL;
}
