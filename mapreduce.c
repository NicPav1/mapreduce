#include "mapreduce.h"
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>

// Used to store key-value pairs?
typedef struct key_value
{
    char key;
    char value;
    struct key_value *next;
} KEY_VALUE;

int nm;

KEY_VALUE *list;

void MR_Emit(char *key, char *value) {
    int hash = MR_DefaultHashPartition(key, nm);
    if ((list + hash) == NULL) {
        KEY_VALUE *new = malloc(sizeof(KEY_VALUE));
        new->key = *key;
        new->value = *value;
        new->next = NULL;
        *(list+hash) = *new;
    } else {
        KEY_VALUE *new = malloc(sizeof(KEY_VALUE));
        new->key = *key;
        new->value = *value;
        new->next = NULL;
        KEY_VALUE *last = (list + hash);
        while (last->next != NULL) {
            last = last->next;
        }
        last->next = new;
    }
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

unsigned long MR_SortedPartition(char *key, int num_partitions) {
    return 0;
}

void MR_Run(int argc, char *argv[], 
            Mapper map, int num_mappers, 
            Reducer reduce, int num_reducers, 
            Partitioner partition, int num_partitions) {

    nm = num_partitions;
    list = malloc(sizeof(KEY_VALUE)*num_partitions);
    for (int k = 0; k < num_partitions; k++) {
        KEY_VALUE *ke = NULL;
        list[k] = *ke;
    }
    if (!(argc > 1)) {
        exit(1);
    }

    for (int i = 1; i < argc; i++) {
        (*map)(argv[i]);
    }
    for (int r = 0; r < num_reducers; r++) {
        (*reduce)() 
    }
}

int g = 0;  // Used to keep track of get_next

void* get_next(char *key, int partition) {
    KEY_VALUE *k = (list + partition);
    for (int i = 0; i < g; i ++) {
        if (k->next != NULL) {
            k = k->next;
        } else {
            g = 0;
            k = NULL;
            return k;
            break;
        }
    }
    g++;
    return k;
}