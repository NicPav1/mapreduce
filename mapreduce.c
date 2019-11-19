#include "mapreduce.h"
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>

void MR_Emit(char *key, char *value);
unsigned long MR_DefaultHashPartition(char *key, int num_partitions);
unsigned long MR_SortedPartition(char *key, int num_partitions);
char* get_next(char *key, int partition);

// Used to store key-value pairs?
struct key_value
{
    char *key;
    char *value;
    struct key_value *next;
};

int nm;

struct key_value **list;

void MR_Run(int argc, char *argv[], 
            Mapper map, int num_mappers, 
            Reducer reduce, int num_reducers, 
            Partitioner partition, int num_partitions) {
    
    nm = num_partitions;
    list = malloc(num_partitions * sizeof(struct key_value*));
    for (int k = 0; k < num_partitions; k++) {
        //list[k] = malloc(sizeof(struct key_value*));
        struct key_value *ke = NULL;
        list[k] = ke;
    }
    if (!(argc > 1)) {
        exit(1);
    }
    
    for (int i = 1; i < argc; i++) {
        (*map)(argv[i]);
    }
    for (int r = 0; r < num_reducers; r++) {
        if (list[r] != NULL) {
            struct key_value *z = malloc(sizeof(struct key_value*));
            z = list[r];
            (*reduce)(z->key, &get_next, r);
        }
    }
}

void MR_Emit(char *key, char *value) {
    int hash = MR_DefaultHashPartition(key, nm);
    if (list[hash] == NULL) {
        struct key_value *new = malloc(sizeof(struct key_value));
        new->key = key;
        new->value = value;
        new->next = NULL;
        list[hash] = new;
    } else {
        struct key_value *new = malloc(sizeof(struct key_value));
        new->key = key;
        new->value = value;
        new->next = NULL;
        struct key_value *last = list[hash];
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

char* get_next(char *key, int partition) {
    struct key_value *k = list[partition];
    if (list[partition] == NULL) {
        return NULL;
    }
    char* val = k->value;
    list[partition] = k->next;
    return val;
}
