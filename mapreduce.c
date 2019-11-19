#include "mapreduce.h"
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <sys/stat.h>
#include <semaphore.h>

struct key_value
{
    char *key;
    char *value;
    struct key_value *next;
};

void MR_Emit(char *key, char *value);
unsigned long MR_DefaultHashPartition(char *key, int num_partitions);
unsigned long MR_SortedPartition(char *key, int num_partitions);
char* get_next(char *key, int partition);
void MergeSort(struct key_value** headRef);
struct key_value* SortedMerge(struct key_value* a, struct key_value* b);
void FrontBackSplit(struct key_value* source, 
                    struct key_value** frontRef, struct key_value** backRef);


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
        map(argv[i]);
    }
    
    for (int i = 0; i < num_partitions; i++) {
        if (list[i] != NULL) {
            MergeSort(&list[i]);
        }
    }

    /*
    for (int r = 0; r < num_partitions; r++) {
        if (list[r] != NULL) {
            struct key_value *z = malloc(sizeof(struct key_value*));
            z = list[r];
            while (z != NULL) {
                printf("%s\n", z->key);
                z = z->next;
            }
        }
    }
    */
    
    for (int r = 0; r < num_reducers; r++) {
        if (list[r] != NULL) {
            struct key_value *z = malloc(sizeof(struct key_value*));
            z = list[r];
            while(z != NULL) {
                reduce(z->key, &get_next, r);
                z = list[r];
                //z = z->next;
            }
        }
    }
}

void MR_Emit(char *key, char *value) {
    int hash = MR_DefaultHashPartition(key, nm);
    if (list[hash] == NULL) {
        struct key_value *new = malloc(sizeof(struct key_value));
        new->key = malloc(sizeof(strlen(key))+1);
        new->value = malloc(sizeof(strlen(value)) + 1);
        strcpy(new->key, key);
        strcpy(new->value, value);
        new->next = NULL;
        list[hash] = new;
    } else {
        struct key_value *new = malloc(sizeof(struct key_value));
        new->key = malloc(sizeof(strlen(key))+1);
        new->value = malloc(sizeof(strlen(value)) + 1);
        strcpy(new->key, key);
        strcpy(new->value, value);
        new->next = NULL;
        struct key_value *last = malloc(sizeof(struct key_value*));
        last = list[hash];
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
    struct key_value *k = malloc(sizeof(struct key_value*));
    struct key_value *prev = malloc(sizeof(struct key_value*));
    k = list[partition];
    if (k == NULL) {
        return NULL;
    }
    if (strcmp(k->key, key) == 0) {
        char* val = k->value;
        list[partition] = k->next;
        return val;
    } else {
        while (k != NULL && strcmp(k->key, key) != 0) {
            prev = k;
            k = k->next;
        }
        if (k == NULL) {
            return NULL;
        } else {
            char* val = k->value;
            prev->next = k->next;
            return val;
        }
    }
}



/*
 * The following 3 methods (MergeSort, SortedMerge, FrontBackSplit)
 * were taken from Geeks for Geeks website in 
 * order to implement linked list merge sort. Professor said that
 * this is okay.
 * https://www.geeksforgeeks.org/merge-sort-for-linked-list/ 
 */
void MergeSort(struct key_value** headRef) 
{ 
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
struct key_value* SortedMerge(struct key_value* a, struct key_value* b) 
{ 
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
    } 
    else { 
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
                    struct key_value** frontRef, struct key_value** backRef) 
{ 
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
